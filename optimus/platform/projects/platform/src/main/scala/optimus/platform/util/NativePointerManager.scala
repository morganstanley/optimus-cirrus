/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.util

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.{Cache => CCache}
import com.sun.management.GarbageCollectionNotificationInfo
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.utils.MiscUtils.Endoish._

import java.lang.management.ManagementFactory
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.time.Duration
import java.util
import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.NotificationListener
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/*
  This cache would typically be used for classes that hold values that represent native (e.g. SWIG) pointers,
  which will ultimately be disposed of by calling some JNI method.
     K = key to be used in the internal LRU cache and created by the override of j2k and disposed of
         with disposeK
     V = values held in the cache.  If the cache does not contain an entry for the K, we create one with j2v.
         These are ultimately destroyed by disposeV, but we do not call this immediately upon expulsion from
         the LRU cache, as there may still be live references to them.

     To destroy the V's safely, we track them by wrapping with a WeakReference, which Java will place on a
     reference queue when the V is GC'd.  In turn the reference queue is drained when a GC is detected, and
     the disposeV method is called for each item.
 */
class NativePointerManager extends Log {
  private val lock = this
  private val rq = new ReferenceQueue[AnyRef]
  private val links = new SyncIntrusiveList[WeakLink[_]](lock)
  def size: Int = links.size

  // Weak reference to the referent, but we hold the actual pointer so we can manage it.
  // Crucially: we don't use the referent argument anywhere other than in the superclass constructor.
  private class WeakLink[A <: AnyRef](referent: A, val ptr: Long, val dispose: Long => Unit)
      extends WeakReference[A](referent, rq)
      with links.Linked {
    final override def onRelease(): Unit = dispose(ptr)
  }

  private val lrus = mutable.WeakHashMap.empty[CCache[_, _], Int]
  def addLRU(lru: CCache[_, _]): Unit = this.synchronized {
    lrus += lru -> 1
  }

  // Release WRs as their referents are GC'd and placed on the reference queue
  private val cleanerWait = new Object
  private val cleaner = new Thread {
    override def run() = while (true) {
      var i = 0
      var wr = rq.poll()
      while (wr ne null) {
        wr.asInstanceOf[WeakLink[_]].release()
        i += 1
        wr = rq.poll()
      }
      if (i > 0) {
        val nlru = lock.synchronized {
          lrus.keys.map(_.estimatedSize()).sum
        }
        val msg = s"PointerCache: Disposed of $i items; cached=$nlru weak=${links.size}"
        log.info(msg)
        Breadcrumbs(
          ChainedID.root,
          PropertiesCrumb(_, Crumb.GCSource, Properties.logMsg -> msg, Properties.numRemoved -> i))
      }
      cleanerWait.synchronized(cleanerWait.wait(60000))
    }
  }

  private def wakeCleaner(): Unit = this.synchronized {
    if (cleaner.getState == Thread.State.NEW) {
      cleaner.setDaemon(true)
      cleaner.setName("Native Cleaner")
      cleaner.start()
    } else if (cleaner.getState == Thread.State.TIMED_WAITING) {
      cleanerWait.synchronized(cleanerWait.notify())
    }
  }

  ManagementFactory.getGarbageCollectorMXBeans.asScala
    .collect { case n: NotificationEmitter => n }
    .foreach { n =>
      n.addNotificationListener(
        new NotificationListener {
          override def handleNotification(notification: Notification, handback: Any): Unit = {
            if (notification.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)
              wakeCleaner()
          }
        },
        null,
        null
      )
    }

  // Once managed, the object's deletion method will be called immediately upon its GC
  def manage[A <: AnyRef](a: A, ptr: Long, delete: Long => Unit): A = {
    new WeakLink(a, ptr, delete) // inserts into linked list of WeakReferences
    a
  }

  abstract class Cache[K <: AnyRef, V <: AnyRef](n: Int) {
    private type W = WeakLink[V]
    private val lru = new util.LinkedHashMap[K, W](n, 1.0f, true) {
      override def removeEldestEntry(eldest: util.Map.Entry[K, W]) = {
        if (this.size > n) {
          log.info(s"Removing ${eldest.getKey} -> ${eldest.getValue}")
          true
        } else false
      }

    }

    def delete(ptr: Long): Unit
    def getPtr(v: V): Long

    final def size = lru.size
    final def put(k: K, v: V): V = this.synchronized {
      log.info(s"Inserting $k -> $v")
      val wr = new WeakLink(v, getPtr(v), delete)
      lru.put(k, wr)
      v
    }
    final def get(k: K): Option[V] = this.synchronized {
      for {
        w <- Option(lru.get(k))
        v <- Option(w.get())
      } yield v
    }

    final def remove(k: K) = this.synchronized {
      lru.remove(k)
    }
  }

  abstract class Interner[K <: AnyRef, V <: AnyRef](size: Int = 100 * 1000, expire: Duration = Duration.ZERO) {

    // Note that caffeine caches are not precisely LRU, but close enough.
    private val lru: CCache[K, V] = Caffeine
      .newBuilder()
      .maximumSize(size)
      .applyIf(expire != Duration.ZERO)(_.expireAfterAccess(expire))
      .build[K, V]

    addLRU(lru)

    // Keep track of LRUs of all PointerCache instances for logging

    def ptrToK(ptr: Long): K // create an LRU key from the PointerCache.apply key
    def ptrToV(ptr: Long): V // create a new value type from the LRU key
    // Clean up K if it turns out to already be in the cache (may be a noop)
    def disposePointer(ptr: Long): Unit
    // Optionally dispose of pointer - e.g. if we got a hit in the LRU
    def maybeDisposePointer(ptr: Long, k: K, v: V, computed: Boolean): Unit

    def get(ptr: Long): V = apply(ptr)
    def apply(j: Long): V = {
      val k = ptrToK(j)
      var computed = false
      val v = lru.get(
        k,
        _ => {
          val v = ptrToV(j)
          computed = true
          manage(v, j, disposePointer)
        })
      maybeDisposePointer(j, k, v, computed)
      v
    }

    def status = {
      (lru.estimatedSize(), NativePointerManager.size)
    }
  }
}

object NativePointerManager extends NativePointerManager
