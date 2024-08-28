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
package optimus.graph.cache

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.annotations.deprecating

import scala.annotation.tailrec
final class Holder(val hash: Int, value: WeakReference[AnyRef]) {
  private[this] val interned = new AtomicReference(value)
  @tailrec private[cache] def internedValueOr[T <: AnyRef](value: T): T = {
    val result = interned.get().get()
    if (result ne null) result.asInstanceOf[T]
    else {
      val wrapped = new WeakReference(value: AnyRef)
      if (interned.compareAndSet(null, wrapped))
        value
      else internedValueOr(value)
    }
  }
  override def toString: String = s"Holder[WeakReference[${interned.get()}"
}
object Interner extends Interner
//its a class to simplify testing
private[cache] class Interner {

  def internedAndHash[T <: AnyRef](value: T): (T, Int) = {
    if (value eq null) (value, 0)
    else locals.get().hashAndInterned(value)
  }
  def intern[T <: AnyRef](value: T): T = {
    if (value eq null) null.asInstanceOf[T]
    else locals.get().intern(value)
  }

  /**
   * its the same as interning, but the passed `value` is expected to be disguarded by the caller, and only the return
   * value is used so useful for deduplication at the point of the creation of the duplicate
   */
  def internFromNew[T <: AnyRef](value: T): T = {
    if (value eq null) null.asInstanceOf[T]
    else locals.get().internFromNew(value)
  }
  private val log: Logger = getLogger(this.getClass)
  private val idRefQueue = new ReferenceQueue[AnyRef]
  private class DeDupRef(val lookup: Lookup, value: AnyRef) extends WeakReference[AnyRef](value, idRefQueue)

  private abstract sealed class Lookup {
    protected[this] var hash: Int = 0
    override def hashCode(): Int = hash
    private[this] var weakRef: DeDupRef = _
    protected[this] var strongRef: AnyRef = _
    protected def ref() = {
      if (strongRef ne null) strongRef
      else weakRef.get
    }

    def afterLookup(): Unit = {
      strongRef = null
    }

    def makeReferencesWeak(): WeakReference[AnyRef] = {
      assert(strongRef ne null)
      assert(weakRef eq null)
      weakRef = new DeDupRef(this, strongRef)
      strongRef = null
      weakRef
    }

  }
  private class IdentityLookup extends Lookup {
    def beforeLookup(v: AnyRef): Unit = {
      hash = System.identityHashCode(v)
      strongRef = v
    }

    override def equals(obj: Any): Boolean = obj match {
      case lookup: IdentityLookup => (lookup eq this) || (ref() eq lookup.ref())
      case _                      => false
    }
  }
  private class HashLookup extends Lookup {
    def buildHolder(): Holder = {
      new Holder(hashCode(), makeReferencesWeak())

    }
    def beforeLookup(v: AnyRef): Unit = {
      hash = v.hashCode()
      strongRef = v
    }

    override def equals(obj: Any): Boolean = obj match {
      case lookup: HashLookup => (lookup eq this) || Objects.equals(ref(), lookup.ref())
      case _                  => false
    }
  }
  private class Locals {
    private[this] var value: AnyRef = _
    def holder[T <: AnyRef](value: T): Holder = {
      this.value = value
      identityLookup.beforeLookup(value)
      val holder = identityTable.computeIfAbsent(identityLookup, identityFunction)
      // identityLookup may be in use in the map and changed
      identityLookup.afterLookup()
      this.value = null
      holder
    }
    def holderFromNew[T <: AnyRef](value: T): Holder = {
      this.value = value

      hashLookup.beforeLookup(value)
      val holder = hashTable.computeIfAbsent(hashLookup, hashFunction)
      // hashLookup may be in use in the map and changed
      hashLookup.afterLookup()
      if (holder.internedValueOr(value) eq value) {
        val id = new IdentityLookup
        id.beforeLookup(value)
        identityTable.put(id, holder)
        id.makeReferencesWeak()
      }

      this.value = null
      holder
    }
    def intern[T <: AnyRef](value: T): T = {
      holder(value).internedValueOr(value)
    }
    def internFromNew[T <: AnyRef](value: T): T = {
      holderFromNew(value).internedValueOr(value)
    }
    def hashAndInterned[T <: AnyRef](value: T): (T, Int) = {
      val h = holder(value)
      (h.internedValueOr(value), h.hashCode())
    }
    object identityFunction extends java.util.function.Function[IdentityLookup, Holder] {
      override def apply(t: IdentityLookup): Holder = {
        t.makeReferencesWeak()
        identityLookup = new IdentityLookup
        hashLookup.beforeLookup(value)
        val holder = hashTable.computeIfAbsent(hashLookup, hashFunction)
        // hashLookup may be in use in the map and changed
        hashLookup.afterLookup()
        holder
      }
    }
    object hashFunction extends java.util.function.Function[HashLookup, Holder] {
      override def apply(t: HashLookup): Holder = {
        hashLookup = new HashLookup
        // t.makeReferencesWeak() is called by buildHolder
        t.buildHolder()
      }
    }

    private var identityLookup: IdentityLookup = new IdentityLookup
    private var hashLookup: HashLookup = new HashLookup
  }
  private[this] val identityTable = new ConcurrentHashMap[IdentityLookup, Holder]
  private[this] val hashTable = new ConcurrentHashMap[HashLookup, Holder]
  private[this] val locals = ThreadLocal.withInitial[Locals](() => new Locals)

  private[cache] def sourceSize: Int = identityTable.size()
  private[cache] def targetSize: Int = hashTable.size()

  @deprecating("TEST_ONLY")
  private[cache] def afterTest(): Unit = {
    thread.interrupt()
  }
  val thread = new Thread(() => poll, "InternerCleaner")
  thread.setDaemon(true)
  thread.start()
  def poll: Unit = {
    while (true) {
      idRefQueue.remove() match {
        case dd: DeDupRef =>
          dd.lookup match {
            case identity: IdentityLookup =>
              identityTable.remove(identity)
            case hash: HashLookup =>
              hashTable.remove(hash)
          }
        case x => log.warn(s"unexpected ${x.getClass()} - $x")

      }
    }
  }

}
