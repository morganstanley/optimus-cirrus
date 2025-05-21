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
package optimus.platform.util.interner

import scala.collection.mutable.WeakHashMap
import java.lang.ref.WeakReference
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Lock
import optimus.platform.storable.RawReference
import optimus.platform.util.Log
import scala.collection.mutable.{Set => MSet}
import optimus.platform.storable.StorableCompanionBase
import scala.collection.immutable.SortedMap

/**
 * the user facing API for interning
 */
trait Interner[T <: AnyRef] {
  def apply[V <: T](value: V): V
}

/**
 * a no-op interner
 */
object NoInterner {
  private object noInternerImpl extends Interner[AnyRef] {
    override def apply[V <: AnyRef](value: V) = value
  }
  def apply[T <: AnyRef] = noInternerImpl.asInstanceOf[Interner[T]]
}

/**
 * a compressor allows for the interning process to store a reduced memory version of the objects, e.g. by interning
 * some of the internal fields of the stored object
 */
trait Compressor[T <: AnyRef] {
  def compress[V <: T](value: V): V
}

/**
 * a field compressor is like a normal compressor but is for fields, and is therefore recursive (as a field can contain
 * other fields)
 */
trait FieldCompressor {

  /**
   * attempt to compress a field, which may be recursive
   * @param value
   *   the value to be compressed
   * @param level
   *   the recursion depth. this should be preserved in any call to fieldInterner
   * @param fieldInterner
   *   the CompressingFieldInterner that is manging this compression
   */
  def compress[V <: AnyRef](value: V, level: Int, fieldInterner: CompressingFieldInterner): Option[V] = None
}

/**
 * FieldCompressor mixin that compresses basic structures
 */
trait BasicStructureCompressor extends FieldCompressor {
  override def compress[V <: AnyRef](value: V, level: Int, fieldInterner: CompressingFieldInterner): Option[V] = {
    val v = value match {
      case s: collection.Seq[t] =>
        s map { v =>
          fieldInterner.compressWithLock(v, level)
        }
      case (k, v) =>
        (fieldInterner.compressWithLock(k, level), fieldInterner.compressWithLock(v, level))
      case (a, b, c) =>
        (
          fieldInterner.compressWithLock(a, level),
          fieldInterner.compressWithLock(b, level),
          fieldInterner.compressWithLock(c, level))
      case m: SortedMap[k, v] =>
        val result: SortedMap[k, v] = m.foldLeft(m.empty) { case (ss, (k, v)) =>
          ss.updated(fieldInterner.compressWithLock(k, level), fieldInterner.compressWithLock(v, level))
        }
        result
      case m: Map[k, v] =>
        m map { case (k, v) => (fieldInterner.compressWithLock(k, level), fieldInterner.compressWithLock(v, level)) }
      case o: Some[v] =>
        val value = o.get
      case _ => null
    }
    Option(v.asInstanceOf[V])
      .map(fieldInterner.compressWithLock(_, level))
      .orElse(super.compress(value, level, fieldInterner))
  }
}

/**
 * compression rules for standard data types
 */
trait BasicDataCompressor extends FieldCompressor {
  import java.time.{Instant, Duration}
  import java.time.temporal.Temporal
  import java.time.ZoneId
  override def compress[V <: AnyRef](value: V, level: Int, fieldInterner: CompressingFieldInterner): Option[V] = {
    val v = value match {
      // simple primitive data types that don't contain object fields, are singletons etc
      case _: String //
          | _: RawReference //
          | _: Number //
          | _: Temporal // most java.time related fields
          | _: Instant //
          | _: Duration //
          | _: ZoneId //
          =>
        fieldInterner.compressWithLock(value, level)
      // singletons
      case Nil //
          | _: Enum[_] //
          | _: Class[_] //
          | _: StorableCompanionBase[_] //
          | _: Enumeration //
          | None =>
        value
      case _ => null.asInstanceOf[V]
    }
    Option(v).orElse(super.compress(value, level, fieldInterner))
  }
}

trait InternerImpl[T <: AnyRef] extends Interner[T] {
  @inline protected def withLock[T](fn: => T) = fn
  protected def getNotNull[V <: T](value: V): Option[V]
  protected def addNotNull[V <: T](value: V): V
}
trait LockedInterner[T <: AnyRef] extends InternerImpl[T] {
  protected val lock: Lock
  @inline override def withLock[T](fn: => T) = {
    lock.lock
    try fn
    finally lock.unlock
  }

}
trait CompressingInternerImpl[T <: AnyRef] extends InternerImpl[T] {
  final def apply[V <: T](value: V) = {
    if (value == null) value
    else {
      withLock(getNotNull(value)) match {
        case None =>
          val compressed = compressNotNull(value)
          withLock(addNotNull(compressed))
        case Some(v) => v
      }
    }
  }

  /** called without the lock being held */
  protected def compressNotNull[V <: T](value: V): V
}
trait CompressingFieldInterner extends InternerImpl[AnyRef] {
  final def apply[V <: AnyRef](value: V) = {
    if (value == null) value
    else {
      withLock {
        compressWithLock(value, 0)
      }
    }
  }
  protected val maxDepth: Int = 10
  final def compressWithLock[V](value: V, level: Int): V = {
    value match {
      case null => value
      case ref: AnyRef =>
        getNotNull(ref) match {
          case None =>
            val compressed = if (level >= maxDepth) ref else compressNotNull(ref, level + 1)
            addNotNull(compressed).asInstanceOf[V]
          case Some(v) => v.asInstanceOf[V]
        }
      case _ => value
    }

  }

  /** called with the lock being held */
  protected def compressNotNull[V <: AnyRef](value: V, level: Int): V
}

trait SimpleInternerImpl[T <: AnyRef] extends InternerImpl[T] {
  def apply[V <: T](value: V) = {
    withLock {
      if (value == null) value
      else {
        getNotNull(value) match {
          case None    => addNotNull(value)
          case Some(v) => v
        }
      }
    }
  }
}

object SimpleWeakInterner {
  def apply[T <: AnyRef](lock: Lock = new ReentrantLock) = new SimpleLockedWeakInterner[T](lock)
  def unlocked[T <: AnyRef] = new SimpleUnlockedWeakInterner[T]
}
object CompressingWeakInterner {
  def apply[T <: AnyRef](compressor: Compressor[T], lock: Lock = new ReentrantLock) =
    new CompressingLockedWeakInterner[T](compressor, lock)
  def unlocked[T <: AnyRef](compressor: Compressor[T]) = new CompressingUnlockedWeakInterner[T](compressor)
}
object CompressingFieldInterner {
  def apply(compressor: FieldCompressor, lock: Lock = new ReentrantLock) =
    new CompressingFieldInternerImpl(compressor, lock)
}
class CompressingFieldInternerImpl(compressor: FieldCompressor, lock: Lock = new ReentrantLock)
    extends CompressingFieldInterner
    with WeakIntererImpl[AnyRef]
    with Log {
  private lazy val uncompressed = MSet.empty[Class[_]]
  override protected def compressNotNull[V <: AnyRef](value: V, level: Int): V = {
    compressor.compress(value, level, this) match {
      case Some(v) if v != null => v
      case _ =>
        if (uncompressed.add(value.getClass)) log.warn("don't know how to compress a " + value.getClass)
        addNotNull(value)
    }
  }

}

class SimpleLockedWeakInterner[T <: AnyRef](protected val lock: Lock)
    extends SimpleUnlockedWeakInterner[T]
    with LockedInterner[T]

trait WeakIntererImpl[T <: AnyRef] extends InternerImpl[T] {
  private val data = new WeakHashMap[T, WeakReference[T]]
  @inline private def toV[V <: T](t: T) = t.asInstanceOf[V]

  override protected def getNotNull[V <: T](value: V): Option[V] = {
    val raw = data.get(value);
    raw flatMap { t =>
      Option(toV[V](t.get))
    }
  }
  override protected def addNotNull[V <: T](value: V): V = {
    val valRef = new WeakReference[T](value)
    data.put(value, valRef) match {
      case None => value
      case Some(old) => {
        val oldVal = old.get()
        if (oldVal == null) {
          // race of the GC, so keep the new value
          value
        } else {
          data.put(oldVal, old)
          toV(oldVal)
        }
      }
    }
  }
}
class SimpleUnlockedWeakInterner[T <: AnyRef] extends WeakIntererImpl[T] with SimpleInternerImpl[T]
class CompressingLockedWeakInterner[T <: AnyRef](compressor: Compressor[T], protected val lock: Lock)
    extends CompressingUnlockedWeakInterner[T](compressor)
    with LockedInterner[T]
class CompressingUnlockedWeakInterner[T <: AnyRef](private val compressor: Compressor[T])
    extends WeakIntererImpl[T]
    with CompressingInternerImpl[T] {
  override protected def compressNotNull[V <: T](value: V) = compressor.compress(value)
}
