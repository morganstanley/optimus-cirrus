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
package optimus.platform.pickling

import optimus.exceptions.RTExceptionTrait
import optimus.graph.PropertyInfo
import optimus.graph.Settings
import optimus.platform.pickling.PropertyMapOutputStream.PickleSeq
import optimus.platform.pickling.WriteContextOutputStream.PickledComparator
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.ModuleEntityToken
import optimus.platform.storable.RawReference
import optimus.platform.storable.Storable

import java.time.Period
import java.time.ZoneId
import java.util
import java.util.Comparator
import scala.collection.mutable
import scala.reflect.ClassTag

class TemporaryEntityException(val entity: Entity, val propertyName: String)
    extends IllegalArgumentException(
      "unexpected temporary entity: %s reached from property %s".format(entity, propertyName))
    with RTExceptionTrait {
  def this(entity: Entity, propertyInfo: PropertyInfo[_]) =
    this(entity, s"${propertyInfo.entityInfo.runtimeClass.getSimpleName}::${propertyInfo.name}")
}

abstract class WriteContextOutputStream[A] extends AbstractPickledOutputStream { os =>
  protected[this] var writeContext: WriteContextStack = ValueWriteContext
  protected def stableOrdering = false

  sealed trait WriteContextStack extends WriteContext {
    def parent: WriteContextStack
    def currentField: Option[String]
    def flush(): Unit
  }

  object ValueWriteContext extends WriteContextStack {
    def parent = throw new UnsupportedOperationException

    def flush(): Unit = throw new UnsupportedOperationException

    def writeFieldName(k: String): Unit = throw new UnsupportedOperationException

    override def writeBoolean(data: Boolean): Unit = os.value = data
    override def writeChar(data: Char): Unit = os.value = data
    override def writeDouble(data: Double): Unit = os.value = data
    override def writeFloat(data: Float): Unit = os.value = data
    override def writeInt(data: Int): Unit = os.value = data
    override def writeLong(data: Long): Unit = os.value = data
    override def writeRawObject(data: AnyRef): Unit = os.value = data

    def currentField: Option[String] = None
  }

  protected def newMapBuilder: mutable.Builder[(String, Any), PickledProperties] =
    new PickledPropertiesBuilder(PicklingMapEntryOrderWorkaround.newBuilder[Any])

  final class MapWriteContext(val parent: WriteContextStack) extends WriteContextStack {
    private[this] val values = newMapBuilder

    private[this] var nextField: String = _

    // Note that we don't sort the keys here (even in stableOrdering mode). That's because the fields have always
    // been stored in the DAL in HashMap ordering, and changing that would change the hash of any keys/indexes (see
    // comments on newMapBuilder)
    def flush(): Unit = parent.writeRawObject(values.result())

    def writeFieldName(k: String): Unit = {
      require(nextField eq null)
      nextField = k
    }

    private def write(data: Any): Unit = {
      require(nextField ne null)
      values += ((nextField, data))
      nextField = null
    }

    override def writeBoolean(data: Boolean): Unit = write(data)

    override def writeChar(data: Char): Unit = write(data)

    override def writeDouble(data: Double): Unit = write(data)

    override def writeFloat(data: Float): Unit = write(data)

    override def writeInt(data: Int): Unit = write(data)

    override def writeLong(data: Long): Unit = write(data)

    override def writeRawObject(data: AnyRef): Unit = write(data)

    def currentField: Option[String] = Option(nextField) orElse parent.currentField
  }

  final class ArrayWriteContext(val parent: WriteContextStack, isUnordered: Boolean) extends WriteContextStack {
    // If all written values are of the same primitive type, the resulting PickleSeq (which is an immutable.ArraySeq)
    // built in `flush` wraps a primitive array.

    private[this] var b: mutable.ArrayBuilder[_] = null
    private[this] var tp: Class[_] = null

    private def builder[T](implicit ct: ClassTag[T]): mutable.ArrayBuilder[T] = {
      if (b == null) {
        tp = ct.runtimeClass
        b = mutable.ArrayBuilder.make(ct)
      } else if (tp != classOf[Any] && tp != ct.runtimeClass) {
        tp = classOf[Any]
        val newB = mutable.ArrayBuilder.make[Any]
        newB.addAll(b.result())
        b = newB
      }
      b.asInstanceOf[mutable.ArrayBuilder[T]]
    }

    def flush(): Unit = {
      val res =
        if (b == null) PickleSeq.empty[Any]
        else {
          val arr = b.result()
          if (Settings.enableStableIndexKeyOrdering && isUnordered && stableOrdering) PickledComparator.sortArray(arr)
          PickleSeq.unsafeWrapArray(arr)
        }
      b = null
      tp = null
      parent.writeRawObject(res)
    }

    def writeFieldName(k: String): Unit =
      throw new UnsupportedOperationException("Sequences do not support named fields")

    def writeBoolean(data: Boolean): Unit = builder[Boolean] match {
      case b: mutable.ArrayBuilder.ofBoolean => b.addOne(data) // no boxing
      case b                                 => b.addOne(data)
    }
    def writeChar(data: Char): Unit = builder[Char] match {
      case b: mutable.ArrayBuilder.ofChar => b.addOne(data) // no boxing
      case b                              => b.addOne(data)
    }
    def writeDouble(data: Double): Unit = builder[Double] match {
      case b: mutable.ArrayBuilder.ofDouble => b.addOne(data) // no boxing
      case b                                => b.addOne(data)
    }
    def writeFloat(data: Float): Unit = builder[Float] match {
      case b: mutable.ArrayBuilder.ofFloat => b.addOne(data) // no boxing
      case b                               => b.addOne(data)
    }
    def writeInt(data: Int): Unit = builder[Int] match {
      case b: mutable.ArrayBuilder.ofInt => b.addOne(data) // no boxing
      case b                             => b.addOne(data)
    }
    def writeLong(data: Long): Unit = builder[Long] match {
      case b: mutable.ArrayBuilder.ofLong => b.addOne(data) // no boxing
      case b                              => b.addOne(data)
    }
    def writeRawObject(data: AnyRef): Unit = builder[Any].addOne(data)

    def currentField: Option[String] = parent.currentField
  }

  override def writeFieldName(k: String): Unit = writeContext.writeFieldName(k)

  override def writeStartArray(isUnordered: Boolean = false): Unit = {
    writeContext = new ArrayWriteContext(writeContext, isUnordered)
  }

  override def writeStartObject(): Unit = {
    writeContext = new MapWriteContext(writeContext)
  }

  override def writeEndArray(): Unit = {
    writeContext.flush()
    writeContext = writeContext.parent
  }

  override def writeEndObject(): Unit = {
    writeContext.flush()
    writeContext = writeContext.parent
  }

  override def writeBoolean(data: Boolean): Unit = writeContext.writeBoolean(data)
  // TODO (OPTIMUS-0000): should be converted at lower layer or DSI-specific thing
  override def writeChar(data: Char): Unit = writeContext.writeInt(data)
  override def writeDouble(data: Double): Unit = writeContext.writeDouble(data)
  override def writeFloat(data: Float): Unit = writeContext.writeFloat(data)
  override def writeInt(data: Int): Unit = writeContext.writeInt(data)
  override def writeLong(data: Long): Unit = writeContext.writeLong(data)
  override def writeRawObject(data: AnyRef): Unit = writeContext.writeRawObject(data)

  def value_=(a: Any): Unit
  def value: A
}

private object WriteContextOutputStream {

  private object PickledComparator extends Comparator[Any] {
    def sortArray(a: Array[_]): Unit =
      a match {
        // all non-primitive arrays are assignable to Array[AnyRef] and we use our pickled comparison logic for these
        case arr: Array[AnyRef] => util.Arrays.sort(arr, this)
        // for primitive arrays we must dispatch to the correct overload of sort
        case arr: Array[Int]     => util.Arrays.sort(arr)
        case arr: Array[Long]    => util.Arrays.sort(arr)
        case arr: Array[Double]  => util.Arrays.sort(arr)
        case arr: Array[Float]   => util.Arrays.sort(arr)
        case arr: Array[Short]   => util.Arrays.sort(arr)
        case arr: Array[Char]    => util.Arrays.sort(arr)
        case arr: Array[Byte]    => util.Arrays.sort(arr)
        case arr: Array[Boolean] => sortBooleans(arr)
      }

    // no built-in sort algo is supplied for Booleans, so we roll our own
    def sortBooleans(a: Array[Boolean]): Unit = {
      // count how many falses
      var falseCount = 0
      var i = 0
      while (i < a.length) {
        if (!a(i)) falseCount += 1
        i += 1
      }
      // rewrite array with falses at the start and trues at the end
      i = 0
      while (i < a.length) {
        a(i) = i >= falseCount
        i += 1
      }
    }

    // Be very careful if changing this logic to preserve the current sorting order for existing types. We must stay
    // compatible with the ordering used by indexes/keys stored in the DAL
    override def compare(o1: Any, o2: Any): Int = (o1, o2) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1

      // lexical comparison for Iterables - do this before checking classes so that e.g. List and Vector are treated
      // as equivalent
      case (x: Iterable[_], y: Iterable[_]) =>
        // We're assuming that if these iterables are pickled representations of unordered collections, they would
        // already have been sorted by ArrayWriteContext#flush. In all other cases they were already ordered in the user
        // code, so we can always safely do a lexical comparison here.
        val it1 = x.iterator
        val it2 = y.iterator
        while (it1.hasNext && it2.hasNext) {
          val c = compare(it1.next(), it2.next())
          if (c != 0) return c
        }
        if (it1.hasNext) 1 else if (it2.hasNext) -1 else 0

      case ((x1, x2), (y1, y2)) =>
        // tuples are produced by MapWriteContext#writeFieldName/write - we assume that calls to writeFieldName are
        // already in deterministic order so it's safe to just compare the tuples in the order that we find them
        val c = compare(x1, y1)
        if (c != 0) c else compare(x2, y2)

      // guard against incompatible classes which probably won't be mutually comparable
      case (x, y) if x.getClass != y.getClass => x.getClass.getName.compareTo(y.getClass.getName)

      // classes are same (due to above check) so should be safe to compare
      case (x: Comparable[Any @unchecked], y: Comparable[Any @unchecked]) => x.compareTo(y)

      // a few special cases for types that don't implement Comparable but are found in pickled data (see
      // ProtoPickleSerializer for full list)
      case (x: RawReference, y: RawReference)             => RawReference.ordering.compare(x, y)
      case (x: ImmutableByteArray, y: ImmutableByteArray) => util.Arrays.compare(x.data, y.data)
      case (x: Array[Byte], y: Array[Byte])               => util.Arrays.compare(x, y)
      case (x: ModuleEntityToken, y: ModuleEntityToken)   => x.className.compareTo(y.className)
      case (x: Period, y: Period)                         => x.toString.compareTo(y.toString)
      case (x: ZoneId, y: ZoneId)                         => x.toString.compareTo(y.toString)
    }
  }
}

object PropertyMapOutputStream {
  // Using these aliases helps understanding / identifying code that deals with pickled entities.
  // The static types don't help there because `entity.toMap` has type `Map[String, Any]`.
  type PickleSeq[+T] = scala.collection.immutable.IndexedSeq[T]
  object PickleSeq
      extends scala.collection.SeqFactory.Delegate[scala.collection.immutable.IndexedSeq](
        scala.collection.immutable.ArraySeq.untagged) {
    override def from[E](it: IterableOnce[E]): scala.collection.immutable.IndexedSeq[E] = it match {
      case ps: PickleSeq[E] => ps
      case _                => super.from(it)
    }

    def unsafeWrapArray[T](x: Array[T]): PickleSeq[T] = scala.collection.immutable.ArraySeq.unsafeWrapArray(x)
  }

  def pickledValue[T](value: T, pickler: Pickler[T], stableOrdering: Boolean = false): Any = {
    val os = new PropertyMapOutputStream(Map.empty, stableOrdering)
    pickler.pickle(value, os)
    os.value
  }

  private[optimus] def pickledStorable(
      s: Storable,
      entityReferences: collection.Map[Entity, EntityReference]
  ): PickledProperties = {
    val os = new PropertyMapOutputStream(entityReferences)
    os.writeStartObject()
    s.pickle(os)
    os.writeEndObject()
    os.value.asInstanceOf[PickledProperties]
  }
}

class PropertyMapOutputStream(
    referenceMap: collection.Map[Entity, EntityReference] = Map.empty,
    /**
     * If stableOrdering is true, we will produce identically ordered output for equivalent inputs - for example
     * entries in any unordered Sets or Maps will be sorted. This is important because keys and indexes in the DAL
     * get hashed with an ordering-dependent hash, so the ordering must be well-defined.
     *
     * It's not necessary to sort the data in ordinary entity properties, so we don't to save the effort.
     */
    override protected val stableOrdering: Boolean = false)
    extends WriteContextOutputStream[Any] {
  private[this] var _value: Any = _

  override def value_=(a: Any): Unit = { _value = a }
  def value: Any = _value

  def getEntityRef(entity: Entity): EntityReference =
    Option(entity.dal$entityRef).getOrElse(referenceMap.getOrElse(entity, null))

  override def writeEntity(entity: Entity): Unit = {
    val obj =
      if (entity.$isModule)
        ModuleEntityToken(entity.getClass.getName)
      else if (getEntityRef(entity) eq null)
        throw new TemporaryEntityException(entity, writeContext.currentField.orNull)
      else
        getEntityRef(entity)

    writeRawObject(obj)
  }
}
