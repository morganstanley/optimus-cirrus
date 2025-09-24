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
import optimus.platform.pickling.PropertyMapOutputStream.PickleSeq
import optimus.platform.storable.Entity

import scala.collection.mutable
import optimus.platform.storable.ModuleEntityToken
import optimus.platform.storable.EntityReference
import optimus.platform.storable.Storable

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

  // This builder retains the exact behaviour from the code prior to the refactoring to make
  // newMapBuilder an extension point.
  // See also comments on PicklingMapEntryOrderWorkaround
  protected def newMapBuilder: mutable.Builder[(String, Any), Map[String, Any]] =
    PicklingMapEntryOrderWorkaround.newBuilder[Any]

  final class MapWriteContext(val parent: WriteContextStack) extends WriteContextStack {
    private[this] val values = newMapBuilder

    private[this] var nextField: String = _

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

  final class ArrayWriteContext(val parent: WriteContextStack) extends WriteContextStack {
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
        else PickleSeq.unsafeWrapArray(b.result())
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

  override def writeStartArray(): Unit = {
    writeContext = new ArrayWriteContext(writeContext)
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

  def pickledValue[T](value: T, pickler: Pickler[T]): Any = {
    val os = new PropertyMapOutputStream(Map.empty)
    pickler.pickle(value, os)
    os.value
  }

  private[optimus] def pickledStorable(s: Storable, entityReferences: collection.Map[Entity, EntityReference]) = {
    val os = new PropertyMapOutputStream(entityReferences)
    os.writeStartObject()
    s.pickle(os)
    os.writeEndObject()
    os.value
  }
}

class PropertyMapOutputStream(referenceMap: collection.Map[Entity, EntityReference])
    extends WriteContextOutputStream[Any] {
  def this() = this(Map.empty)
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
