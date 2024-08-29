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

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Year
import java.time.YearMonth
import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.pickling.ChainedIDPickler
import optimus.datatype.Classification.DataSubjectCategory
import optimus.datatype.FullName
import optimus.platform.Compressed
import optimus.platform.Compressible
import optimus.platform.CovariantSet
import optimus.platform.ImmutableArray
import optimus.platform.cm.Knowable
import optimus.platform.cm.Known
import optimus.platform.cm.NotApplicable
import optimus.platform.cm.Unknown
import optimus.platform.dal.session.RolesetMode
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.InMemoryReferenceHolder
import optimus.platform.storable.ReferenceHolder

import scala.collection.immutable

class CompressedPickler[T, S <: AnyRef](pickler: ExplicitOutputPickler[T, S], compressible: Compressible[S])
    extends Pickler[Compressed[T]] {
  override def pickle(t: Compressed[T], out: PickledOutputStream) = {
    // HACK? What if the outer pickling stream has an Entity -> EntityRef context?
    val input = PropertyMapOutputStream.pickledValue(t.data, pickler)
    out.writeRawObject(compressible.compress(input.asInstanceOf[S]))
  }
}

class KnowablePickler[T](innerPickler: Pickler[T]) extends Pickler[Knowable[T]] {
  import KnowablePickler.Constants

  override def pickle(s: Knowable[T], visitor: PickledOutputStream) = {
    if (s eq null)
      throw new PicklingException("Cannot pickle null values")

    visitor.writeStartArray()
    s match {
      case Unknown()     => visitor.writeInt(Constants.Unknown)
      case NotApplicable => visitor.writeInt(Constants.NotApplicable)
      case Known(v) => {
        visitor.writeInt(Constants.Known)
        innerPickler.pickle(v, visitor)
      }
    }
    visitor.writeEndArray()
  }
}

object KnowablePickler {
  object Constants {
    val Unknown = 0
    val NotApplicable = 1
    val Known = 2
  }
}

class IdentityPickler[T] extends ExplicitOutputPickler[T, T] {
  def pickle(t: T, visitor: PickledOutputStream) = {
    val anyt = t.asInstanceOf[AnyRef]
    if (anyt eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeRawObject(anyt)
  }
}

object FloatPickler extends Pickler[Float] {
  def pickle(t: Float, visitor: PickledOutputStream) = visitor.writeRawObject(t.toDouble.asInstanceOf[AnyRef])
}

class OptionPickler[T](innerPickler: Pickler[T]) extends Pickler[Option[T]] {
  override def pickle(s: Option[T], visitor: PickledOutputStream) = {
    if (s eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    s foreach { innerPickler.pickle(_, visitor) }
    visitor.writeEndArray()
  }
}

object OptionPickler {
  def apply[T](innerPickler: Pickler[T]): Pickler[Option[T]] = {
    new OptionPickler[T](innerPickler)
  }
}

class CollectionPickler[T, Repr <: Traversable[T]](innerPickler: Pickler[T]) extends Pickler[Repr] {
  override def pickle(s: Repr, visitor: PickledOutputStream) = {
    if (s eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    s foreach { innerPickler.pickle(_, visitor) }
    visitor.writeEndArray()
  }
}

object UnitPickler extends Pickler[Unit] {
  override def pickle(s: Unit, visitor: PickledOutputStream) = {
    visitor.writeStartArray()
    visitor.writeEndArray()
  }
}

object EntityPickler extends Pickler[Entity] {
  override def pickle(s: Entity, visitor: PickledOutputStream) = {
    if (s eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeEntity(s)
  }
}

class JavaEnumPickler[E <: Enum[E]] extends Pickler[E] {

  override def pickle(data: E, visitor: PickledOutputStream) = {
    visitor.writeRawObject(data.name)
  }
}

class MSUUIdPickler extends Pickler[MSUuid] {
  override def pickle(id: MSUuid, visitor: PickledOutputStream) = {
    visitor.writeStartArray()
    visitor.writeBoolean(id.isTrueBase64())
    visitor.writeRawObject(ImmutableByteArray(id.asBytes()))
    visitor.writeEndArray()
  }
}

object ImmutableByteArrayPickler extends Pickler[ImmutableArray[Byte]] {
  override def pickle(data: ImmutableArray[Byte], visitor: PickledOutputStream) = {
    val mode = Compression.compressionMode(data.rawArray)
    visitor.writeStartArray()
    visitor.writeInt(mode)
    visitor.writeInt(data.size)
    visitor.writeRawObject(Compression.compress(mode, data.rawArray))
    visitor.writeEndArray()
  }
}

object OffsetTimePickler extends Pickler[OffsetTime] {
  private[optimus] val NanosPerSecond = 1000000000L

  override def pickle(t: OffsetTime, visitor: PickledOutputStream): Unit = {
    // We pickle into a list containing two parts:
    // - a long representing nanos since midnight when the LocalTime is adjusted to UTC
    // - an int representing offset seconds
    // we could technically pack all of this into a single long and still support strict ordering, but it would
    // inhibit our ability to support rich operators on the pickled value (such as isBefore/isAfter)
    val nanoOfDay = t.toLocalTime.toNanoOfDay
    val offsetSeconds = t.getOffset.getTotalSeconds
    val nanosSinceMidnight = nanoOfDay - (offsetSeconds * NanosPerSecond)
    visitor.writeStartArray()
    visitor.writeLong(nanosSinceMidnight)
    visitor.writeInt(offsetSeconds)
    visitor.writeEndArray()
  }
}

object RolesetModePickler extends Pickler[RolesetMode] {

  object Constants {
    val AllRoles = 0
    val SpecificRolesets = 1
    val UseLegacyEntitlements = 2
  }

  private val sortedSetPickler = DefaultPicklers.sortedSetPickler(DefaultPicklers.stringPickler)

  override def pickle(t: RolesetMode, visitor: PickledOutputStream): Unit = {
    visitor.writeStartArray()
    t match {
      case RolesetMode.AllRoles => visitor.writeInt(Constants.AllRoles)
      case RolesetMode.SpecificRoleset(underlying) =>
        visitor.writeInt(Constants.SpecificRolesets)
        sortedSetPickler.pickle(underlying, visitor)
      case RolesetMode.UseLegacyEntitlements => visitor.writeInt(Constants.UseLegacyEntitlements)
    }
    visitor.writeEndArray()
  }
}

trait PicklersLow2 extends TuplePicklers {
  def iterablePickler[T](innerPickler: Pickler[T]): Pickler[Iterable[T]] =
    new CollectionPickler[T, Iterable[T]](innerPickler)
}

trait PicklersLow1 extends PicklersLow2 {
  def enumPickler[T <: Enumeration]: Pickler[T#Value] = new Pickler[T#Value] {
    def pickle(t: T#Value, visitor: PickledOutputStream) = visitor.write(t.toString, DefaultPicklers.stringPickler)
  }

  def javaEnumPickler[E <: Enum[E]]: Pickler[E] = new JavaEnumPickler[E]

  def entityPickler[T <: Entity] = EntityPickler.asInstanceOf[Pickler[T]]
  def inonstringMapPickler[A, B](picklerA: Pickler[A], picklerB: Pickler[B]): Pickler[immutable.Map[A, B]] =
    new CollectionPickler[(A, B), immutable.Map[A, B]](tuple2pickler(picklerA, picklerB))
  def seqPickler[T](innerPickler: Pickler[T]): Pickler[Seq[T]] =
    new CollectionPickler[T, Seq[T]](innerPickler)
  def sortedMapPickler[A, B](picklerA: Pickler[A], picklerB: Pickler[B]): Pickler[immutable.SortedMap[A, B]] =
    new CollectionPickler[(A, B), immutable.SortedMap[A, B]](tuple2pickler(picklerA, picklerB))
  def listMapPickler[A, B](picklerA: Pickler[A], picklerB: Pickler[B]): Pickler[immutable.ListMap[A, B]] =
    new CollectionPickler[(A, B), immutable.ListMap[A, B]](tuple2pickler(picklerA, picklerB))
  def treeMapPickler[A, B](picklerA: Pickler[A], picklerB: Pickler[B]): Pickler[immutable.TreeMap[A, B]] =
    new CollectionPickler[(A, B), immutable.TreeMap[A, B]](tuple2pickler(picklerA, picklerB))
}

trait DefaultPicklers extends PicklersLow1 {
  import DateTimePicklers._

  val intPickler: Pickler[Int] = new IdentityPickler[Int]
  val longPickler: Pickler[Long] = new IdentityPickler[Long]
  val floatPickler: Pickler[Float] = FloatPickler
  val doublePickler: Pickler[Double] = new IdentityPickler[Double]
  val booleanPickler: Pickler[Boolean] = new IdentityPickler[Boolean]
  val stringPickler: ExplicitOutputPickler[String, String] = new IdentityPickler[String]
  // docs-snippet:DefaultPickler
  def fullNamePickler = new PIIElementPickler[DataSubjectCategory, FullName[DataSubjectCategory]](stringPickler)
  // docs-snippet:DefaultPickler
  def compressedPickler[T, S <: AnyRef](
      pickler: ExplicitOutputPickler[T, S],
      compressible: Compressible[S]): Pickler[Compressed[T]] = new CompressedPickler[T, S](pickler, compressible)
  def knowablePickler[T](innerPickler: Pickler[T]): Pickler[Knowable[T]] =
    new KnowablePickler[T](innerPickler)
  def optionPickler[T](innerPickler: Pickler[T]): Pickler[Option[T]] = OptionPickler[T](innerPickler)

  def unitPickler: Pickler[Unit] = UnitPickler

  def iseqPickler[T](innerPickler: Pickler[T]): Pickler[immutable.Seq[T]] =
    new CollectionPickler[T, immutable.Seq[T]](innerPickler)
  def indexedSeqPickler[T](innerPickler: Pickler[T]): Pickler[collection.IndexedSeq[T]] =
    new CollectionPickler[T, collection.IndexedSeq[T]](innerPickler)
  def vectorPickler[T](innerPickler: Pickler[T]): Pickler[Vector[T]] =
    new CollectionPickler[T, Vector[T]](innerPickler)
  def listPickler[T](innerPickler: Pickler[T]): Pickler[List[T]] =
    new CollectionPickler[T, List[T]](innerPickler)
  def setPickler[T](innerPickler: Pickler[T]): Pickler[collection.Set[T]] =
    new CollectionPickler[T, collection.Set[T]](innerPickler)
  def isetPickler[T](innerPickler: Pickler[T]): Pickler[immutable.Set[T]] =
    new CollectionPickler[T, immutable.Set[T]](innerPickler)
  def sortedSetPickler[T](innerPickler: Pickler[T]): Pickler[collection.SortedSet[T]] =
    new CollectionPickler[T, collection.SortedSet[T]](innerPickler)
  def isortedSetPickler[T](innerPickler: Pickler[T]): Pickler[immutable.SortedSet[T]] =
    new CollectionPickler[T, immutable.SortedSet[T]](innerPickler)

  val chainedIdPickler: Pickler[ChainedID] = new ChainedIDPickler
  val zdtPickler: Pickler[ZonedDateTime] = new IdentityPickler[ZonedDateTime]
  val localDatePickler: Pickler[LocalDate] = new IdentityPickler[LocalDate]
  val localTimePickler: Pickler[LocalTime] = new IdentityPickler[LocalTime]
  val zoneIdPickler: Pickler[ZoneId] = new IdentityPickler[ZoneId]
  val periodPickler: Pickler[Period] = new IdentityPickler[Period]
  val instantPickler: Pickler[Instant] = new InstantPickler
  val durationPickler: Pickler[Duration] = new DurationPickler
  val yearMonthPickler: Pickler[YearMonth] = new MonthPickler
  val yearPickler: Pickler[Year] = new YearPickler
  val offsetTimePickler: Pickler[OffsetTime] = OffsetTimePickler
  val rolesetModePickler: Pickler[RolesetMode] = RolesetModePickler

  val uuidPickler: Pickler[MSUuid] = new MSUUIdPickler

  // tentative
  val shortPickler: Pickler[Short] = new IdentityPickler[Short]
  val bytePickler: Pickler[Byte] = new IdentityPickler[Byte]
  val bdPickler: Pickler[BigDecimal] = new Pickler[BigDecimal] {
    def pickle(t: BigDecimal, stream: PickledOutputStream): Unit = {
      if (t == null) throw new PicklingException("Cannot pickle null values")
      stream.writeRawObject(t.toString)
    }
  }

  def ibytearrayPickler: Pickler[ImmutableArray[Byte]] = ImmutableByteArrayPickler
  def iarrayPickler[T](innerPickler: Pickler[T]): Pickler[ImmutableArray[T]] =
    new CollectionPickler[T, ImmutableArray[T]](innerPickler)

  val charPickler: Pickler[Char] = new Pickler[Char] {
    def pickle(c: Char, visitor: PickledOutputStream) = visitor.writeChar(c)
  }

  def covariantSetPickler[T](innerPickler: Pickler[T]): Pickler[CovariantSet[T]] =
    new CollectionPickler[T, CovariantSet[T]](innerPickler)

  // pickles an EntityReferenceHolder in exactly the same format as a direct entity reference, so you can switch a
  // field between E and EntityReferenceHolder[E] with no DAL schema change
  val referenceHolderPickler = new Pickler[ReferenceHolder[Entity]] {
    override def pickle(t: ReferenceHolder[Entity], visitor: PickledOutputStream): Unit = t match {
      // writeEntity will use the existing reference if the payload was already stored, else it will look for
      // a reference in its internal map in case the entity is written in the same transaction, else it will fail.
      case r: InMemoryReferenceHolder[_] => visitor.writeEntity(r.payload.asInstanceOf[Entity])
      // if we have a reference, use that directly to avoid unnecessarily retrieving the payload. Note that
      // writeRawObject is exactly what writeEntity does with the reference, so this produces the same output
      case e: EntityReferenceHolder[_] => visitor.writeRawObject(e.ref)
    }
  }
}

// Do not use directly.  Only needed if platform package object is not on your compile classpath.
private[optimus] object DefaultPicklers extends DefaultPicklers
