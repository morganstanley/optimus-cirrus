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

import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.pickling.ChainedIDUnpickler
import optimus.datatype.Classification.DataSubjectCategory
import optimus.datatype.FullName
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.cm.Knowable
import optimus.platform.cm.Known
import optimus.platform.cm.NotApplicable
import optimus.platform.cm.Unknown
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dal.session.RolesetMode
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.ModuleEntityToken
import optimus.platform.storable.ReferenceHolder
import optimus.platform.temporalSurface.operations.EntityReferenceQueryReason
import optimus.scalacompat.collection._

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.Year
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import scala.collection.compat._
import scala.collection.compat.{Factory => ScalaFactory}
import scala.collection.immutable
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class CompressedUnpickler[T, S <: AnyRef](
    unpickler: Unpickler[T],
    pickler: ExplicitOutputPickler[T, S],
    compressible: Compressible[S])
    extends Unpickler[Compressed[T]] {
  @node def unpickle(data: Any, in: PickledInputStream): Compressed[T] = {
    val input = compressible.decompress(data)
    Compressed(unpickler.unpickle(input, in))
  }
}

class KnowableUnpickler[T: Manifest](private[optimus] val innerUnpickler: Unpickler[T]) extends Unpickler[Knowable[T]] {
  import KnowablePickler.Constants

  @node def unpickle(pickled: Any, ctxt: PickledInputStream): Knowable[T] = pickled match {
    case s: Seq[_] if s.length == 1 && s(0).asInstanceOf[Int] == Constants.NotApplicable => NotApplicable
    case s: Seq[_] if s.length == 1 && s(0).asInstanceOf[Int] == Constants.Unknown       => Unknown()
    case s: Seq[_] if s.length == 2 && s(0).asInstanceOf[Int] == Constants.Known =>
      Known(innerUnpickler.unpickle(s(1), ctxt))
    case o => throw new UnexpectedPickledTypeException(implicitly[Manifest[Knowable[T]]], pickled.getClass)
  }
}

class IdentityUnpickler[T: Manifest] extends Unpickler[T] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): T = pickled match {
    case _ if implicitly[Manifest[T]].runtimeClass == pickled.getClass =>
      pickled.asInstanceOf[T]
    case _ => throw new UnexpectedPickledTypeException(implicitly[Manifest[T]], pickled.getClass)
  }
}

object IntUnpickler extends Unpickler[Int] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Int = pickled match {
    case i: Int   => i
    case s: Short => s.toInt
    case _        => throw new UnexpectedPickledTypeException(Manifest.Int, pickled.getClass)
  }
}

object LongUnpickler extends Unpickler[Long] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Long = pickled match {
    case l: Long  => l
    case i: Int   => i.toLong
    case s: Short => s.toLong
    case _        => throw new UnexpectedPickledTypeException(Manifest.Long, pickled.getClass)
  }
}

sealed trait NumericBoundConstants {
  protected val Infinity: String = "Infinity"
  protected val NegInfinity: String = "-Infinity"
  protected val NaN: String = "NaN"
}

object DoubleUnpickler extends Unpickler[Double] with NumericBoundConstants {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Double = pickled match {
    case d: Double => d
    case f: Float  => f.toDouble
    case i: Int    => i.toDouble
    case s: Short  => s.toDouble
    case l: Long   => l.toDouble
    case s: String if s == Infinity || s == NegInfinity || s == NaN => {
      s match {
        case Infinity    => Double.PositiveInfinity
        case NegInfinity => Double.NegativeInfinity
        case NaN         => Double.NaN
      }
    }
    case _ => throw new UnexpectedPickledTypeException(Manifest.Double, pickled.getClass)
  }
}

object FloatUnpickler extends Unpickler[Float] with NumericBoundConstants {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Float = pickled match {
    case f: Float  => f
    case d: Double => d.toFloat
    case s: Short  => s.toFloat
    case l: Long   => l.toFloat
    case s: String if s == Infinity || s == NegInfinity || s == NaN => {
      s match {
        case Infinity    => Float.PositiveInfinity
        case NegInfinity => Float.NegativeInfinity
        case NaN         => Float.NaN
      }
    }
    case _ => throw new UnexpectedPickledTypeException(Manifest.Float, pickled.getClass)
  }
}

object BooleanUnpickler extends Unpickler[Boolean] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Boolean = pickled match {
    case b: Boolean => b
    case _          => throw new UnexpectedPickledTypeException(Manifest.Boolean, pickled.getClass)
  }
}

object ShortUnpickler extends Unpickler[Short] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
    case l: Long  => l.toShort
    case i: Int   => i.toShort
    case s: Short => s.toShort
    case _        => throw new UnexpectedPickledTypeException(Manifest.Short, pickled.getClass)
  }
}

object CharUnpickler extends Unpickler[Char] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
    case l: Long => l.toChar
    case i: Int  => i.toChar
    case s: Char => s
    case _       => throw new UnexpectedPickledTypeException(Manifest.Char, pickled.getClass)
  }
}

object ByteUnpickler extends Unpickler[Byte] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
    case l: Long  => l.toByte
    case i: Int   => i.toByte
    case s: Short => s.toByte
    case b: Byte  => b
    case _        => throw new UnexpectedPickledTypeException(Manifest.Long, pickled.getClass)
  }
}

object BigDecimalUnpickler extends Unpickler[BigDecimal] {
  @nodeSync def unpickle(t: Any, stream: PickledInputStream): BigDecimal = t match {
    case bigDec: BigDecimal => bigDec
    case str: String        => BigDecimal(str)
    case _                  => throw new UnexpectedPickledTypeException(implicitly[Manifest[BigDecimal]], t.getClass)
  }
}

class OptionUnpickler[T: Manifest](val innerUnpickler: Unpickler[T]) extends Unpickler[Option[T]] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): Option[T] = pickled match {
    case Some(v)                    => Some(innerUnpickler.unpickle(v, ctxt))
    case None                       => None
    case null                       => None
    case s: Seq[_] if s.length == 1 => Some(innerUnpickler.unpickle(s.head, ctxt))
    case s: Seq[_] if s.length == 0 => None
    case r: AnyRef                  => Some(innerUnpickler.unpickle(r, ctxt))
    case o => throw new UnexpectedPickledTypeException(implicitly[Manifest[Option[T]]], pickled.getClass)
  }
}

object UnitUnpickler extends Unpickler[Unit] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): Unit = ()
}

class CollectionUnpickler[T, Impl <: Iterable[T]](private[optimus] val innerUnpickler: Unpickler[T])(implicit
    factory: ScalaFactory[T, Impl])
    extends Unpickler[Impl] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): Impl = pickled match {
    case i: Iterable[a] =>
      i.apar.map { n =>
        innerUnpickler.unpickle(n, ctxt)
      }(factory.breakOut)
    case a: Any =>
      import scala.collection.compat._
      val b = factory.newBuilder
      b += innerUnpickler.unpickle(a, ctxt)
      b.result()
  }
}

class EntityUnpickler[T <: Entity: Manifest] extends Unpickler[T] {
  @node def unpickle(s: Any, ctxt: PickledInputStream): T = s match {
    case ref: EntityReference =>
      val inlined = ctxt.inlineEntitiesByRef.get(ref)
      val resolver = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl]
      val actualType = manifest[T].runtimeClass.asSubclass(classOf[Entity])
      inlined
        .getOrElse(resolver
          .findByReferenceWithType(ref, ctxt.temporalContext, actualType, false, EntityReferenceQueryReason.Unpickling))
        .asInstanceOf[T]
    // We can get raw entities here when dealing with Java Serialization of temporary entity graphs for dist.
    case ent: T =>
      ent
    case m: ModuleEntityToken => m.resolve.asInstanceOf[T]
    case s                    => throw new UnpickleException(s, manifest[T].runtimeClass.toString)
  }
}

class JavaEnumUnpickler[E <: Enum[E]: Manifest] extends Unpickler[E] {
  private[this] val manifestE = manifest[E]

  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
    case name: String => Enum.valueOf(manifestE.runtimeClass.asInstanceOf[Class[E]], name)
    case _            => throw new UnexpectedPickledTypeException(implicitly[Manifest[Array[Any]]], pickled.getClass)
  }
}

class MSUUidUnpickler extends Unpickler[MSUuid] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
    case buf: Seq[_] =>
      val is64Bit = buf(0).asInstanceOf[Boolean]
      val data = buf(1).asInstanceOf[ImmutableByteArray].data

      new MSUuid(data, is64Bit)
  }
}

class EnumerationUnpickler[T <: Enumeration: Manifest] extends Unpickler[T#Value] {
  val klass = implicitly[Manifest[T]].runtimeClass
  lazy val module = klass.getField("MODULE$").get(klass).asInstanceOf[T]

  @nodeSync def unpickle(t: Any, ctxt: PickledInputStream) = {
    if (classOf[T#Value].isAssignableFrom(t.getClass))
      t.asInstanceOf[T#Value]
    else
      try {
        module.withName(t.asInstanceOf[String])
      } catch {
        case ex: NoSuchElementException =>
          val msg = "Cannot find enumeration with name %s in %s during unpickling.".format(t, klass.getName)
          throw new NoSuchElementException(msg)
      }
  }
}

object ImmutableByteArrayUnpickler extends Unpickler[ImmutableArray[Byte]] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) =
    ImmutableArray.wrapped[Byte](Compression.decompressTaggedArray(pickled))
}

object OffsetTimeUnpickler extends Unpickler[OffsetTime] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): OffsetTime = pickled match {
    case Seq(utcNanosSinceMidnight: Long, offsetSecs: Int) =>
      // More information about the choice of pickled format in the pickler declaration, but in brief it is a list
      // consisting of:
      // - a long representing nanos since midnight when the LocalTime is adjusted to UTC
      // - an int representing offset seconds
      // To unpickle, we need to get the original, non-adjusted LocalTime back by adding the offset on.
      val localTimeNanos = utcNanosSinceMidnight + (offsetSecs * OffsetTimePickler.NanosPerSecond)
      val localTime = LocalTime.ofNanoOfDay(localTimeNanos)
      val offset = ZoneOffset.ofTotalSeconds(offsetSecs)
      OffsetTime.of(localTime, offset)
    case o => throw new IllegalArgumentException(s"Cannot unpickle OffsetTime from raw value of type ${o.getClass}: $o")
  }
}

object RolesetModeUnpickler extends Unpickler[RolesetMode] {
  import RolesetModePickler.Constants

  private val sortedSetUnpickler = DefaultUnpicklers.sortedSetUnpickler(DefaultUnpicklers.stringUnpickler)

  @node def unpickle(pickled: Any, ctxt: PickledInputStream): RolesetMode = pickled match {
    case Seq(Constants.AllRoles) => RolesetMode.AllRoles
    case Seq(Constants.SpecificRolesets, underlying) =>
      RolesetMode.SpecificRoleset(sortedSetUnpickler.unpickle(underlying, ctxt))
    case Seq(Constants.UseLegacyEntitlements) => RolesetMode.UseLegacyEntitlements
    case o =>
      throw new IllegalArgumentException(s"Cannot unpickle RolesetMode from raw value of type ${o.getClass}: $o")
  }
}

trait UnpicklersLow1 extends TupleUnpicklers {
  def entityUnpickler[T <: Entity: Manifest]: Unpickler[T] =
    (new EntityUnpickler[T]).asInstanceOf[Unpickler[T]]
  def enumUnpickler[T <: Enumeration: Manifest]: Unpickler[T#Value] = new EnumerationUnpickler[T]
  def javaEnumUnpickler[E <: Enum[E]: Manifest]: Unpickler[E] = new JavaEnumUnpickler[E]

  def nonstringMapUnpickler[A: Manifest, B: Manifest](
      under1: Unpickler[A],
      under2: Unpickler[B]): Unpickler[Map[A, B]] = {
    new CollectionUnpickler[(A, B), Map[A, B]](tuple2Unpickler(under1, under2))
  }

  def collUnpickler[T, Impl <: Iterable[T]](
      under: Unpickler[T])(implicit manifest: Manifest[T], factory: ScalaFactory[T, Impl]): Unpickler[Impl] =
    new CollectionUnpickler[T, Impl](under)
}

trait DefaultUnpicklers extends UnpicklersLow1 {
  import DateTimeUnpicklers._

  val intUnpickler: Unpickler[Int] = IntUnpickler
  val longUnpickler: Unpickler[Long] = LongUnpickler
  val floatUnpickler: Unpickler[Float] = FloatUnpickler
  val doubleUnpickler: Unpickler[Double] = DoubleUnpickler
  val booleanUnpickler: Unpickler[Boolean] = BooleanUnpickler
  val shortUnpickler: Unpickler[Short] = ShortUnpickler
  val byteUnpickler: Unpickler[Byte] = ByteUnpickler
  val charUnpickler: Unpickler[Char] = CharUnpickler
  val stringUnpickler: Unpickler[String] = new IdentityUnpickler[String]

  val bigdecimalUnpickler: Unpickler[BigDecimal] = BigDecimalUnpickler

  def compressedUnpickler[T, S <: AnyRef](
      unpickler: Unpickler[T],
      pickler: ExplicitOutputPickler[T, S],
      compressible: Compressible[S]): Unpickler[Compressed[T]] =
    new CompressedUnpickler[T, S](unpickler, pickler, compressible)
  def knowableUnpickler[T](under: Unpickler[T], manifest: Manifest[T]): Unpickler[Knowable[T]] =
    new KnowableUnpickler(under)(manifest)
  def optionUnpickler[T](under: Unpickler[T], manifest: Manifest[T]): Unpickler[Option[T]] =
    new OptionUnpickler(under)(manifest)
  // docs-snippet:DefaultUnpickler
  def fullNameUnpickler = new PIIElementUnpickler[DataSubjectCategory, FullName[DataSubjectCategory]](FullName.apply)
  // docs-snippet:DefaultUnpickler
  def unitUnpickler: Unpickler[Unit] = UnitUnpickler

  def seqUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[Seq[T]] =
    collUnpickler[T, Seq[T]](inner)(manifest[T], IndexedSeq)
  def iseqUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[immutable.Seq[T]] =
    collUnpickler[T, immutable.Seq[T]](inner)(manifest[T], immutable.IndexedSeq)

  def indexedSeqUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[IndexedSeq[T]] =
    collUnpickler[T, IndexedSeq[T]](inner)(manifest[T], IndexedSeq)
  def linearSeqUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[immutable.LinearSeq[T]] =
    collUnpickler[T, immutable.LinearSeq[T]](inner)
  def vectorUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[Vector[T]] = collUnpickler[T, Vector[T]](inner)
  def listUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[List[T]] = collUnpickler[T, List[T]](inner)
  def setUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[Set[T]] = collUnpickler[T, Set[T]](inner)
  def sortedSetUnpickler[T: Manifest: Ordering](inner: Unpickler[T]): Unpickler[immutable.SortedSet[T]] =
    collUnpickler[T, immutable.SortedSet[T]](inner)
  def iterableUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[Iterable[T]] =
    collUnpickler[T, Iterable[T]](inner)(manifest[T], IndexedSeq)
  def sortedMapUnpickler[A: Manifest: Ordering, B: Manifest](
      under1: Unpickler[A],
      under2: Unpickler[B]): Unpickler[SortedMap[A, B]] = {
    val inner = tuple2Unpickler(under1, under2)
    new CollectionUnpickler[(A, B), SortedMap[A, B]](inner)
  }

  def listMapUnpickler[A: Manifest, B: Manifest](
      under1: Unpickler[A],
      under2: Unpickler[B]): Unpickler[immutable.ListMap[A, B]] = {
    val inner = tuple2Unpickler(under1, under2)
    new CollectionUnpickler[(A, B), immutable.ListMap[A, B]](inner)
  }
  def treeMapUnpickler[T: Manifest: Ordering, V: Manifest](
      innerT: Unpickler[T],
      innerV: Unpickler[V]): Unpickler[TreeMap[T, V]] = {
    val inner = tuple2Unpickler(innerT, innerV)
    collUnpickler[(T, V), immutable.TreeMap[T, V]](inner)
  }

  def ibytearrayUnpickler: Unpickler[ImmutableArray[Byte]] = ImmutableByteArrayUnpickler
  def iarrayUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[ImmutableArray[T]] =
    collUnpickler[T, ImmutableArray[T]](inner)(implicitly[Manifest[T]], ImmutableArray)

  val chainedIdUnpickler: Unpickler[ChainedID] = new ChainedIDUnpickler

  val zdtUnpickler: Unpickler[ZonedDateTime] = new IdentityUnpickler[ZonedDateTime]
  val localDateUnpickler: Unpickler[LocalDate] = LocalDateUnpickler
  val localTimeUnpickler: Unpickler[LocalTime] = new IdentityUnpickler[LocalTime]
  val zoneIdUnpickler: Unpickler[ZoneId] = ZoneIdUnpickler
  val periodUnpickler: Unpickler[Period] = new IdentityUnpickler[Period]
  val instantUnpickler: Unpickler[Instant] = new InstantUnpickler
  val durationUnpickler: Unpickler[Duration] = new DurationUnpickler
  val yearMonthUnpickler: Unpickler[YearMonth] = new MonthUnpickler
  val yearUnpickler: Unpickler[Year] = new YearUnpickler
  val offsetTimeUnpickler: Unpickler[OffsetTime] = OffsetTimeUnpickler
  val rolesetModeUnpickler: Unpickler[RolesetMode] = RolesetModeUnpickler

  val msuuidUnpickler: Unpickler[MSUuid] = new MSUUidUnpickler

  def covariantSetUnpickler[T: Manifest](inner: Unpickler[T]): Unpickler[CovariantSet[T]] =
    collUnpickler[T, CovariantSet[T]](inner)

  // unpickles an EntityReferenceHolder from exactly the same format as a direct entity reference, so you can switch a
  // field between E and EntityReferenceHolder[E] with no DAL schema change
  val referenceHolderUnpickler: Unpickler[ReferenceHolder[Entity]] = new Unpickler[ReferenceHolder[Entity]] {
    @node override def unpickle(pickled: Any, ctxt: PickledInputStream): EntityReferenceHolder[Entity] =
      EntityReferenceHolder(pickled.asInstanceOf[EntityReference], ctxt.temporalContext)
  }
}

// Do not use directly.  Only needed if platform package object is not on your compile classpath.
private[optimus] object DefaultUnpicklers extends DefaultUnpicklers
