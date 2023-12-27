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
package optimus.platform.relational.dal.accelerated

import optimus.breadcrumbs.ChainedID
import optimus.datatype.FullName
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.cm.Knowable
import optimus.platform.pickling.DefaultPicklers._
import optimus.platform.pickling.DefaultPicklers.fullNamePickler
import optimus.platform.pickling.DefaultUnpicklers._
import optimus.platform.pickling._
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.core.DALExecutionProvider.RowReader
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.VersionedReference

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.Year
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZonedDateTime
import scala.collection.compat._
import scala.collection.Map
import scala.collection.SortedSet
import scala.collection.immutable
import scala.reflect.ManifestFactory
import scala.reflect.runtime.{currentMirror => cm}
import scala.reflect.runtime.{universe => ru}

object PicklerSelector {
  val IdentityPickler = new IdentityPickler[Any]
  val JavaEnumPickler = new JavaEnumPickler()

  val IdentityUnpickler = new IdentityUnpickler[Any]
  val EntityReferenceUnpickler = new IdentityUnpickler[EntityReference]
  val VersionedReferenceUnpickler = new IdentityUnpickler[VersionedReference]

  lazy val EmbeddableAnnotation = ru.typeOf[embeddable]

  def caseobjectPickler[T](pickler: Pickler[T]): Pickler[T] = new CaseObjectPickler(pickler)

  def getInnerUnpickler(unpickler: Unpickler[_]): Unpickler[_] = {
    unpickler match {
      case optionUnpickler: OptionUnpickler[_] => getInnerUnpickler(optionUnpickler.innerUnpickler)
      case _                                   => unpickler
    }
  }

  def getPickler(value: Any): Pickler[Any] = {
    val pickler = value match {
      case _: Int        => intPickler
      case _: String     => stringPickler
      case _: Double     => doublePickler
      case _: Boolean    => booleanPickler
      case _: Char       => charPickler
      case _: Long       => longPickler
      case _: Short      => shortPickler
      case _: Byte       => bytePickler
      case _: Float      => floatPickler
      case _: BigDecimal => bdPickler

      case _: ChainedID     => chainedIdPickler
      case _: ZonedDateTime => zdtPickler
      case _: LocalDate     => localDatePickler
      case _: LocalTime     => localTimePickler
      case _: OffsetTime    => offsetTimePickler
      case _: ZoneId        => zoneIdPickler
      case _: Period        => periodPickler
      case _: Instant       => instantPickler
      case _: Duration      => durationPickler
      case _: YearMonth     => yearMonthPickler
      case _: Year          => yearPickler

      case _: EntityReference      => IdentityPickler
      case e: Entity               => entityPickler
      case be: BusinessEvent       => BusinessEventPickler
      case enum: Enumeration#Value => enumPickler[Enumeration]
      case enum: Enum[_]           => JavaEnumPickler

      case map: Map[_, _] =>
        val (pickler1, pickler2) =
          if (map.isEmpty) (IdentityPickler, IdentityPickler)
          else
            (
              getPickler(map.keys.find(_ != None).getOrElse(None)),
              getPickler(map.values.find(_ != None).getOrElse(None)))
        getMapPickler(pickler1, pickler2, map)

      case iarr: ImmutableArray[_] if iarr.rawArray.isInstanceOf[Array[Byte]] => ibytearrayPickler

      case seq: Iterable[_] =>
        val pickler = if (seq.isEmpty) IdentityPickler else getPickler(seq.find(_ != None).getOrElse(None))
        getSeqPickler(pickler, seq)

      case opt: Option[_] =>
        val pickler = opt.map(t => getPickler(t)).getOrElse(IdentityPickler)
        optionPickler(pickler)

      case Compressed(data) =>
        require(data.isInstanceOf[String])
        compressedPickler(stringPickler, Compression.stringCompressible)

      case kn: Knowable[_] =>
        val pickler = kn.map(t => getPickler(t)).getOrElse(IdentityPickler)
        knowablePickler(pickler)

      case _: FullName[_] => fullNamePickler

      case _ =>
        // Tuple pickler
        getTuplePickler(value).getOrElse {
          // @embeddable final case class pickler
          val classSymbol = cm.classSymbol(value.getClass)
          if (classSymbol.annotations.exists(a => a.tree.tpe =:= EmbeddableAnnotation)) {
            val isCaseObject = classSymbol.companion == ru.NoSymbol
            if (isCaseObject) {
              val symbol = classSymbol.baseClasses
                .find(t => t.isAbstract && t.annotations.exists(a => a.tree.tpe =:= EmbeddableAnnotation))
                .get
              caseobjectPickler(Registry.picklerOfType(symbol.asType.toType))
            } else {
              Registry.picklerOfType(classSymbol.toType)
            }
          } else throw new RelationalUnsupportedException(s"Can't find Pickler for ${value.getClass()}")
        }
    }
    pickler.asInstanceOf[Pickler[Any]]
  }

  def getUnpickler(info: TypeInfo[_]): Unpickler[Any] = {
    val target = info.clazz

    val unpickler = target match {
      case _ if target == classOf[Int]        => intUnpickler
      case _ if target == classOf[String]     => stringUnpickler
      case _ if target == classOf[Double]     => doubleUnpickler
      case _ if target == classOf[Boolean]    => booleanUnpickler
      case _ if target == classOf[Char]       => charUnpickler
      case _ if target == classOf[Long]       => longUnpickler
      case _ if target == classOf[Short]      => shortUnpickler
      case _ if target == classOf[Byte]       => byteUnpickler
      case _ if target == classOf[Float]      => floatUnpickler
      case _ if target == classOf[BigDecimal] => bigdecimalUnpickler

      case _ if target == classOf[ChainedID]          => chainedIdUnpickler
      case _ if target == classOf[ZonedDateTime]      => zdtUnpickler
      case _ if target == classOf[LocalDate]          => localDateUnpickler
      case _ if target == classOf[LocalTime]          => localTimeUnpickler
      case _ if target == classOf[OffsetTime]         => offsetTimeUnpickler
      case _ if target == classOf[ZoneId]             => zoneIdUnpickler
      case _ if target == classOf[Period]             => periodUnpickler
      case _ if target == classOf[Instant]            => instantUnpickler
      case _ if target == classOf[Duration]           => durationUnpickler
      case _ if target == classOf[YearMonth]          => yearMonthUnpickler
      case _ if target == classOf[Year]               => yearUnpickler
      case _ if target == classOf[FullName[_]]        => fullNameUnpickler
      case _ if target == classOf[EntityReference]    => EntityReferenceUnpickler
      case _ if target == classOf[VersionedReference] => VersionedReferenceUnpickler
      case _ if target == classOf[BusinessEvent]      => BusinessEventUnpickler
      case _ if target.isEnum                         => javaEnumUnpickler(ManifestFactory.classType(target))
      case _ if info <:< classOf[Enumeration]         => enumUnpickler(ManifestFactory.classType(target))

      case _ if target == classOf[Map[_, _]] =>
        val unpickler1 = getUnpickler(info.typeParams(0))
        val unpickler2 = getUnpickler(info.typeParams(1))
        new CollectionUnpickler(new Tuple2Unpickler(unpickler1, unpickler2))(IndexedSeq)

      case _ if target == classOf[ImmutableArray[_]] =>
        val unpickler = getUnpickler(info.typeParams.head)
        if (unpickler == byteUnpickler) ibytearrayUnpickler
        else iarrayUnpickler(unpickler)

      case _ if info <:< classOf[Iterable[_]] =>
        val unpickler = getUnpickler(info.typeParams.head)
        getSeqUnpickler(unpickler, target)

      case _ if target == classOf[Option[_]] =>
        val unpickler = getUnpickler(info.typeParams.head)
        new OptionUnpickler(unpickler)

      case _ if info <:< classOf[Compressed[_]] =>
        require(info.typeParams.exists(_ <:< classOf[String]))
        compressedUnpickler(
          DefaultUnpicklers.stringUnpickler,
          DefaultPicklers.stringPickler,
          Compression.stringCompressible)

      case _ if info <:< classOf[Knowable[_]] =>
        info.typeParams.headOption
          .map(t => new KnowableUnpickler(getUnpickler(t))(ManifestFactory.classType(t.clazz)))
          .getOrElse(new KnowableUnpickler(IdentityUnpickler)(Manifest.Any))

      case _ if info <:< classOf[Entity] =>
        val manifest = ManifestFactory.classType[Entity](target)
        entityUnpickler(manifest)

      case _ =>
        // Tuple unpickler
        getTupleUnpickler(target, info).getOrElse {
          // @embeddabe case class pickler
          val classSymbol = cm.classSymbol(info.clazz)
          if (classSymbol.annotations.exists(a => a.tree.tpe =:= EmbeddableAnnotation)) {
            val isCaseObject = classSymbol.companion == ru.NoSymbol
            if (isCaseObject) {
              val symbol = classSymbol.baseClasses
                .find(t => t.isAbstract && t.annotations.exists(a => a.tree.tpe =:= EmbeddableAnnotation))
                .get
              Registry.unpicklerOfType(symbol.asType.toType)
            } else {
              Registry.unpicklerOfType(classSymbol.toType)
            }
          } else throw new RelationalUnsupportedException(s"Can't find Unpickler for ${info.runtimeClassName}")
        }
    }
    unpickler.asInstanceOf[Unpickler[Any]]
  }

  def getSeqPickler(pickler: Pickler[_], value: Any) = value match {
    case _: immutable.SortedSet[_] => isortedSetPickler(pickler)
    case _: SortedSet[_]           => sortedSetPickler(pickler)
    case _: Set[_]                 => setPickler(pickler)
    case _: CovariantSet[_]        => covariantSetPickler(pickler)
    case _: List[_]                => listPickler(pickler)
    case _: ImmutableArray[_]      => iarrayPickler(pickler)
    case _: IndexedSeq[_]          => indexedSeqPickler(pickler)
    case _: immutable.Seq[_]       => iseqPickler(pickler)
    case _: Seq[_]                 => seqPickler(pickler)
    case _                         => iterablePickler(pickler)
  }

  def getMapPickler(under1: Pickler[_], under2: Pickler[_], value: Any) = value match {
    case _: immutable.TreeMap[_, _]   => treeMapPickler(under1, under2)
    case _: immutable.SortedMap[_, _] => sortedMapPickler(under1, under2)
    case _: immutable.ListMap[_, _]   => listMapPickler(under1, under2)
    case _                            => inonstringMapPickler(under1, under2)
  }

  def getSeqUnpickler[T](unpickler: Unpickler[T], target: Class[_]) = target match {
    case _ if target == classOf[Set[_]]          => new CollectionUnpickler[T, Set[T]](unpickler)
    case _ if target == classOf[List[_]]         => new CollectionUnpickler[T, List[T]](unpickler)
    case _ if target == classOf[Seq[_]]          => new CollectionUnpickler[T, Seq[T]](unpickler)(IndexedSeq)
    case _ if target == classOf[CovariantSet[_]] => new CollectionUnpickler[T, CovariantSet[T]](unpickler)
    case _                                       => new CollectionUnpickler[T, Iterable[T]](unpickler)(IndexedSeq)
  }

  def getTuplePickler(value: Any): Option[Pickler[_]] = value match {
    case (t1, t2) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      Some(tuple2pickler(pickler1, pickler2))

    case (t1, t2, t3) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      Some(tuple3pickler(pickler1, pickler2, pickler3))

    case (t1, t2, t3, t4) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      Some(tuple4pickler(pickler1, pickler2, pickler3, pickler4))

    case (t1, t2, t3, t4, t5) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      Some(tuple5pickler(pickler1, pickler2, pickler3, pickler4, pickler5))

    case (t1, t2, t3, t4, t5, t6) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      val pickler6 = getPickler(t6)
      Some(tuple6pickler(pickler1, pickler2, pickler3, pickler4, pickler5, pickler6))

    case (t1, t2, t3, t4, t5, t6, t7) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      val pickler6 = getPickler(t6)
      val pickler7 = getPickler(t7)
      Some(tuple7pickler(pickler1, pickler2, pickler3, pickler4, pickler5, pickler6, pickler7))

    case (t1, t2, t3, t4, t5, t6, t7, t8) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      val pickler6 = getPickler(t6)
      val pickler7 = getPickler(t7)
      val pickler8 = getPickler(t8)
      Some(tuple8pickler(pickler1, pickler2, pickler3, pickler4, pickler5, pickler6, pickler7, pickler8))

    case (t1, t2, t3, t4, t5, t6, t7, t8, t9) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      val pickler6 = getPickler(t6)
      val pickler7 = getPickler(t7)
      val pickler8 = getPickler(t8)
      val pickler9 = getPickler(t9)
      Some(tuple9pickler(pickler1, pickler2, pickler3, pickler4, pickler5, pickler6, pickler7, pickler8, pickler9))

    case (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10) =>
      val pickler1 = getPickler(t1)
      val pickler2 = getPickler(t2)
      val pickler3 = getPickler(t3)
      val pickler4 = getPickler(t4)
      val pickler5 = getPickler(t5)
      val pickler6 = getPickler(t6)
      val pickler7 = getPickler(t7)
      val pickler8 = getPickler(t8)
      val pickler9 = getPickler(t9)
      val pickler10 = getPickler(t10)
      Some(
        tuple10pickler(
          pickler1,
          pickler2,
          pickler3,
          pickler4,
          pickler5,
          pickler6,
          pickler7,
          pickler8,
          pickler9,
          pickler10))
    case _ => None
  }

  def getTupleUnpickler(target: Class[_], info: TypeInfo[_]): Option[Unpickler[_]] = {
    def getTypeParamsUnpickler(typeParamsCount: Int): Vector[Unpickler[_]] = {
      if (info.typeParams.length != typeParamsCount)
        throw new RelationalUnsupportedException(
          s"Expect $typeParamsCount typeParams in ${info.runtimeClassName} but got ${info.typeParams.length}")
      info.typeParams.iterator.map(getUnpickler).toVector
    }

    target match {
      case _ if target == classOf[Tuple2[_, _]] =>
        val unpicklers = getTypeParamsUnpickler(2)
        Some(new Tuple2Unpickler(unpicklers(0), unpicklers(1)))

      case _ if target == classOf[Tuple3[_, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(3)
        Some(new Tuple3Unpickler(unpicklers(0), unpicklers(1), unpicklers(2)))

      case _ if target == classOf[Tuple4[_, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(4)
        Some(new Tuple4Unpickler(unpicklers(0), unpicklers(1), unpicklers(2), unpicklers(3)))

      case _ if target == classOf[Tuple5[_, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(5)
        Some(new Tuple5Unpickler(unpicklers(0), unpicklers(1), unpicklers(2), unpicklers(3), unpicklers(4)))

      case _ if target == classOf[Tuple6[_, _, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(6)
        Some(
          new Tuple6Unpickler(unpicklers(0), unpicklers(1), unpicklers(2), unpicklers(3), unpicklers(4), unpicklers(5)))

      case _ if target == classOf[Tuple7[_, _, _, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(7)
        Some(
          new Tuple7Unpickler(
            unpicklers(0),
            unpicklers(1),
            unpicklers(2),
            unpicklers(3),
            unpicklers(4),
            unpicklers(5),
            unpicklers(6)))

      case _ if target == classOf[Tuple8[_, _, _, _, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(8)
        Some(
          new Tuple8Unpickler(
            unpicklers(0),
            unpicklers(1),
            unpicklers(2),
            unpicklers(3),
            unpicklers(4),
            unpicklers(5),
            unpicklers(6),
            unpicklers(7)))

      case _ if target == classOf[Tuple9[_, _, _, _, _, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(9)
        Some(
          new Tuple9Unpickler(
            unpicklers(0),
            unpicklers(1),
            unpicklers(2),
            unpicklers(3),
            unpicklers(4),
            unpicklers(5),
            unpicklers(6),
            unpicklers(7),
            unpicklers(8)))

      case _ if target == classOf[Tuple10[_, _, _, _, _, _, _, _, _, _]] =>
        val unpicklers = getTypeParamsUnpickler(10)
        Some(
          new Tuple10Unpickler(
            unpicklers(0),
            unpicklers(1),
            unpicklers(2),
            unpicklers(3),
            unpicklers(4),
            unpicklers(5),
            unpicklers(6),
            unpicklers(7),
            unpicklers(8),
            unpicklers(9)))
      case _ => None
    }
  }
}

private[accelerated] class CaseObjectPickler[T](pickler: Pickler[T]) extends Pickler[T] {
  def pickle(s: T, visitor: PickledOutputStream) = {
    pickler.pickle(s, visitor)
  }
}

private[accelerated] class AggColumnUnpickler(length: Int) extends Unpickler[FieldReader] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): FieldReader = {
    if (length == 1) new RowReader(Array(pickled), ctxt)
    else
      pickled match {
        case values: Iterable[_] => new RowReader(values.toArray, ctxt)
        case _ =>
          throw new RelationalUnsupportedException(s"Can't unpickle value $pickled since an Iterable is expected.")
      }
  }
}
