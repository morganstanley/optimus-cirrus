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
package optimus.platform.versioning

import java.time.Duration
import java.time.Instant
import java.time._
import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.datatype.Classification.DataSubjectCategory
import optimus.datatype.PIIElement
import optimus.graph.Node
import optimus.platform.ImmutableArray
import optimus.platform._
import optimus.platform.storable._

import scala.collection.immutable.ListMap
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeMap
import scala.reflect.api.Universe

private[optimus] trait VersioningUtilsBase {
  type U <: Universe with Singleton
  val u: U
  import u._

  implicit class TypeOps(val t: Type) {
    def getUnderlying: Type = t match {
      case n: NullaryMethodTypeApi =>
        val resultType = n.resultType
        resultType match {
          case a: AnnotatedTypeApi => a.underlying
          case o                   => o
        }
      case c: ConstantTypeApi =>
        c.value.tpe
      case o => o
    }

    def fieldType: RegisteredFieldType = {
      val typeSig = getUnderlying.dealias
      val typeSym = typeSig.erasure.typeSymbol
      val typeArgs = typeSig.typeArgs.toVector
      val typeName = typeSym.fullName

      typeSym.initialize()

      if (typeSym.hasAnnotation[entity]) RegisteredFieldType.EntityReference(typeName)
      else if (typeSym.hasAnnotation[event]) RegisteredFieldType.BusinessEventReference(typeName)
      else if (typeSym.hasAnnotation[embeddable]) {
        typeSym.annotations
          .map(_.tree)
          .collectFirst { case q"new $embeddable(customPickling=$_)" =>
            RegisteredFieldType.Unknown(typeName)
          }
          .getOrElse {
            RegisteredFieldType.Embeddable(typeName)
          }
      } else if (typeSig <:< types.referenceHolder && typeArgs.size == 1)
        RegisteredFieldType.ReferenceHolder(typeArgs.head.typeSymbol.fullName)
      else if (typeSig <:< types.boolean || typeSig <:< types.javaBoolean) RegisteredFieldType.Boolean
      else if (typeSig <:< types.byte || typeSig <:< types.javaByte) RegisteredFieldType.Byte
      else if (typeSig <:< types.char || typeSig <:< types.javaChar) RegisteredFieldType.Char
      else if (typeSig <:< types.double || typeSig <:< types.javaDouble) RegisteredFieldType.Double
      else if (typeSig <:< types.float || typeSig <:< types.javaFloat) RegisteredFieldType.Float
      else if (typeSig <:< types.int || typeSig <:< types.javaInt) RegisteredFieldType.Int
      else if (typeSig <:< types.long || typeSig <:< types.javaLong) RegisteredFieldType.Long
      else if (typeSig <:< types.short || typeSig <:< types.javaShort) RegisteredFieldType.Short
      else if (typeSig <:< types.string || typeSig <:< types.javaString) RegisteredFieldType.String
      else if (typeSig <:< types.unit) RegisteredFieldType.Unit
      else if (typeSig <:< types.bigDecimal) RegisteredFieldType.BigDecimal
      else if (typeSig <:< types.msUuid) RegisteredFieldType.MsUuid
      else if (typeSig <:< types.chainedId) RegisteredFieldType.ChainedID
      else if (typeSig <:< types.msUnique) RegisteredFieldType.MsUnique
      else if (typeSig <:< types.javaEnum) RegisteredFieldType.JavaEnum(typeName)
      else if (typeSig <:< types.enumerationValue) RegisteredFieldType.ScalaEnum(typeName)
      else if (typeSig <:< types.piiElement) RegisteredFieldType.PIIElement(typeName)
      else if (typeSig <:< types.tuple2 && typeArgs.size == 2) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        RegisteredFieldType.Tuple2(t1, t2)
      } else if (typeSig <:< types.tuple3 && typeArgs.size == 3) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        RegisteredFieldType.Tuple3(t1, t2, t3)
      } else if (typeSig <:< types.tuple4 && typeArgs.size == 4) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        RegisteredFieldType.Tuple4(t1, t2, t3, t4)
      } else if (typeSig <:< types.tuple5 && typeArgs.size == 5) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        RegisteredFieldType.Tuple5(t1, t2, t3, t4, t5)
      } else if (typeSig <:< types.tuple6 && typeArgs.size == 6) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        val t6 = typeArgs(5).fieldType
        RegisteredFieldType.Tuple6(t1, t2, t3, t4, t5, t6)
      } else if (typeSig <:< types.tuple7 && typeArgs.size == 7) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        val t6 = typeArgs(5).fieldType
        val t7 = typeArgs(6).fieldType
        RegisteredFieldType.Tuple7(t1, t2, t3, t4, t5, t6, t7)
      } else if (typeSig <:< types.tuple8 && typeArgs.size == 8) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        val t6 = typeArgs(5).fieldType
        val t7 = typeArgs(6).fieldType
        val t8 = typeArgs(7).fieldType
        RegisteredFieldType.Tuple8(t1, t2, t3, t4, t5, t6, t7, t8)
      } else if (typeSig <:< types.tuple9 && typeArgs.size == 9) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        val t6 = typeArgs(5).fieldType
        val t7 = typeArgs(6).fieldType
        val t8 = typeArgs(7).fieldType
        val t9 = typeArgs(8).fieldType
        RegisteredFieldType.Tuple9(t1, t2, t3, t4, t5, t6, t7, t8, t9)
      } else if (typeSig <:< types.tuple10 && typeArgs.size == 10) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        val t3 = typeArgs(2).fieldType
        val t4 = typeArgs(3).fieldType
        val t5 = typeArgs(4).fieldType
        val t6 = typeArgs(5).fieldType
        val t7 = typeArgs(6).fieldType
        val t8 = typeArgs(7).fieldType
        val t9 = typeArgs(8).fieldType
        val t10 = typeArgs(9).fieldType
        RegisteredFieldType.Tuple10(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10)
      } else if (typeSig <:< types.instant) RegisteredFieldType.Instant
      else if (typeSig <:< types.period) RegisteredFieldType.Period
      else if (typeSig <:< types.zonedDateTime) RegisteredFieldType.ZonedDateTime
      else if (typeSig <:< types.duration) RegisteredFieldType.Duration
      else if (typeSig <:< types.yearMonth) RegisteredFieldType.YearMonth
      else if (typeSig <:< types.year) RegisteredFieldType.Year
      else if (typeSig <:< types.localDate) RegisteredFieldType.LocalDate
      else if (typeSig <:< types.localTime) RegisteredFieldType.LocalTime
      else if (typeSig <:< types.offsetTime) RegisteredFieldType.OffsetTime
      else if (typeSig <:< types.zoneId) RegisteredFieldType.ZoneId
      else if (typeSig <:< types.listMap && typeArgs.size == 2) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        RegisteredFieldType.ListMap(t1, t2)
      } else if (typeSig <:< types.treeMap && typeArgs.size == 2) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        RegisteredFieldType.TreeMap(t1, t2)
      } else if (typeSig <:< types.map && typeArgs.size == 2) {
        val t1 = typeArgs(0).fieldType
        val t2 = typeArgs(1).fieldType
        RegisteredFieldType.Map(t1, t2)
      } else if (typeSig <:< types.option && typeArgs.size == 1)
        RegisteredFieldType.Option(typeArgs(0).fieldType)
      else if (typeSig.baseClasses.exists(_.fullName == "optimus.platform.Compressed") && typeArgs.size == 1)
        RegisteredFieldType.Compressed(typeArgs(0).fieldType)
      else if ((typeSig <:< types.array) && typeArgs.size == 1)
        RegisteredFieldType.Array(typeArgs(0).fieldType)
      else if (typeSig <:< types.immutableArray && typeArgs.size == 1)
        RegisteredFieldType.ImmutableArray(typeArgs(0).fieldType)
      else if (typeSig <:< types.seq && typeArgs.size == 1)
        RegisteredFieldType.Seq(typeArgs(0).fieldType)
      else if (typeSig.baseClasses.exists(_.fullName == "optimus.platform.CovariantSet") && typeArgs.size == 1)
        RegisteredFieldType.CovariantSet(typeArgs(0).fieldType)
      else if (typeSig <:< types.sortedSet && typeArgs.size == 1)
        RegisteredFieldType.SortedSet(typeArgs(0).fieldType)
      else if (typeSig <:< types.set && typeArgs.size == 1)
        RegisteredFieldType.Set(typeArgs(0).fieldType)
      else if (typeSig <:< types.iterable && typeArgs.size == 1) {
        // for now at least not even trying to figure out if it should be OrderedCollection or UnorderedCollection
        RegisteredFieldType.Collection(typeArgs(0).fieldType, typeName)
      } else if (typeSig.baseClasses.exists(_.fullName == "optimus.platform.cm.Knowable") && typeArgs.size == 1)
        RegisteredFieldType.Knowable(typeArgs(0).fieldType)
      else if (typeSig <:< types.product) RegisteredFieldType.Product(typeName)
      else RegisteredFieldType.Unknown(typeName)
    }
  }

  implicit class NameOps(val t: Name) {
    def stringify: String = t.decodedName.toString.trim
  }

  implicit class SymbolOps(val s: Symbol) {
    def hasAnnotation[T: WeakTypeTag]: Boolean = {
      s.annotations.exists(_.tree.tpe =:= weakTypeOf[T])
    }

    // true if the symbol is a nullary method whose return type is annotated with T
    def isNullaryMethodReturningAnnotatedType[T: WeakTypeTag]: Boolean = {
      s.typeSignature match {
        case n: NullaryMethodTypeApi =>
          n.resultType match {
            case a: AnnotatedTypeApi =>
              a.annotations.exists(_.tree.tpe =:= weakTypeOf[T])
            case _ =>
              false
          }
        case _ =>
          false
      }
    }

    def typeName: Name = s.typeSignature.typeSymbol.name

    def fieldType: RegisteredFieldType = {
      s.typeSignature.fieldType
    }

    /** Run the type completer for this symbol. Obviously dangerous, but otherwise you might not see annotations. */
    def initialize(): Unit = { s.info; () }
  }

  protected object types {
    lazy val anyRef: Type = typeOf[AnyRef]
    lazy val array: Type = typeOf[Array[_]]
    lazy val bigDecimal: Type = typeOf[BigDecimal]
    lazy val boolean: Type = typeOf[Boolean]
    lazy val byte: Type = typeOf[Byte]
    lazy val businessEvent: Type = typeOf[BusinessEvent]
    lazy val char: Type = typeOf[Char]
    lazy val double: Type = typeOf[Double]
    lazy val duration: Type = typeOf[Duration]
    lazy val float: Type = typeOf[Float]
    lazy val entity: Type = typeOf[Entity]
    lazy val entityCompanionBase: Type = typeOf[EntityCompanionBase[_]]
    lazy val enumerationValue: Type = typeOf[Enumeration#Value]
    lazy val eventCompanionBase: Type = typeOf[EventCompanionBase[_]]
    lazy val immutableArray: Type = typeOf[ImmutableArray[_]]
    lazy val instant: Type = typeOf[Instant]
    lazy val int: Type = typeOf[Int]
    lazy val iterable: Type = typeOf[Iterable[_]]
    lazy val javaBoolean: Type = typeOf[java.lang.Boolean]
    lazy val javaByte: Type = typeOf[java.lang.Byte]
    lazy val javaChar: Type = typeOf[java.lang.Character]
    lazy val javaDouble: Type = typeOf[java.lang.Double]
    lazy val javaEnum: Type = typeOf[java.lang.Enum[_]]
    lazy val javaFloat: Type = typeOf[java.lang.Float]
    lazy val javaInt: Type = typeOf[java.lang.Integer]
    lazy val javaLong: Type = typeOf[java.lang.Long]
    lazy val javaShort: Type = typeOf[java.lang.Short]
    lazy val javaString: Type = typeOf[java.lang.String]
    lazy val listMap: Type = typeOf[ListMap[_, Any]]
    lazy val localDate: Type = typeOf[LocalDate]
    lazy val localTime: Type = typeOf[LocalTime]
    lazy val long: Type = typeOf[Long]
    lazy val map: Type = typeOf[Map[_, Any]]
    lazy val msUnique: Type = typeOf[MSUnique]
    lazy val msUuid: Type = typeOf[MSUuid]
    lazy val chainedId: Type = typeOf[ChainedID]
    lazy val node: Type = typeOf[Node[Any]]
    lazy val offsetTime: Type = typeOf[OffsetTime]
    lazy val option: Type = typeOf[Option[Any]]
    lazy val period: Type = typeOf[Period]
    lazy val product: Type = typeOf[Product]
    lazy val referenceHolder: Type = typeOf[ReferenceHolder[_]]
    lazy val registeredFieldType: Type = typeOf[RegisteredFieldType]
    lazy val sortedSet: Type = typeOf[SortedSet[_]]
    lazy val set: Type = typeOf[Set[_]]
    lazy val seq: Type = typeOf[Seq[Any]]
    lazy val short: Type = typeOf[Short]
    lazy val string: Type = typeOf[String]
    lazy val treeMap: Type = typeOf[TreeMap[_, Any]]
    lazy val tuple2: Type = typeOf[Tuple2[Any, Any]]
    lazy val tuple3: Type = typeOf[Tuple3[Any, Any, Any]]
    lazy val tuple4: Type = typeOf[Tuple4[Any, Any, Any, Any]]
    lazy val tuple5: Type = typeOf[Tuple5[Any, Any, Any, Any, Any]]
    lazy val tuple6: Type = typeOf[Tuple6[Any, Any, Any, Any, Any, Any]]
    lazy val tuple7: Type = typeOf[Tuple7[Any, Any, Any, Any, Any, Any, Any]]
    lazy val tuple8: Type = typeOf[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]
    lazy val tuple9: Type = typeOf[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]
    lazy val tuple10: Type = typeOf[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]
    lazy val unit: Type = typeOf[Unit]
    lazy val yearMonth: Type = typeOf[YearMonth]
    lazy val year: Type = typeOf[Year]
    lazy val zonedDateTime: Type = typeOf[ZonedDateTime]
    lazy val zoneId: Type = typeOf[ZoneId]
    lazy val piiElement: Type = typeOf[PIIElement[_ <: DataSubjectCategory]]
  }

  def backed(nme: String): String = s"backed$$$nme"

  def unbacked(nme: String): String = if (nme.startsWith("backed$")) nme.replace("backed$", "") else nme
}
