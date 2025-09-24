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
import optimus.platform.embeddable
import optimus.platform.pickling.CustomPicklingSpec
import optimus.platform.pickling.CustomPicklingImplementationException

import scala.util.control.NonFatal

@embeddable sealed trait RegisteredFieldType extends RegisteredFieldTypeT {
  override final def toFieldType(): FieldType = RegisteredFieldType.rftToFt(this)
}

object RegisteredFieldType {
  @embeddable final case class Unknown(additionalInfo: String) extends RegisteredFieldType

  // Storable types
  @embeddable final case class EntityReference(className: String) extends RegisteredFieldType
  @embeddable final case class BusinessEventReference(className: String) extends RegisteredFieldType
  @embeddable final case class Embeddable(className: String) extends RegisteredFieldType
  @embeddable final case class ReferenceHolder(className: String) extends RegisteredFieldType

  // Primitives
  @embeddable case object Boolean extends RegisteredFieldType
  @embeddable case object Byte extends RegisteredFieldType
  @embeddable case object Char extends RegisteredFieldType
  @embeddable case object Double extends RegisteredFieldType
  @embeddable case object Float extends RegisteredFieldType
  @embeddable case object Int extends RegisteredFieldType
  @embeddable case object Long extends RegisteredFieldType
  @embeddable case object Short extends RegisteredFieldType
  @embeddable case object String extends RegisteredFieldType

  // Primitive-ish types
  @embeddable case object Unit extends RegisteredFieldType
  @embeddable case object BigDecimal extends RegisteredFieldType
  @embeddable case object MsUuid extends RegisteredFieldType
  @embeddable case object ChainedID extends RegisteredFieldType
  @embeddable case object MsUnique extends RegisteredFieldType
  @embeddable final case class JavaEnum(className: String) extends RegisteredFieldType
  @embeddable final case class ScalaEnum(className: String) extends RegisteredFieldType
  @embeddable final case class PIIElement(className: String) extends RegisteredFieldType

  // Tuples
  @embeddable final case class Product(className: String) extends RegisteredFieldType
  @embeddable final case class Tuple2(ft1: RegisteredFieldType, ft2: RegisteredFieldType) extends RegisteredFieldType
  @embeddable final case class Tuple3(ft1: RegisteredFieldType, ft2: RegisteredFieldType, ft3: RegisteredFieldType)
      extends RegisteredFieldType
  @embeddable final case class Tuple4(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple5(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple6(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType,
      ft6: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple7(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType,
      ft6: RegisteredFieldType,
      ft7: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple8(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType,
      ft6: RegisteredFieldType,
      ft7: RegisteredFieldType,
      ft8: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple9(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType,
      ft6: RegisteredFieldType,
      ft7: RegisteredFieldType,
      ft8: RegisteredFieldType,
      ft9: RegisteredFieldType
  ) extends RegisteredFieldType
  @embeddable final case class Tuple10(
      ft1: RegisteredFieldType,
      ft2: RegisteredFieldType,
      ft3: RegisteredFieldType,
      ft4: RegisteredFieldType,
      ft5: RegisteredFieldType,
      ft6: RegisteredFieldType,
      ft7: RegisteredFieldType,
      ft8: RegisteredFieldType,
      ft9: RegisteredFieldType,
      ft10: RegisteredFieldType
  ) extends RegisteredFieldType

  // Date-time types
  @embeddable case object Instant extends RegisteredFieldType
  @embeddable case object Period extends RegisteredFieldType
  @embeddable case object ZonedDateTime extends RegisteredFieldType
  @embeddable case object Duration extends RegisteredFieldType
  @embeddable case object YearMonth extends RegisteredFieldType
  @embeddable case object Year extends RegisteredFieldType
  @embeddable case object LocalDate extends RegisteredFieldType
  @embeddable case object LocalTime extends RegisteredFieldType
  @embeddable case object OffsetTime extends RegisteredFieldType
  @embeddable case object ZoneId extends RegisteredFieldType

  // Collection types
  @embeddable final case class OrderedCollection(elemType: RegisteredFieldType, collectionType: String)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class UnorderedCollection(elemType: RegisteredFieldType, collectionType: String)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class Collection(elemType: RegisteredFieldType, collectionType: String)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class Compressed(t: RegisteredFieldType) extends RegisteredFieldType
  @embeddable final case class Option(t: RegisteredFieldType) extends RegisteredFieldType
  @embeddable final case class Array(t: RegisteredFieldType) extends RegisteredFieldType with RegisteredCollectionTypeT
  @embeddable final case class ImmutableArray(t: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class Seq(t: RegisteredFieldType) extends RegisteredFieldType with RegisteredCollectionTypeT
  @embeddable final case class Set(t: RegisteredFieldType) extends RegisteredFieldType with RegisteredCollectionTypeT
  @embeddable final case class CovariantSet(t: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class SortedSet(t: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class TreeMap(kt: RegisteredFieldType, vt: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class ListMap(kt: RegisteredFieldType, vt: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  @embeddable final case class Map(kt: RegisteredFieldType, vt: RegisteredFieldType)
      extends RegisteredFieldType
      with RegisteredCollectionTypeT
  // Others (things with custom picklers etc)
  @embeddable final case class Knowable(t: RegisteredFieldType) extends RegisteredFieldType

  private def rftToFt(rft: RegisteredFieldType): FieldType = rft match {
    case _: RegisteredFieldType.EntityReference | _: RegisteredFieldType.BusinessEventReference |
        _: RegisteredFieldType.ReferenceHolder =>
      FieldType.StorableReference
    case RegisteredFieldType.Boolean                        => FieldType.Boolean
    case RegisteredFieldType.Byte                           => FieldType.Byte
    case RegisteredFieldType.Char                           => FieldType.Char
    case RegisteredFieldType.Double                         => FieldType.Double
    case RegisteredFieldType.Float                          => FieldType.Double
    case RegisteredFieldType.Int | RegisteredFieldType.Year => FieldType.Int
    case RegisteredFieldType.Long                           => FieldType.Long
    case RegisteredFieldType.Short                          => FieldType.Short
    case RegisteredFieldType.Period                         => FieldType.Period
    case RegisteredFieldType.ZoneId                         => FieldType.ZoneId
    /* TODO (OPTIMUS-65182): BUGFIX -- fix OffsetTime logic as it doesnt match def optimus.platform.pickling.OffsetTimePickler.pickle.
    Also confirm whether behaviour is as expected for all types
     */
    case RegisteredFieldType.OffsetTime    => FieldType.OffsetTime
    case RegisteredFieldType.ZonedDateTime => FieldType.ZonedDateTime
    case RegisteredFieldType.LocalTime     => FieldType.LocalTime
    case RegisteredFieldType.LocalDate     => FieldType.LocalDate
    case RegisteredFieldType.String | _: RegisteredFieldType.Embeddable | RegisteredFieldType.Unit |
        RegisteredFieldType.BigDecimal | RegisteredFieldType.MsUuid | RegisteredFieldType.MsUnique |
        _: RegisteredFieldType.JavaEnum | _: RegisteredFieldType.ScalaEnum | _: RegisteredFieldType.Product |
        _: RegisteredFieldType.Tuple2 | _: RegisteredFieldType.Tuple3 | _: RegisteredFieldType.Tuple4 |
        _: RegisteredFieldType.Tuple5 | _: RegisteredFieldType.Tuple6 | _: RegisteredFieldType.Tuple7 |
        _: RegisteredFieldType.Tuple8 | _: RegisteredFieldType.Tuple9 | _: RegisteredFieldType.Tuple10 |
        RegisteredFieldType.Instant | RegisteredFieldType.Duration | RegisteredFieldType.YearMonth |
        _: RegisteredFieldType.OrderedCollection | _: RegisteredFieldType.UnorderedCollection |
        _: RegisteredFieldType.Collection | _: RegisteredFieldType.Compressed | _: RegisteredFieldType.Option |
        _: RegisteredFieldType.Array | _: RegisteredFieldType.ImmutableArray | _: RegisteredFieldType.Seq |
        _: RegisteredFieldType.Set | _: RegisteredFieldType.CovariantSet | _: RegisteredFieldType.SortedSet |
        _: RegisteredFieldType.TreeMap | _: RegisteredFieldType.ListMap | _: RegisteredFieldType.Map |
        _: RegisteredFieldType.Knowable | RegisteredFieldType.ChainedID | _: RegisteredFieldType.PIIElement =>
      FieldType.IterableOrString
    case RegisteredFieldType.Unknown(additionalInfo) =>
      try {
        CustomPicklingSpec
          .ofClass(Class.forName(additionalInfo))
          .map { _.pickledType }
          .getOrElse { FieldType.UnknownFieldType(additionalInfo) }
      } catch {
        case NonFatal(ex) if !ex.isInstanceOf[CustomPicklingImplementationException] =>
          FieldType.UnknownFieldType(additionalInfo)
      }
  }

  private[optimus] def downCast(rftt: RegisteredFieldTypeT): RegisteredFieldType = rftt match {
    case rft: RegisteredFieldType => rft
    case o                        => throw new IllegalArgumentException(s"Cannot create RegisteredFieldType from $o")
  }
}
