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

import optimus.platform.annotations.PickledType
import optimus.platform.storable.ModuleEntityToken
import optimus.platform.storable.StorableReference

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZonedDateTime

// super-type of RegisteredFieldType and FieldType
private[optimus] trait FieldTypeT

// A field type as we understand it during registration
// This can be further specialized over time, as long as the code for deserializing from storage can deal with reading
// existing metadata into this hydrated form
trait RegisteredFieldTypeT extends FieldTypeT {
  def toFieldType(): FieldType
}

trait RegisteredCollectionTypeT { self: RegisteredFieldTypeT => }

// Field type representations for client-side versioning
sealed trait FieldType extends FieldTypeT
object FieldType {
  // We can't work out the type of a referenced entity/event etc. at runtime
  case object StorableReference extends FieldType
  // We can't differentiate between embeddables and iterables, and case objects pickle to string. Great.
  case object IterableOrString extends FieldType

  case object Boolean extends FieldType
  case object Byte extends FieldType
  case object Char extends FieldType
  case object Double extends FieldType
  case object Int extends FieldType
  case object Long extends FieldType
  case object Short extends FieldType

  // Other things which are differentiable in the pickled representation (these are basically derived from dsi.proto's
  // FieldProto.Type and PropertyMapProtoSerialization)
  case object Period extends FieldType
  case object ZoneId extends FieldType
  case object OffsetTime extends FieldType
  case object ZonedDateTime extends FieldType
  case object LocalTime extends FieldType
  case object LocalDate extends FieldType
  final case class UnknownFieldType(className: String) extends FieldType

  def fromPickledValue[T](value: T): FieldType = value match {
    case _: StorableReference | _: ModuleEntityToken =>
      // NB StorableReference and ModuleEntityToken are distinguishable at runtime, but entity objects might be assigned
      // to fields which are typed as entities and therefore could end up being either an entity ref or a module entity
      // token. We therefore want to assume that pickled fields of either ref or module token are the same FieldType so
      // that transformers match appropriately.
      StorableReference
    case _: Iterable[_] | _: String        => IterableOrString
    case _: Boolean | _: java.lang.Boolean => Boolean
    case _: Byte | _: java.lang.Byte       => Byte
    case _: Char | _: java.lang.Character  => Char
    case _: Double | _: java.lang.Double   => Double
    case _: Float | _: java.lang.Float     => Double // NB doubles only in PG
    case _: Int | _: java.lang.Integer     => Int
    case _: Long | _: java.lang.Long       => Long
    case _: Short | _: java.lang.Short     => Short
    case _: Period                         => Period
    case _: ZoneId                         => ZoneId
    case _: OffsetTime                     => OffsetTime
    case _: ZonedDateTime                  => ZonedDateTime
    case _: LocalTime                      => LocalTime
    case _: LocalDate                      => LocalDate
    case _                                 => UnknownFieldType(value.getClass.getName)
  }

  def fromPickledType(pt: PickledType): FieldType = {
    pt match {
      case PickledType.StorableReference => StorableReference
      case PickledType.IterableOrString  => IterableOrString
      case PickledType.Boolean           => Boolean
      case PickledType.Byte              => Byte
      case PickledType.Char              => Char
      case PickledType.Double            => Double
      case PickledType.Int               => Int
      case PickledType.Long              => Long
      case PickledType.Short             => Short
      case PickledType.Period            => Period
      case PickledType.ZoneId            => ZoneId
      case PickledType.OffsetTime        => OffsetTime
      case PickledType.ZonedDateTime     => ZonedDateTime
      case PickledType.LocalTime         => LocalTime
      case PickledType.LocalDate         => LocalDate
    }
  }
}
