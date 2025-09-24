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
package optimus.platform.dsi.accelerated

import optimus.platform.versioning.RegisteredFieldType
import optimus.utils.CollectionUtils._

import scala.collection.immutable

object AcceleratedInfoConversions {
  private def convertToRegisterFieldType(value: Any): RegisteredFieldType = {
    value match {
      case strValue: String =>
        registeredFieldTypeMap.getOrElse(
          strValue,
          throw new IllegalArgumentException(s"${strValue} is not a case object of RegisteredFieldType"))
      case mapValue: Map[String @unchecked, String @unchecked] =>
        mapValue
          .get("_tag")
          .map {
            case "EntityReference" => RegisteredFieldType.EntityReference(mapValue.getOrElse("className", ""))
            case "BusinessEventReference" =>
              RegisteredFieldType.BusinessEventReference(mapValue.getOrElse("className", ""))
            case "Embeddable" => RegisteredFieldType.Embeddable(mapValue.getOrElse("className", ""))
            case "JavaEnum"   => RegisteredFieldType.JavaEnum(mapValue.getOrElse("className", ""))
            case "PIIElement" => RegisteredFieldType.PIIElement(mapValue.getOrElse("className", ""))
            case "ScalaEnum"  => RegisteredFieldType.ScalaEnum(mapValue.getOrElse("className", ""))
            case "Array"      => RegisteredFieldType.Array(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "ImmutableArray" =>
              RegisteredFieldType.ImmutableArray(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "Seq" => RegisteredFieldType.Seq(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "Set" => RegisteredFieldType.Set(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "CovariantSet" =>
              RegisteredFieldType.CovariantSet(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "SortedSet" => RegisteredFieldType.SortedSet(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "OrderedCollection" =>
              RegisteredFieldType.OrderedCollection(
                convertToRegisterFieldType(mapValue.getOrElse("elemType", "")),
                mapValue.getOrElse("collectionType", ""))
            case "UnorderedCollection" =>
              RegisteredFieldType.UnorderedCollection(
                convertToRegisterFieldType(mapValue.getOrElse("elemType", "")),
                mapValue.getOrElse("collectionType", ""))
            case "Collection" =>
              RegisteredFieldType.Collection(
                convertToRegisterFieldType(mapValue.getOrElse("elemType", "")),
                mapValue.getOrElse("collectionType", ""))
            case "TreeMap" =>
              RegisteredFieldType.TreeMap(
                convertToRegisterFieldType(mapValue.getOrElse("kt", "")),
                convertToRegisterFieldType(mapValue.getOrElse("vt", "")))
            case "ListMap" =>
              RegisteredFieldType.ListMap(
                convertToRegisterFieldType(mapValue.getOrElse("kt", "")),
                convertToRegisterFieldType(mapValue.getOrElse("vt", "")))
            case "Map" =>
              RegisteredFieldType.Map(
                convertToRegisterFieldType(mapValue.getOrElse("kt", "")),
                convertToRegisterFieldType(mapValue.getOrElse("vt", "")))
            case "Option" => RegisteredFieldType.Option(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            // We need field typeInfo to find right convertor
            case "Tuple2" =>
              RegisteredFieldType.Tuple2(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")))
            case "Tuple3" =>
              RegisteredFieldType.Tuple3(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", ""))
              )
            case "Tuple4" =>
              RegisteredFieldType.Tuple4(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", ""))
              )
            case "Tuple5" =>
              RegisteredFieldType.Tuple5(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", ""))
              )
            case "Tuple6" =>
              RegisteredFieldType.Tuple6(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft6", ""))
              )
            case "Tuple7" =>
              RegisteredFieldType.Tuple7(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft6", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft7", ""))
              )
            case "Tuple8" =>
              RegisteredFieldType.Tuple8(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft6", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft7", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft8", ""))
              )
            case "Tuple9" =>
              RegisteredFieldType.Tuple9(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft6", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft7", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft8", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft9", ""))
              )
            case "Tuple10" =>
              RegisteredFieldType.Tuple10(
                convertToRegisterFieldType(mapValue.getOrElse("ft1", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft2", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft3", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft4", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft5", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft6", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft7", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft8", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft9", "")),
                convertToRegisterFieldType(mapValue.getOrElse("ft10", ""))
              )
            case "Knowable"   => RegisteredFieldType.Knowable(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "Compressed" => RegisteredFieldType.Compressed(convertToRegisterFieldType(mapValue.getOrElse("t", "")))
            case "Unknown"    => RegisteredFieldType.Unknown("")
            case _ =>
              throw new IllegalArgumentException(
                s"${mapValue.getOrElse("_tag", "")} is not defined in RegisteredFieldType")
          } get
      case _ =>
        throw new IllegalArgumentException(
          s"RegisteredFieldType is expected to be a constant string or map, but actual is ${value}")
    }
  }

  private val registeredFieldTypeMap = Map(
    "Boolean" -> RegisteredFieldType.Boolean,
    "Char" -> RegisteredFieldType.Char,
    "Byte" -> RegisteredFieldType.Byte,
    "Short" -> RegisteredFieldType.Short,
    "Int" -> RegisteredFieldType.Int,
    "Long" -> RegisteredFieldType.Long,
    "String" -> RegisteredFieldType.String,
    "ZonedDateTime" -> RegisteredFieldType.ZonedDateTime,
    "Double" -> RegisteredFieldType.Double,
    "Float" -> RegisteredFieldType.Float,
    "LocalDate" -> RegisteredFieldType.LocalDate,
    "BigDecimal" -> RegisteredFieldType.BigDecimal,
    "Instant" -> RegisteredFieldType.Instant,
    "MsUuid" -> RegisteredFieldType.MsUuid,
    "Duration" -> RegisteredFieldType.Duration,
    "Period" -> RegisteredFieldType.Period,
    "YearMonth" -> RegisteredFieldType.YearMonth,
    "Year" -> RegisteredFieldType.Year,
    "ZoneId" -> RegisteredFieldType.ZoneId,
    "OffsetTime" -> RegisteredFieldType.OffsetTime,
    "LocalTime" -> RegisteredFieldType.LocalTime
  )

  private val collFlagMap =
    Map("Single" -> CollFlag.Single, "Multiple" -> CollFlag.Multiple, "Linked" -> CollFlag.Linked)

  def toAcceleratedInfo(properties: Map[String, Any]): AcceleratedInfo = {
    val key = properties
      .get("id")
      .map { case id: Map[String @unchecked, _] =>
        new AcceleratedKey(id("name").asInstanceOf[String], id("schemaVersion").asInstanceOf[Int])
      } get
    val types = properties("types").asInstanceOf[collection.Seq[String]].toList
    val fields = properties
      .get("fields")
      .map { case fields: collection.Seq[Map[String, _] @unchecked] =>
        fields.iterator
          .map(accFieldMap => {
            val compositeFields = accFieldMap("compositeFields").asInstanceOf[collection.Seq[String]].toSeq
            AcceleratedField(
              accFieldMap("name").asInstanceOf[String],
              convertToRegisterFieldType(accFieldMap("typeInfo")),
              accFieldMap("indexed").asInstanceOf[Boolean],
              collFlagMap.getOrElse(
                accFieldMap("collFlag").asInstanceOf[String],
                throw new IllegalArgumentException("CollFlag is not a valid value")),
              None,
              compositeFields
            )
          })
          .toSeq
      }
      .get
    val canRead = properties("canRead").asInstanceOf[Boolean]
    val canWrite = properties("canWrite").asInstanceOf[Boolean]
    val tableHash = properties("tableHash").asInstanceOf[collection.Seq[String]].toList
    val nameLookup: immutable.Map[String, String] = properties("nameLookup")
      .asInstanceOf[collection.Seq[collection.Seq[String]]]
      .iterator
      .map { case collection.Seq(hash, name) =>
        hash -> name
      }
      .toMap
    val parallelWorkers = properties.get("parallelWorkers").flatMap(_.asInstanceOf[collection.Seq[Int]].singleOption)
    val enableSerializedKeyBasedFilter =
      properties.get("enableSerializedKeyBasedFilter").flatMap(_.asInstanceOf[collection.Seq[Boolean]].singleOption)
    // we don't use classpathHashes and rwTTScope now, so set them to be Nil and None
    AcceleratedInfo(
      key,
      types,
      fields,
      immutable.Set.empty[String],
      (None, None),
      canRead,
      canWrite,
      tableHash,
      nameLookup,
      parallelWorkers,
      enableSerializedKeyBasedFilter)
  }
}
