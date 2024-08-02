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
package optimus.platform.metadatas.internal
import optimus.config.spray.json.DefaultJsonProtocol

// implicit values used for spray json
object MetaJsonProtocol extends DefaultJsonProtocol {
  import optimus.config.spray.json._

  private def convertTo[T: JsonReader](jsObject: JsObject, fieldName: String, default: T): T =
    jsObject.fields.get(fieldName).map(_.convertTo[T]).getOrElse(default)

  private def skipIfDefault[T](value: T, default: T): Option[T] =
    if (value == default) None else Some(value)

  private implicit val piiDetailsProtocol: JsonFormat[PIIDetails] = jsonFormat3(PIIDetails.apply)

  implicit val entityBaseMetaDataProtocol: RootJsonFormat[EntityBaseMetaData] = new RootJsonFormat[EntityBaseMetaData] {
    override def read(json: JsValue): EntityBaseMetaData = {
      val jsObject = json.asJsObject
      jsObject.getFields("fullClassName", "packageName") match {
        case Seq(fullClassName, packageName) =>
          EntityBaseMetaData(
            fullClassName = fullClassName.convertTo[String],
            packageName = packageName.convertTo[String],
            isStorable = convertTo(jsObject, "isStorable", default = false),
            isAbstract = convertTo(jsObject, "isAbstract", default = false),
            isObject = convertTo(jsObject, "isObject", default = false),
            isTrait = convertTo(jsObject, "isTrait", default = false),
            slotNumber = convertTo(jsObject, "slotNumber", default = 0),
            explicitSlotNumber = convertTo(jsObject, "explicitSlotNumber", default = false),
            parentNames = convertTo[List[String]](jsObject, "parentNames", Nil),
            piiElements = convertTo[Seq[PIIDetails]](jsObject, "piiElements", Seq.empty)
          )
        case other => deserializationError("Cannot deserialize EntityBaseMetaData: invalid input. Raw input: " + other)
      }
    }

    override def write(obj: EntityBaseMetaData): JsValue = JsObject(
      List(
        Some("fullClassName" -> obj.fullClassName.toJson),
        Some("packageName" -> obj.packageName.toJson),
        skipIfDefault(obj.isStorable, default = false).map("isStorable" -> _.toJson),
        skipIfDefault(obj.isAbstract, default = false).map("isAbstract" -> _.toJson),
        skipIfDefault(obj.isObject, default = false).map("isObject" -> _.toJson),
        skipIfDefault(obj.isTrait, default = false).map("isTrait" -> _.toJson),
        skipIfDefault(obj.slotNumber, default = 0).map("slotNumber" -> _.toJson),
        skipIfDefault(obj.explicitSlotNumber, default = false).map("explicitSlotNumber" -> _.toJson),
        skipIfDefault[List[String]](obj.parentNames, Nil).map("parentNames" -> _.toJson),
        skipIfDefault[Seq[PIIDetails]](obj.piiElements, Seq.empty).map("piiElements" -> _.toJson)
      ).flatten: _*
    )
  }

  implicit val embeddableBaseMetaDataProtocol: RootJsonFormat[EmbeddableBaseMetaData] =
    new RootJsonFormat[EmbeddableBaseMetaData] {
      override def read(json: JsValue): EmbeddableBaseMetaData = {
        val jsObject = json.asJsObject
        jsObject.getFields("fullClassName", "packageName") match {
          case Seq(fullClassName, packageName) =>
            EmbeddableBaseMetaData(
              fullClassName = fullClassName.convertTo[String],
              packageName = packageName.convertTo[String],
              isTrait = convertTo(jsObject, "isTrait", default = false),
              isObject = convertTo(jsObject, "isObject", default = false),
              parentNames = convertTo[List[String]](jsObject, "parentNames", Nil),
              embeddablePiiElements = convertTo[Seq[PIIDetails]](jsObject, "piiElements", Seq.empty)
            )
          case other =>
            deserializationError("Cannot deserialize EmbeddableBaseMetaData: invalid input. Raw input: " + other)
        }
      }

      override def write(obj: EmbeddableBaseMetaData): JsValue = JsObject(
        List(
          Some("fullClassName" -> obj.fullClassName.toJson),
          Some("packageName" -> obj.packageName.toJson),
          skipIfDefault(obj.isTrait, default = false).map("isTrait" -> _.toJson),
          skipIfDefault(obj.isObject, default = false).map("isObject" -> _.toJson),
          skipIfDefault(obj.parentNames, Nil).map("parentNames" -> _.toJson),
          skipIfDefault(obj.embeddablePiiElements, Nil).map("piiElements" -> _.toJson),
        ).flatten: _*
      )
    }

  implicit val eventBaseMetaDataProtocol: RootJsonFormat[EventBaseMetaData] = new RootJsonFormat[EventBaseMetaData] {
    override def read(json: JsValue): EventBaseMetaData = {
      val jsObject = json.asJsObject
      jsObject.getFields("fullClassName", "packageName") match {
        case Seq(fullClassName, packageName) =>
          EventBaseMetaData(
            fullClassName = fullClassName.convertTo[String],
            packageName = packageName.convertTo[String],
            isTrait = convertTo(jsObject, "isTrait", default = false),
            isObject = convertTo(jsObject, "isObject", default = false),
            parentNames = convertTo[List[String]](jsObject, "parentNames", Nil)
          )
        case other => deserializationError("Cannot deserialize EventBaseMetaData: invalid input. Raw input: " + other)
      }
    }

    override def write(obj: EventBaseMetaData): JsValue = JsObject(
      List(
        Some("fullClassName" -> obj.fullClassName.toJson),
        Some("packageName" -> obj.packageName.toJson),
        skipIfDefault(obj.isTrait, default = false).map("isTrait" -> _.toJson),
        skipIfDefault(obj.isObject, default = false).map("isObject" -> _.toJson),
        skipIfDefault(obj.parentNames, Nil).map("parentNames" -> _.toJson)
      ).flatten: _*
    )
  }

  implicit val metaBaseMetaDataProtocol: RootJsonFormat[MetaBaseMetaData] = new RootJsonFormat[MetaBaseMetaData] {
    override def read(json: JsValue): MetaBaseMetaData = {
      val jsObject = json.asJsObject
      jsObject.getFields("fullClassName", "packageName", "owner", "catalogClass") match {
        case Seq(fullClassName, packageName, owner, catalogClass) =>
          MetaBaseMetaData(
            fullClassName = fullClassName.convertTo[String],
            packageName = packageName.convertTo[String],
            owner = owner.convertTo[String],
            catalogClass = catalogClass.convertTo[String],
            isEntity = convertTo(jsObject, "isEntity", default = false),
            isStorable = convertTo(jsObject, "isStorable", default = false),
            isObject = convertTo(jsObject, "isObject", default = false),
            isTrait = convertTo(jsObject, "isTrait", default = false),
            parentNames = convertTo[List[String]](jsObject, "parentNames", Nil)
          )
        case other => deserializationError("Cannot deserialize MetaBaseMetaData: invalid input. Raw input: " + other)
      }
    }

    override def write(obj: MetaBaseMetaData): JsValue = JsObject(
      List(
        Some("fullClassName" -> obj.fullClassName.toJson),
        Some("packageName" -> obj.packageName.toJson),
        Some("owner" -> obj.owner.toJson),
        Some("catalogClass" -> obj.catalogClass.toJson),
        skipIfDefault(obj.isEntity, default = false).map("isEntity" -> _.toJson),
        skipIfDefault(obj.isStorable, default = false).map("isStorable" -> _.toJson),
        skipIfDefault(obj.isObject, default = false).map("isObject" -> _.toJson),
        skipIfDefault(obj.isTrait, default = false).map("isTrait" -> _.toJson),
        skipIfDefault(obj.parentNames, Nil).map("parentNames" -> _.toJson)
      ).flatten: _*
    )
  }
  implicit object BaseMetaDataFormat extends RootJsonFormat[BaseMetaData] {
    def write(b: BaseMetaData): JsValue = {
      b match {
        case md: EntityBaseMetaData     => md.toJson
        case md: EmbeddableBaseMetaData => md.toJson
        case md: MetaBaseMetaData       => md.toJson
        case md: EventBaseMetaData      => md.toJson
      }
    }
    def read(value: JsValue): BaseMetaData =
      deserializationError("Should not call Json.convertTo[BaseMetaData], please use subclasses instead")
  }
}
