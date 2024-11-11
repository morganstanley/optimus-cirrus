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

import java.time.Instant

import optimus.platform.storable.SerializedEntity
import optimus.platform.versioning.RegisteredFieldTypeT

import scala.collection.mutable

final case class AcceleratedKey(name: SerializedEntity.TypeRef, schemaVersion: Int)

//mark this field is a single, multiple or linked(childToParent) type
sealed trait CollFlag
object CollFlag {
  case object Single extends CollFlag
  case object Multiple extends CollFlag
  case object Linked extends CollFlag
}

final case class AcceleratedField(
    name: String,
    typeInfo: RegisteredFieldTypeT,
    indexed: Boolean,
    collFlag: CollFlag,
    target: Option[AcceleratedKey] = None,
    compositeFields: Seq[String] = Nil)

final case class AcceleratedInfo(
    id: AcceleratedKey,
    types: List[String],
    fields: Seq[AcceleratedField],
    classpathHashes: Set[String],
    rwTTScope: (Option[Instant], Option[Instant]),
    canRead: Boolean,
    canWrite: Boolean,
    tableHash: List[String],
    nameLookup: Map[String, String],
    parallelWorkers: Option[Int] = None,
    enableSerializedKeyBasedFilter: Option[Boolean] = None) {

  val nameHash: Sha1Hash = Sha1Hash(tableHash.head)

  lazy val projectedFieldsMap: Map[String, AcceleratedField] = {
    fields.iterator.withFilter(f => !f.name.endsWith(")")).map(f => f.name -> f).toMap
  }

  lazy val embeddableInfoMap: Map[EmbeddableLinkageKey, AccEmbeddableInfo] = {
    val fieldsMap = mutable.Map[EmbeddableLinkageKey, mutable.ListBuffer[AcceleratedField]]()
    // Get (fieldName, tag) -> Seq[AcceleratedField]
    fields
      .filter(f => f.name.endsWith(")"))
      .foreach(f => {
        f.name match {
          case AcceleratedInfo.fieldPattern(fieldName, tag, embeddableField) =>
            val info = EmbeddableLinkageKey(fieldName, tag)
            val fields = fieldsMap.getOrElseUpdate(info, mutable.ListBuffer[AcceleratedField]())
            fields.append(f.copy(name = embeddableField))
          case _ =>
        }
      })
    fieldsMap.iterator.map { case (key, embeddableFields) =>
      val embeddableFieldsMap: Map[String, AcceleratedField] = embeddableFields.iterator.map(f => f.name -> f).toMap

      val embeddableName = s"${id.name}_${key.fieldName}_${key.embeddableTag}"
      val (nameHash, _) = nameLookup.find { case (hash, name) =>
        name == embeddableName
      } getOrElse (throw new IllegalArgumentException(s"Can't find hash value for embeddable name: $embeddableName"))

      (key, AccEmbeddableInfo(id.schemaVersion, Sha1Hash(nameHash), embeddableFieldsMap))
    }.toMap
  }
}

object AcceleratedInfo {
  val fieldPattern = """([^.]+)\.\(([^.]+)::([^.]+)\)""".r
  // TODO (OPTIMUS-51356): Change back to full platform classname for AcceleratedInfoE entity after migration is complete
  val acceleratedInfoEName = ".session.AcceleratedInfoE"
  val invalidSlot = -1
}

final case class Sha1Hash(value: String)
final case class EmbeddableLinkageKey(fieldName: String, embeddableTag: String)
final case class AccEmbeddableInfo(slot: Int, nameHash: Sha1Hash, fieldsMap: Map[String, AcceleratedField])
