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
package optimus.platform.dsi.versioning

import optimus.platform.versioning.FieldType

import scala.collection.SortedMap

final case class WriteShapeMetadata(
    key: SlotMetadata.Key,
    shapes: Set[Map[String, FieldType]],
    storageMetadata: Set[WriteStorageMetadata])

object WriteShapeMetadata {
  def empty(key: SlotMetadata.Key) =
    WriteShapeMetadata(key, Set.empty[Map[String, FieldType]], Set.empty[WriteStorageMetadata])
}

sealed trait WriteStorageMetadata
object WriteStorageMetadata {
  final case class Index(indexedFields: SortedMap[String, FieldType], unique: Boolean) extends WriteStorageMetadata

  final case class Key(fields: SortedMap[String, FieldType]) extends WriteStorageMetadata

  // For C2P field, we only need store the fieldName since the FieldType of RFT.EntityReference is always StorableReference
  final case class ChildToParentLinkage(fieldName: String) extends WriteStorageMetadata

  def index(indexedFields: SortedMap[String, FieldType], unique: Boolean): Index = Index(indexedFields, unique)

  def key(fields: SortedMap[String, FieldType]): Key = Key(fields)

  def linkage(fieldName: String): ChildToParentLinkage = ChildToParentLinkage(fieldName)
}
