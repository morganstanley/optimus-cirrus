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

import optimus.platform.ImmutableArray
import optimus.platform.storable.SerializedEntity
import optimus.platform.versioning.RegisteredFieldTypeT
import org.apache.commons.codec.binary.Base32

import scala.collection.immutable.SortedMap

final case class SlotMetadata(
    key: SlotMetadata.Key,
    shapes: Set[StoredShapeT],
    classpathHashes: Set[PathHashT],
    storageMetadata: Set[StorageMetadataT])
object SlotMetadata {
  final case class Key(className: SerializedEntity.TypeRef, slotNumber: Int)

  def empty(key: Key): SlotMetadata =
    apply(key, Set.empty[StoredShapeT], Set.empty[PathHashT], Set.empty[StorageMetadataT])
}

trait StoredShapeT {
  def fields: SortedMap[String, RegisteredFieldTypeT]
}

trait PathHashT {
  def hash: ImmutableArray[Byte]
  override def toString(): String = s"PathHash(${id})"

  // This is used as a representation in ZK nodes etc. Please don't change it without understanding the ramifications.
  final def id = PathHashT.idCodec.bytesToId(hash.rawArray)

  final override def hashCode(): Int = hash.hashCode
  final override def equals(other: Any): Boolean = other.isInstanceOf[PathHashT] && {
    other.asInstanceOf[PathHashT].hash.equals(hash)
  }
}
object PathHashT {
  private[optimus] object idCodec {
    // We use base-32 not base-64 primarily because of Treadmill limitations. Classpath hash id strings get used in
    // naming services on Treadmill, but Treadmill service names are not permitted to contain any punctuation other than
    // underscores, so base-64 encoding is no good. Base-32 will only include a subset of alphanumerics so is fine.
    private val base32 = new Base32

    def idToBytes(id: String): Array[Byte] = base32.decode(id)
    def bytesToId(bytes: Array[Byte]): String = base32.encodeAsString(bytes)
  }

  def toBytes(hashString: String): ImmutableArray[Byte] = {
    val arr = idCodec.idToBytes(hashString)
    ImmutableArray.wrapped(arr)
  }
}

final case class PathHashRepr(hash: ImmutableArray[Byte]) extends PathHashT
object PathHashRepr {
  def apply(hashString: String): PathHashRepr = apply(PathHashT.toBytes(hashString))
}

trait StorageMetadataT {
  def fieldName: String
}

object StorageMetadataT {
  final case class Index(
      fieldName: String,
      indexedFields: Seq[(String, RegisteredFieldTypeT)],
      unique: Boolean,
      typeName: Option[String])
      extends StorageMetadataT

  // represents an @indexed annotation on one or more fields of a stored field
  final case class Key(fieldName: String, fields: Seq[(String, RegisteredFieldTypeT)]) extends StorageMetadataT

  // represents an @stored(childToParent = true) annotation on a field of a stored field
  // e.g. `@stored(childToParent = true) val foo: Set[Bar]` will give `ChildToParentLinkage("foo", RFT.Entity("Bar"))`
  final case class ChildToParentLinkage(fieldName: String, linkedType: RegisteredFieldTypeT) extends StorageMetadataT

  def index(
      fieldName: String,
      indexedFields: Seq[(String, RegisteredFieldTypeT)],
      unique: Boolean,
      typeName: Option[String]): Index =
    Index(fieldName, indexedFields, unique, typeName)

  def key(fieldName: String, fields: Seq[(String, RegisteredFieldTypeT)]): Key = Key(fieldName, fields)

  def linkage(fieldName: String, linkedType: RegisteredFieldTypeT): ChildToParentLinkage =
    ChildToParentLinkage(fieldName, linkedType)
}
