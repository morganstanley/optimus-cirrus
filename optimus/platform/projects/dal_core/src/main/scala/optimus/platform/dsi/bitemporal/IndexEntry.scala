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
package optimus.platform.dsi.bitemporal

import optimus.platform.{TimeInterval, ValidTimeInterval}
import optimus.platform.storable._

sealed trait IndexEntryBase extends Temporal {
  def id: AnyRef
  def versionedRef: VersionedReference
  def permanentRef: FinalTypedReference
  def typeName: String
  def properties: Seq[(String, Any)]
}

/**
 * Represents the entry in the index table.
 */
final case class IndexEntry(
    override val id: AnyRef,
    override val versionedRef: VersionedReference,
    override val permanentRef: FinalTypedReference,
    override val typeName: String,
    override val properties: Seq[(String, Any)],
    override val vtInterval: ValidTimeInterval,
    override val txInterval: TimeInterval,
    hash: Array[Byte],
    refFilter: Boolean)
    extends Locator
    with IndexEntryBase {

  override def hashCode = versionedRef.hashCode

  import java.util.Arrays
  override def equals(o: Any) = o match {
    case entry: IndexEntry =>
      entry.id == id && entry.versionedRef == versionedRef && entry.typeName == typeName &&
      entry.properties == properties && entry.vtInterval == vtInterval && entry.txInterval == txInterval &&
      Arrays.equals(entry.hash, hash) && entry.refFilter == refFilter
    case _ => false
  }
}

// Represents the entry of index collection/table (per type) in new format.
final case class RegisteredIndexEntry(
    override val id: AnyRef,
    override val versionedRef: VersionedReference,
    override val permanentRef: FinalTypedReference,
    override val typeName: String,
    override val properties: Seq[(String, Any)],
    override val vtInterval: ValidTimeInterval,
    override val txInterval: TimeInterval)
    extends Locator
    with IndexEntryBase {
  lazy val propertiesMap: Map[String, Any] = properties.toMap
}
