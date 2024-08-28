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
package optimus.platform.storable

import optimus.platform.{TimeInterval, ValidTimeInterval}

/**
 * SerializedEntity that is also Referrable
 */
final case class PersistentEntity(
    serialized: SerializedEntity,
    versionedRef: VersionedReference,
    lockToken: Long,
    vtInterval: ValidTimeInterval = ValidTimeInterval.max,
    txInterval: TimeInterval = null
) extends Temporal {

  // helper methods
  def entityRef = serialized.entityRef
  def className = serialized.className
  def types = serialized.types
  def cmid = serialized.cmid

  def keys = serialized.keys
  def linkages = serialized.linkages
  def properties = serialized.properties

  def copyPersistent(
      properties: Map[String, Any] = serialized.properties,
      linkages: Option[SerializedEntity.LinkageMap] = serialized.linkages) =
    this.copy(serialized = serialized.copy(properties = properties, linkages = linkages))

  def timesliceString =
    s"${serialized.className} lt: ${lockToken} tx: ${txInterval} vt: ${vtInterval} vref: ${versionedRef} permref: ${serialized.entityRef}"

  override def toString =
    s"PersistentEntity($timesliceString ${serialized.properties})"

  def toVerboseString =
    s"PersistentEntity($timesliceString se: $serialized)"
}

object PersistentEntity {
  // We need a placeholder in EntityResolverReadImple.decodePersistentEntity
  // where we need ternary logic, so Option[PersistentEntity] and None alone
  // are insufficient.
  private[optimus /*platform*/ ] val Null =
    PersistentEntity(SerializedEntity.Null, VersionedReference.Nil, 0L, ValidTimeInterval.max, TimeInterval.max)
}
