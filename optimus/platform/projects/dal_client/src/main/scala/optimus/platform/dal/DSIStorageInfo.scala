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
package optimus.platform.dal

import optimus.platform.storable.{StorageInfo, VersionedReference}
import java.time.Instant
import optimus.platform.ValidTimeInterval
import optimus.platform.storable.PersistentEntity

private[optimus] object DSIStorageInfo {
  def fromPersistentEntity(e: PersistentEntity) = {
    new DSIStorageInfo(e.lockToken, e.vtInterval, e.txInterval.from, e.versionedRef)
  }
}

private[optimus] final case class DSIStorageInfo(
    lt: Long,
    val validTime: ValidTimeInterval,
    val txTime: Instant,
    versionedRef: VersionedReference)
    extends StorageInfo {
  override def lockToken = Some(lt)
  override def descriptorString = s"version: $versionedRef"
}
