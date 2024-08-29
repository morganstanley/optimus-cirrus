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

import optimus.platform.storable.{BusinessEventReference, EntityReference, StorableReference, CmReference}

final case class CmReferenceEntry(cmid: CmReference, stableId: StorableReference) {
  lazy val isEntity = stableId.isInstanceOf[EntityReference]
  lazy val entityReferenceOpt = if (isEntity) Some(stableId.asInstanceOf[EntityReference]) else None
  lazy val businessEventReferenceOpt = if (!isEntity) Some(stableId.asInstanceOf[BusinessEventReference]) else None
  def getRefTypeIdOpt =
    if (isEntity) entityReferenceOpt.flatMap(_.getTypeId) else businessEventReferenceOpt.flatMap(_.getTypeId)
}
