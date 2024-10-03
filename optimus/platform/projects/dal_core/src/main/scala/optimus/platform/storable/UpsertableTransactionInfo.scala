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

import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.utils.CollectionUtils._

trait UpsertableTransactionInfo {
  def putEvent: PutApplicationEvent

  def writeBusinessEvent: WriteBusinessEvent = putEvent.bes.singleOrNone
    .getOrThrow(s"Expecting single Event included in Transaction, but got ${putEvent.bes}")

  def businessEvent: SerializedBusinessEvent = writeBusinessEvent.evt

  def businessEventId: BusinessEventReference = businessEvent.id

  def entityPerPutEvent: Map[WriteBusinessEvent.Put, SerializedEntity] =
    SerializedEntityHelper.entityPerPutEvent(putEvent)

  /** Scope: All Entities - including both topLevel (heap) and inlined entities. */
  def allSerializedEntities: Seq[SerializedEntity] =
    SerializedEntityHelper.allSerializedEntities(entityPerPutEvent.values.toSeq)

  /** Scope: All Entities - including both topLevel (heap) and inlined entities. */
  def allSerializedEntityClassNames: Seq[String] = allSerializedEntities.map(_.className)

  /** Scope: Top-level Entities - including only topLevel (heap) entities defined in txn body via DAL.upserts. */
  def topLevelSerializedEntityClassNames: Seq[String] = putEvent.bes.flatMap(_.puts.map(_.ent.className))

  /**
   * UpsertableTransaction should be targeting a single partition - both Event & Classes.
   * BusinessEvent should be only checked if it's a heap/new instance.
   */
  def classNamesForPartitionCheck: Set[String] =
    if (writeBusinessEvent.state == EventStateFlag.NEW_EVENT)
      Set(businessEvent.className) ++ allSerializedEntityClassNames.toSet
    else allSerializedEntityClassNames.toSet

  /** ClassNames used for checking the subscription target and match against message content. */
  def classNamesForMessageContentCheck: Set[String] =
    if (writeBusinessEvent.state == EventStateFlag.NEW_EVENT)
      Set(businessEvent.className) ++ topLevelSerializedEntityClassNames.toSet
    else topLevelSerializedEntityClassNames.toSet
}
