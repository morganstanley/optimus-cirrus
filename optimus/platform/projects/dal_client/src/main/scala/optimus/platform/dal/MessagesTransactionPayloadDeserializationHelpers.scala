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
import optimus.breadcrumbs.ChainedID
import optimus.dsi.messages.source.kafka.StreamEventKey
import optimus.logging.LoggingInfo
import optimus.platform._
import optimus.platform.dal.ContainedEventSerializer.EntityVisitor
import optimus.platform.dal.DalAPI._
import optimus.platform.dal.messages.MessagesTransactionPayload
import optimus.platform.dsi.bitemporal.ClientAppIdentifier
import optimus.platform.storable.DeserializedTransaction
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedEntity
import optimus.platform.storable.SerializedUpsertableTransaction
import optimus.utils.CollectionUtils._

import java.time.Instant
import java.util.UUID

private[optimus] object MessagesTransactionPayloadDeserializationHelpers {
  @async def deserialize[T](topic: String, key: StreamEventKey, transaction: MessagesTransactionPayload): T = {
    val deserialized = UpsertableTransactionSerializer.deserialize(deserializeTime = Instant.now, transaction.message)
    deserialize[T](topic, key, transaction, deserialized)
  }

  @async def deserialize[T](
      topic: String,
      key: StreamEventKey,
      transaction: MessagesTransactionPayload,
      deserialized: DeserializedTransaction): T = {
    // all entities must be deserialized to resolve contained temporary references but only one must be returned for
    // each kafka record key (use == not eq here because we create MessagesTransactionPayload in multiple places)
    val entityMatchingKey = key.findEntityInTransaction(topic, transaction)
    def filter(se: SerializedEntity): Boolean = se == entityMatchingKey

    val deserializedEntities = UpsertableTransactionSerializer.deserializeAllEntities(deserialized, filter)
    val entity = deserializedEntities.singleOrThrow(_ => "Expected one matching entity key in the transaction")
    entity.asInstanceOf[T]
  }

  // pull out all contained entity fields to include in the same transaction
  private def containedEntityReferences(entity: Entity): Map[Entity, EntityReference] = {
    val vis = new EntityVisitor
    vis.apply(entity)
    vis.getResults
  }

  @async
  def writeContained(entity: Entity, event: BusinessEvent, appId: ClientAppIdentifier): MessagesTransactionPayload = {
    val entities = MessagesTransactionPayloadDeserializationHelpers.containedEntityReferences(entity).keys

    val txn = delayedTransaction {
      newEvent(event) {
        // since dal_client doesn't depend on optimus.platform.package (should it?) we can't use DAL.upsert here
        entities.foreach(DALModuleAPI.upsert)
      }
    }

    val serializedUpsertableTransaction = UpsertableTransactionSerializer.serialize(txn)
    generateMessagesPayload(appId, serializedUpsertableTransaction)
  }

  // See MessagesClient.generateMessagesPayload
  def generateMessagesPayload(
      clientAppIdentifier: ClientAppIdentifier,
      transaction: SerializedUpsertableTransaction): MessagesTransactionPayload = {
    val user = LoggingInfo.getUser
    val rId = UUID.randomUUID.toString
    MessagesTransactionPayload(Instant.now, clientAppIdentifier, user, ChainedID.root, rId, transaction)
  }
}
