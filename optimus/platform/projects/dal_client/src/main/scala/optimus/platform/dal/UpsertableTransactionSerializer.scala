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

import optimus.platform._
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.platform.storable._
import optimus.platform.util.Log

import java.time.Instant
import scala.reflect.ClassTag

object UpsertableTransactionSerializer extends Log {
  @async def serialize[T <: Transaction](
      transaction: T
  ): SerializedUpsertableTransaction = SerializedUpsertableTransaction(transaction.createCommand().pae)

  @async def deserialize(
      deserializeTime: Instant,
      transaction: SerializedUpsertableTransaction
  ): DeserializedTransaction = DeserializedTransaction(deserializeTime, transaction.putEvent)

  @async def deserializeBusinessEvent(
      transaction: DeserializedTransaction
  ): BusinessEvent =
    EventSerializer.deserializeBusinessEvent(
      FixedTransactionTimeContext(transaction.deserializeTime)
    )(transaction.businessEvent)

  /**
   * @param serializedEntities includes all serialized entities - both Heap + Stored ones
   * @param storedEntityReferences stored entity refs based on SerializedEntities in the Txn
   */
  @node
  def deserializeEntity(
      vt: Instant,
      tt: Instant,
      serializedEntities: Seq[SerializedEntity],
      storedEntityReferences: Seq[FinalTypedReference],
      serializedEntityFilter: SerializedEntity => Boolean
  ): Seq[Entity] = {
    log.info(
      s"Deserializing Transaction with SerializedEntities [size:${serializedEntities.size}; StoredRefs: ${storedEntityReferences.size}] @ (vt=$vt, tt=$tt)")
    val heapEnts: Map[EntityReference, SerializedContainedEvent.ContainedEntity] =
      serializedEntities.map(se => se.entityRef -> SerializedContainedEvent.UniqueHeapEntity(se)).toMap
    val storedEntities: Map[EntityReference, SerializedContainedEvent.ContainedEntity] =
      storedEntityReferences.map(se => se -> SerializedContainedEvent.StoredEntity(vt, tt)).toMap
    // heap entityrefs takes precedence over stored refs to handle case
    // where heap state is available for an already stored entity - e.g. it was tweaked, manipulated
    ContainedEventSerializer
      .ContainedEntityDeserializer(storedEntities ++ heapEnts)
      .deserEnts
      .filter { case (serializedWithEref, _) => serializedWithEref.se.forall(serializedEntityFilter) }
      .values
      .toSeq
  }

  @node
  def deserializeAllEntities(
      transaction: DeserializedTransaction,
      serializedEntityFilter: SerializedEntity => Boolean = _ => true
  ): Seq[Entity] = deserializeEntity(
    vt = transaction.businessEvent.validTime,
    tt = transaction.deserializeTime,
    serializedEntities = transaction.allSerializedEntities,
    storedEntityReferences = transaction.allStoredEntityReferences,
    serializedEntityFilter = serializedEntityFilter
  )

  @node
  def extractEntities[E <: Entity: ClassTag](
      transaction: DeserializedTransaction
  )(implicit ctag: ClassTag[E]): Seq[E] =
    extractEntitiesTyped(transaction, ctag.runtimeClass.asInstanceOf[Class[E]])

  @node
  def extractEntitiesTyped[E <: Entity](
      transaction: DeserializedTransaction,
      clazz: Class[E]
  ): Seq[E] = extractEntitiesUntyped(transaction, clazz).map(_.asInstanceOf[E])

  @node
  def extractEntitiesUntyped(
      transaction: DeserializedTransaction,
      clazz: Class[_]
  ): Seq[Entity] = SerializedEntityHelper.collectEntitiesUnTyped(deserializeAllEntities(transaction), clazz)

  @node
  def resolveCmdToEntityMap(
      transaction: DeserializedTransaction
  ): Map[WriteBusinessEvent.Put, Entity] = {
    val entities = UpsertableTransactionSerializer.deserializeAllEntities(transaction)
    transaction.putEvent.bes.flatMap {
      _.puts.map { p =>
        p -> entities
          .find(_.dal$entityRef == p.ent.entityRef)
          .getOrElse(
            throw new IllegalArgumentException(s"Couldn't find ${p.ent.entityRef} in ${entities.map(_.dal$entityRef)}"))
      }
    }.toMap
  }
}
