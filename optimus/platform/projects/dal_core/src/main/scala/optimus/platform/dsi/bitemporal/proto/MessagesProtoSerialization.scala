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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dal.messages.StreamsACLs
import optimus.platform.dal.messages.MessagesSubscriptionType
import optimus.platform.dal.messages.MessagesSubscription
import optimus.platform.dal.messages.StreamsEntitlement
import optimus.platform.dsi.bitemporal.MessagesNotificationResult.SimpleEntry
import optimus.platform.dsi.bitemporal.MessagesNotificationResult.TransactionEntry
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase.Option.Ack
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase.Option.IgnoreAck
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedContainedEvent
import optimus.platform.storable.SerializedContainedEvent.AppliedHeapEntity
import optimus.platform.storable.SerializedContainedEvent.ContainedEntity
import optimus.platform.storable.SerializedContainedEvent.StoredEntity
import optimus.platform.storable.SerializedContainedEvent.UniqueHeapEntity
import optimus.platform.storable.SerializedUpsertableTransaction

import scala.jdk.CollectionConverters._

trait MessagesProtoSerialization extends ProtoSerialization {

  implicit val publishMessagesSerializer: PublishMessagesCommandSerializer.type = PublishMessagesCommandSerializer
  implicit val publishTransactionSerializer: PublishTransactionMessagesCommandSerializer.type =
    PublishTransactionMessagesCommandSerializer

  implicit val createMessagesStreamSerializer: CreateMessagesStreamCommandSerializer.type =
    CreateMessagesStreamCommandSerializer
  implicit val changeMessagesSubscriptionSerializer: ChangeMessagesSubscriptionCommandSerializer.type =
    ChangeMessagesSubscriptionCommandSerializer
  implicit val closeMessagesStreamSerializer: CloseMessagesStreamCommandSerializer.type =
    CloseMessagesStreamCommandSerializer
  implicit val commitMessagesStreamSerializer: CommitMessagesStreamCommandSerializer.type =
    CommitMessagesStreamCommandSerializer
  implicit val publishMessagesResultSerializer: PublishMessagesResultSerializer.type =
    PublishMessagesResultSerializer
  implicit val createMessagesStreamResultSerializer: CreateMessagesStreamResultSerializer.type =
    CreateMessagesStreamResultSerializer
  implicit val changeMessagesSubscriptionResultSerializer: ChangeMessagesSubscriptionResultSerializer.type =
    ChangeMessagesSubscriptionResultSerializer
  implicit val closeMessagesStreamResultSerializer: CloseMessagesStreamResultSerializer.type =
    CloseMessagesStreamResultSerializer
  implicit val messagesNotificationResultSerializer: MessagesNotificationResultSerializer.type =
    MessagesNotificationResultSerializer
  implicit val streamsACLsCommandSerializer: StreamsACLsCommandSerializer.type =
    StreamsACLsCommandSerializer
  implicit val streamsACLsResultSerializer: StreamsACLsResultSerializer.type =
    StreamsACLsResultSerializer
}

trait MessagesCommandProtoSerializationBase extends BasicProtoSerialization {
  implicit val serializedMessageSerializer: SerializedContainedEventSerializer.type = SerializedContainedEventSerializer
  implicit val serializedTransactionSerializer: SerializedTransactionSerializer.type = SerializedTransactionSerializer

  implicit val entityReferenceToContainedEntitySerializer: EntityReferenceToContainedEntitySerializer.type =
    EntityReferenceToContainedEntitySerializer
  implicit val containedEntitySerializer: ContainedEntitySerializer.type = ContainedEntitySerializer
  implicit val messagesSubscriptionSerializer: MessagesSubscriptionSerializer.type = MessagesSubscriptionSerializer
  implicit val messagesNotificationEntrySerializer: MessagesNotificationEntrySerializer.type =
    MessagesNotificationEntrySerializer
  implicit val messagesNotificationTransactionEntrySerializer: MessagesNotificationTransactionEntrySerializer.type =
    MessagesNotificationTransactionEntrySerializer

  implicit val putApplicationEventSerializer: PutApplicationEventSerializer.type = PutApplicationEventSerializer

  implicit val streamsACLsSerializer: StreamsACLsSerializer.type = StreamsACLsSerializer

  implicit val hierarchicalEntitiesSerializer: HierarchicalEntitiesSerializer.type = HierarchicalEntitiesSerializer
}

object PublishMessagesCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesPublishCommand, PublishMessagesProto] {
  override def serialize(
      cmd: MessagesPublishCommand
  ): PublishMessagesProto = ??? /* {
    PublishMessagesProto.newBuilder
      .setMessageType(toProto(cmd.msg))
      .setWaitForAck(cmd.option.waitForAck)
      .build()
  } */

  override def deserialize(
      proto: PublishMessagesProto
  ): MessagesPublishCommand = ??? /* {
    val messageType = fromProto(proto.getMessageType)
    val option = if (proto.getWaitForAck) Ack else IgnoreAck
    MessagesPublishCommand(messageType, option)
  } */
}

object PublishTransactionMessagesCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesPublishTransactionCommand, PublishTransactionProto] {
  override def serialize(
      cmd: MessagesPublishTransactionCommand
  ): PublishTransactionProto =
    ??? /* PublishTransactionProto.newBuilder
      .setTransaction(toProto(cmd.msg))
      .build() */

  override def deserialize(
      proto: PublishTransactionProto
  ): MessagesPublishTransactionCommand = ??? /* {
    val transaction = fromProto(proto.getTransaction)
    MessagesPublishTransactionCommand(transaction)
  } */
}

object CreateMessagesStreamCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[CreateMessagesStream, CreateMessagesStreamProto] {
  override def serialize(cmd: CreateMessagesStream): CreateMessagesStreamProto = ??? /* {
    val streamProtoBuilder = CreateMessagesStreamProto.newBuilder
      .setStreamId(cmd.streamId)
      .addAllSubscriptions(cmd.subs.map(toProto(_)).asJava)
    cmd.appSpecificConsumerId match {
      case Some(consumerId) => streamProtoBuilder.setAppSpecificConsumerId(consumerId)
      case None             => ()
    }
    cmd.startTime.foreach(st => streamProtoBuilder.setStartTime(toProto(st)))
    streamProtoBuilder.build()
  } */
  override def deserialize(proto: CreateMessagesStreamProto): CreateMessagesStream = ??? /* {
    val streamId = proto.getStreamId
    val subscriptions = proto.getSubscriptionsList.asScala.map(fromProto(_)).toSet
    val appSpecificConsumerId = Option(proto.getAppSpecificConsumerId).filter(!_.isBlank)
    val startTime = if (proto.hasStartTime) Some(fromProto(proto.getStartTime)) else None
    CreateMessagesStream(streamId, subscriptions, appSpecificConsumerId, startTime)
  } */
}

object ChangeMessagesSubscriptionCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[ChangeMessagesSubscription, ChangeMessagesSubscriptionProto] {
  override def serialize(cmd: ChangeMessagesSubscription): ChangeMessagesSubscriptionProto = ??? /* {
    ChangeMessagesSubscriptionProto.newBuilder
      .setStreamId(cmd.streamId)
      .setChangeRequestId(cmd.changeRequestId)
      .addAllAddSubscriptions(cmd.newSubs.map(toProto(_)).asJava)
      .addAllRemoveSubscriptionIds(cmd.removeSubs.map(Integer.valueOf).asJava)
      .build
  } */
  override def deserialize(proto: ChangeMessagesSubscriptionProto): ChangeMessagesSubscription = ??? /* {
    val streamId = proto.getStreamId
    val changeRequestId = proto.getChangeRequestId
    val addSubscriptions = proto.getAddSubscriptionsList.asScala.map(fromProto(_))
    val removeSubscriptions = proto.getRemoveSubscriptionIdsList.asScala.map(_.intValue)
    ChangeMessagesSubscription(streamId, changeRequestId, addSubscriptions.toSet, removeSubscriptions.toSet)
  } */
}

object CloseMessagesStreamCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[CloseMessagesStream, CloseMessagesStreamProto] {
  override def serialize(cmd: CloseMessagesStream): CloseMessagesStreamProto = ??? /* {
    CloseMessagesStreamProto.newBuilder.setStreamId(cmd.streamId).build
  } */
  override def deserialize(proto: CloseMessagesStreamProto): CloseMessagesStream = ??? /* {
    CloseMessagesStream(proto.getStreamId)
  } */
}

object CommitMessagesStreamCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[CommitMessagesStream, CommitMessagesStreamProto] {
  override def serialize(cmd: CommitMessagesStream): CommitMessagesStreamProto = ??? /* {
    val builder = CommitMessagesStreamProto.newBuilder.setStreamId(cmd.streamId)
    cmd.commitIds.foreach(builder.addCommitIds)
    builder.build
  } */
  override def deserialize(proto: CommitMessagesStreamProto): CommitMessagesStream = ??? /* {
    CommitMessagesStream(proto.getStreamId, proto.getCommitIdsList.asScala.map(_.toLong))
  } */
}

object PublishMessagesResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesPublishSuccessResult, PublishMessagesSuccessProto] {
  override def serialize(cmd: MessagesPublishSuccessResult): PublishMessagesSuccessProto = ??? /* {
    PublishMessagesSuccessProto.newBuilder.setEventClassName(cmd.eventClassName).build
  } */
  override def deserialize(proto: PublishMessagesSuccessProto): MessagesPublishSuccessResult = ??? /* {
    MessagesPublishSuccessResult(proto.getEventClassName)
  } */
}

object CreateMessagesStreamResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesCreateStreamSuccessResult, CreateMessagesStreamSuccessProto] {
  override def serialize(cmd: MessagesCreateStreamSuccessResult): CreateMessagesStreamSuccessProto = ??? /* {
    CreateMessagesStreamSuccessProto.newBuilder.setStreamId(cmd.streamId).build
  } */
  override def deserialize(proto: CreateMessagesStreamSuccessProto): MessagesCreateStreamSuccessResult = ??? /* {
    MessagesCreateStreamSuccessResult(proto.getStreamId)
  } */
}

object ChangeMessagesSubscriptionResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesChangeSubscriptionSuccessResult, ChangeMessagesSubscriptionSuccessProto] {
  override def serialize(cmd: MessagesChangeSubscriptionSuccessResult): ChangeMessagesSubscriptionSuccessProto = ??? /* {
    ChangeMessagesSubscriptionSuccessProto.newBuilder
      .setStreamId(cmd.streamId)
      .setChangeRequestId(cmd.changeRequestId)
      .build
  } */
  override def deserialize(proto: ChangeMessagesSubscriptionSuccessProto): MessagesChangeSubscriptionSuccessResult = ??? /* {
    MessagesChangeSubscriptionSuccessResult(proto.getStreamId, proto.getChangeRequestId)
  } */
}

object CloseMessagesStreamResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesCloseStreamSuccessResult, CloseMessagesStreamSuccessProto] {
  override def serialize(cmd: MessagesCloseStreamSuccessResult): CloseMessagesStreamSuccessProto = ??? /* {
    CloseMessagesStreamSuccessProto.newBuilder.setStreamId(cmd.streamId).build
  } */
  override def deserialize(proto: CloseMessagesStreamSuccessProto): MessagesCloseStreamSuccessResult = ??? /* {
    MessagesCloseStreamSuccessResult(proto.getStreamId)
  } */
}

object MessagesNotificationResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesNotificationResult, MessagesNotificationProto] {
  override def serialize(cmd: MessagesNotificationResult): MessagesNotificationProto = ??? /* {
    val builder = MessagesNotificationProto.newBuilder
      .setStreamUuid(cmd.streamId)
      .setPublishRequestId(cmd.publishReqId)
      .setCommitId(cmd.commitId)
    cmd match {
      case serialized: DirectMessagesNotificationResult =>
        builder.addAllEntries(
          serialized.entries
            .collect { case e: SimpleEntry => e }
            .map(toProto(_))
            .asJava)
        builder.addAllTransactions(
          serialized.entries
            .collect { case e: TransactionEntry => e }
            .map(toProto(_))
            .asJava)
    }
    builder.build()
  } */
  override def deserialize(
      proto: MessagesNotificationProto
  ): MessagesNotificationResult = ??? /* {
    val streamId = proto.getStreamUuid
    val publishReqId = proto.getPublishRequestId
    val commitId = proto.getCommitId
    val entries = proto.getEntriesList.asScala.map(fromProto(_))
    val transactions = proto.getTransactionsList.asScala.map(fromProto(_))
    MessagesNotificationResult(streamId, publishReqId, commitId, entries ++ transactions)
  } */
}

object MessagesNotificationEntrySerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[SimpleEntry, MessagesNotificationEntryProto] {
  override def serialize(cmd: SimpleEntry): MessagesNotificationEntryProto =
    ??? /* MessagesNotificationEntryProto.newBuilder
      .setSubId(cmd.subId)
      .setSerializedMessage(toProto(cmd.serializedMsg))
      .build */

  override def deserialize(proto: MessagesNotificationEntryProto): SimpleEntry = ??? /* {
    val serializedMsg = fromProto(proto.getSerializedMessage)
    SimpleEntry(proto.getSubId, serializedMsg)
  } */
}

object MessagesNotificationTransactionEntrySerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[TransactionEntry, MessagesTransactionNotificationEntryProto] {
  override def serialize(
      cmd: TransactionEntry
  ): MessagesTransactionNotificationEntryProto =
    ??? /* MessagesTransactionNotificationEntryProto.newBuilder
      .setSubId(cmd.subId)
      .setClassName(cmd.className)
      .setSerializedMessage(toProto(cmd.serializedMsg))
      .build */

  override def deserialize(
      proto: MessagesTransactionNotificationEntryProto
  ): TransactionEntry = ??? /* {
    val serializedMsg = fromProto(proto.getSerializedMessage)
    TransactionEntry(proto.getSubId, proto.getClassName, serializedMsg)
  } */
}

// Map[EntityReference, ContainedEntity] is equal to EntityReferenceToContainedEntity message
// MessageType is defined as SerializedMessage
object SerializedContainedEventSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[SerializedContainedEvent, SerializedMessageProto] {
  override def serialize(cmd: SerializedContainedEvent): SerializedMessageProto = ??? /* {
    SerializedMessageProto.newBuilder
      .setSbe(toProto(cmd.sbe))
      .addAllEntityMap(cmd.entityMap.map { case (k, v) => toProto((k, v)) }.asJava)
      .build
  } */

  override def deserialize(proto: SerializedMessageProto): SerializedContainedEvent = ??? /* {
    val sbe = fromProto(proto.getSbe)
    val entityMap = proto.getEntityMapList.asScala.map(fromProto(_)).toMap
    SerializedContainedEvent(sbe, entityMap)
  } */
}

object EntityReferenceToContainedEntitySerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[(EntityReference, ContainedEntity), EntityReferenceToContainedEntityProto] {
  override def serialize(cmd: (EntityReference, ContainedEntity)): EntityReferenceToContainedEntityProto = ??? /* {
    EntityReferenceToContainedEntityProto.newBuilder
      .setEntityReference(toProto(cmd._1))
      .setContainedEntity(toProto(cmd._2))
      .build
  } */
  override def deserialize(proto: EntityReferenceToContainedEntityProto): (EntityReference, ContainedEntity) = ??? /* {
    val entityReference = fromProto(proto.getEntityReference)
    val containedEntity = fromProto(proto.getContainedEntity)
    (entityReference, containedEntity)
  } */
}

object ContainedEntitySerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[ContainedEntity, ContainedEntityProto] {
  override def serialize(cmd: ContainedEntity): ContainedEntityProto = ??? /* {
    cmd match {
      case a: AppliedHeapEntity =>
        ContainedEntityProto.newBuilder
          .setType(ContainedEntityProto.Type.APPLIED_HEAP_ENTITY)
          .setEntity(toProto(a.entity))
          .build
      case u: UniqueHeapEntity =>
        ContainedEntityProto.newBuilder
          .setType(ContainedEntityProto.Type.UNIQUE_HEAP_ENTITY)
          .setEntity(toProto(u.entity))
          .build
      case s: StoredEntity =>
        ContainedEntityProto.newBuilder
          .setType(ContainedEntityProto.Type.STORED_ENTITY)
          .setVt(toProto(s.vt))
          .setTt(toProto(s.tt))
          .build
    }
  } */
  override def deserialize(proto: ContainedEntityProto): ContainedEntity = ??? /* {
    proto.getType match {
      case ContainedEntityProto.Type.APPLIED_HEAP_ENTITY => AppliedHeapEntity(fromProto(proto.getEntity))
      case ContainedEntityProto.Type.UNIQUE_HEAP_ENTITY  => UniqueHeapEntity(fromProto(proto.getEntity))
      case ContainedEntityProto.Type.STORED_ENTITY       => StoredEntity(fromProto(proto.getVt), fromProto(proto.getTt))
    }
  } */
}

object MessagesSubscriptionSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[MessagesSubscription, MessagesSubscriptionProto] {
  override def serialize(
      cmd: MessagesSubscription
  ): MessagesSubscriptionProto = ??? /* {
    val subscriptionType = cmd.subscriptionType match {
      case MessagesSubscriptionType.ContainedEvent => MessagesSubscriptionProto.SubscriptionType.CONTAINED_EVENT
      case MessagesSubscriptionType.UpsertableTransaction =>
        MessagesSubscriptionProto.SubscriptionType.UPSERTABLE_TRANSACTION
      case MessagesSubscriptionType.Streamable => MessagesSubscriptionProto.SubscriptionType.STREAMABLE
    }

    MessagesSubscriptionProto.newBuilder
      .setSubId(cmd.subId)
      .setEventClassName(cmd.eventClassName)
      .setSubscriptionType(subscriptionType)
      .build
  } */

  override def deserialize(
      proto: MessagesSubscriptionProto
  ): MessagesSubscription = ??? /* {
    val subscriptionType = proto.getSubscriptionType match {
      case MessagesSubscriptionProto.SubscriptionType.CONTAINED_EVENT => MessagesSubscriptionType.ContainedEvent
      case MessagesSubscriptionProto.SubscriptionType.UPSERTABLE_TRANSACTION =>
        MessagesSubscriptionType.UpsertableTransaction
      case MessagesSubscriptionProto.SubscriptionType.STREAMABLE => MessagesSubscriptionType.Streamable
    }
    MessagesSubscription(
      subId = proto.getSubId,
      proto.getEventClassName,
      subscriptionType
    )
  } */
}

object SerializedTransactionSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[SerializedUpsertableTransaction, SerializedTransactionProto] {
  override def serialize(
      cmd: SerializedUpsertableTransaction
  ): SerializedTransactionProto =
    ??? /* SerializedTransactionProto.newBuilder
      .setPutApplicationEvent(toProto(cmd.putEvent))
      .build */

  override def deserialize(
      proto: SerializedTransactionProto
  ): SerializedUpsertableTransaction = ??? /* {
    val putEvent = fromProto(proto.getPutApplicationEvent)
    SerializedUpsertableTransaction(putEvent)
  } */
}

object StreamsACLsCommandSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[StreamsACLsCommand, StreamsACLsCommandProto] {
  override def serialize(
      cmd: StreamsACLsCommand
  ): StreamsACLsCommandProto =
    ??? /* StreamsACLsCommandProto.newBuilder
      .setStreamAppId(cmd.streamAppId)
      .addAllAcls(cmd.acls.map(streamsACLsSerializer.serialize).asJava)
      .build() */

  override def deserialize(
      proto: StreamsACLsCommandProto
  ): StreamsACLsCommand = ??? /* {
    val streamAppId = proto.getStreamAppId
    val acls = proto.getAclsList.asScala.map(streamsACLsSerializer.deserialize)
    StreamsACLsCommand(streamAppId, acls)
  } */
}

object StreamsACLsSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[StreamsACLs, StreamsACLsProto] {

  /* val protoEntitlementMapping = Map(
    StreamsACLsProto.Entitlement.Produce -> StreamsEntitlement.Produce,
    StreamsACLsProto.Entitlement.Consume -> StreamsEntitlement.Consume
  )

  val entitlementPrototMapping = protoEntitlementMapping
    .map { case (proto, action) => action -> proto }
    .toMap[StreamsEntitlement, StreamsACLsProto.Entitlement] */

  override def serialize(
      cmd: StreamsACLs
  ): StreamsACLsProto =
    ??? /* StreamsACLsProto.newBuilder
      .setEntitlement(entitlementPrototMapping(cmd.entitlement))
      .addAllHierarchicalEntities(cmd.hierarchicalEntities.map(hierarchicalEntitiesSerializer.serialize).asJava)
      .addAllInternalTopics(cmd.internalTopics.asJava)
      .build */

  override def deserialize(
      proto: StreamsACLsProto
  ): StreamsACLs = ??? /* {
    val entitlement = protoEntitlementMapping(proto.getEntitlement)
    val hierarchicalEntities = proto.getHierarchicalEntitiesList.asScala.map(hierarchicalEntitiesSerializer.deserialize)
    val internalTopics = proto.getInternalTopicsList.asScala
    StreamsACLs(entitlement, hierarchicalEntities, internalTopics)
  } */
}

object HierarchicalEntitiesSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[Seq[String], HierarchicalEntitiesProto] {
  override def serialize(
      cmd: Seq[String]
  ): HierarchicalEntitiesProto =
    ??? /* HierarchicalEntitiesProto.newBuilder
      .addAllEntityNames(cmd.asJava)
      .build() */

  override def deserialize(
      proto: HierarchicalEntitiesProto
  ): Seq[String] = ??? // proto.getEntityNamesList.asScala

}

object StreamsACLsResultSerializer
    extends MessagesCommandProtoSerializationBase
    with ProtoSerializer[StreamsACLsCommandSuccessResult, StreamsACLsCommandSuccessProto] {
  override def serialize(cmd: StreamsACLsCommandSuccessResult): StreamsACLsCommandSuccessProto = ??? /* {
    StreamsACLsCommandSuccessProto.newBuilder.setAppId(cmd.appId).build
  } */
  override def deserialize(proto: StreamsACLsCommandSuccessProto): StreamsACLsCommandSuccessResult = ??? /* {
    StreamsACLsCommandSuccessResult(proto.getAppId)
  } */
}
