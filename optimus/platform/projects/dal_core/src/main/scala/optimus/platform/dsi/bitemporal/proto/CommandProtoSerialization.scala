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

import optimus.dsi.base.DSIHaltException
import optimus.platform.util.Log
import optimus.platform.dal.prc.RedirectionReason
import optimus.platform.dsi.bitemporal._
import optimus.platform.util.Log
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.bitemporal.proto._

private[proto] trait CommandProtoSerializationBase
    extends GetCommandProtoSerialization
    with EventCommandProtoSerialization
    with EnumerateCommandProtoSerialization
    with WriteCommandProtoSerialization
    with QueryEntityMetadataProtoSerialization
    with AdminCommandProtoSerialization
    with PubSubProtoSerialization
    with IsEntitledProtoSerialization
    with MessagesProtoSerialization {
  implicit val commandSerializer: CommandSerializer.type = CommandSerializer
  implicit val resultSerializer: ResultSerializer.type = ResultSerializer
  implicit val errorResultSerializer: ErrorResultSerializer.type = ErrorResultSerializer
}

trait CommandProtoSerialization extends CommandProtoSerializationBase {
  final def toProto(cmd: Command): CommandProto = CommandSerializer.serialize(cmd)
  final def fromProto(proto: CommandProto): Command = CommandSerializer.deserialize(proto)

  final def toProto(res: Result): ResultProto = ResultSerializer.serialize(res)
  final def fromProto(proto: ResultProto): Result = ResultSerializer.deserialize(proto)

  final def toProto(res: VersioningResult): VersioningResultProto = VersioningResultSerializer.serialize(res)
  final def fromProto(proto: VersioningResultProto): VersioningResult = VersioningResultSerializer.deserialize(proto)
}

object ErrorResultSerializer
    extends CommandProtoSerializationBase
    with ProtoSerializer[ErrorResult, ErrorResultProto]
    with VersioningProtoSerialization
    with Log {

  override def deserialize(proto: ErrorResultProto): ErrorResult = ??? /* {
    val readAfter = if (proto.hasReadAfter) fromProto(proto.getReadAfter) else null
    val eref = if (proto.hasEntityReference) fromProto(proto.getEntityReference) else null
    val existingEref = if (proto.hasExistingEntityReference) Some(fromProto(proto.getExistingEntityReference)) else None
    val existingKey = if (proto.hasExistingKey) Some(fromProto(proto.getExistingKey)) else None
    val versioningRedirectionInfo = if (proto.hasRedirectionInfo) fromProto(proto.getRedirectionInfo) else null
    val pubSubStreamId = if (proto.hasPubsubStreamUuid) proto.getPubsubStreamUuid else ""

    val throwable = proto.getType match {
      // This code path is dead
      // case ErrorResultProto.Type.DUPLICATE_VERSION => new DuplicateVersionException(Instant.EPOCH, None)
      case ErrorResultProto.Type.OUTDATED_VERSION => new OutdatedVersionException(eref)
      case ErrorResultProto.Type.DUPLICATE_KEY =>
        DuplicateStoredKeyException(eref, readAfter, existingEref, existingKey)
      case ErrorResultProto.Type.DUPLICATE_UNIQUE_INDEX =>
        DuplicateUniqueIndexException(eref, readAfter, existingEref, existingKey)
      case ErrorResultProto.Type.NOT_LEAD_BROKER   => new NonLeaderBrokerException(proto.getMessage)
      case ErrorResultProto.Type.NOT_READER_BROKER => new NonReaderBrokerException(proto.getMessage)
      case ErrorResultProto.Type.ENTITLEMENT_FAILURE =>
        EntitlementCheckFailed.fromMessage(proto.getMessage, enrichMsg = false)
      case ErrorResultProto.Type.ASSERT_VALID_FAILURE    => new AssertValidException(proto.getMessage)
      case ErrorResultProto.Type.RETRYABLE_PROXY_SUB     => new RetryableProxySubException(proto.getMessage)
      case ErrorResultProto.Type.NON_RETRYABLE_PROXY_SUB => new NonRetryableProxySubException(proto.getMessage)
      case ErrorResultProto.Type.DSI_TRANSIENT_ERROR     => new DsiSpecificTransientError(proto.getMessage)
      case ErrorResultProto.Type.SESSION_ESTABLISHMENT   => new SessionEstablishmentException(proto.getMessage)
      case ErrorResultProto.Type.INCOMPLETE_WRITE_REQUEST =>
        new IncompleteWriteRequestException(versioningRedirectionInfo)
      // EntityReference gD4L1l0/1tbWdrwu/ZJCUw== fails here on deserializing effectDocs.
      case ErrorResultProto.Type.CREATE_PUBSUB_STREAM_ERROR =>
        new PubSubStreamCreationException(pubSubStreamId, proto.getMessage)
      case ErrorResultProto.Type.CLOSE_PUBSUB_STREAM_ERROR =>
        new PubSubStreamClosureException(pubSubStreamId, proto.getMessage)
      case ErrorResultProto.Type.SUBSCRIPTION_CHANGE_ERROR if proto.hasChangeSubscriptionRequestId =>
        val changeId = proto.getChangeSubscriptionRequestId
        new PubSubSubscriptionChangeException(changeId, pubSubStreamId, proto.getMessage)
      case ErrorResultProto.Type.PUBSUB_TRANSIENT_ERROR =>
        new PubSubTransientException(pubSubStreamId, proto.getMessage)
      case ErrorResultProto.Type.PUBSUB_MULTI_PARTITION_ERROR if proto.hasChangeSubscriptionRequestId =>
        val changeId = proto.getChangeSubscriptionRequestId
        new ChangeSubscriptionMultiPartitionException(
          pubSubStreamId,
          changeId,
          PubSubPartitionToSubIdsMapSerializer.deserialize(proto.getPartitionToSubMap))
      case ErrorResultProto.Type.PUBSUB_MULTI_PARTITION_ERROR =>
        new CreatePubSubStreamMultiPartitionException(
          pubSubStreamId,
          PubSubPartitionToSubIdsMapSerializer.deserialize(proto.getPartitionToSubMap))
      case ErrorResultProto.Type.WM_HALT_ERROR =>
        new DSIHaltException(proto.getMessage)
      case ErrorResultProto.Type.DUPLICATE_APP_EVENT_REFERENCE =>
        new DuplicateAppEventReferenceException(proto.getMessage)
      case ErrorResultProto.Type.MESSAGES_TRANSIENT_ERROR =>
        new MessagesTransientException(proto.getMessagesStreamUuid, proto.getMessage)
      case ErrorResultProto.Type.PUBLISH_MESSAGES_ERROR =>
        new MessagesPublishException(proto.getMessage)
      case ErrorResultProto.Type.CREATE_MESSAGES_STREAM_ERROR =>
        new MessagesStreamCreationException(proto.getMessagesStreamUuid, proto.getMessage)
      case ErrorResultProto.Type.CHANGE_MESSAGES_SUBSCRIPTION_ERROR =>
        new MessagesSubscriptionChangeException(
          proto.getMessagesStreamUuid,
          proto.getMessagesChangeSubscriptionRequestId,
          proto.getMessage)
      case ErrorResultProto.Type.CLOSE_MESSAGES_STREAM_ERROR =>
        new MessagesStreamClosureException(proto.getMessagesStreamUuid, proto.getMessage)
      case ErrorResultProto.Type.STREAMS_ACLS_COMMAND_ERROR =>
        new StreamsACLsCommandException(proto.getMessage)
      case _ => new DSISpecificError(proto.getMessage)
    }
    ErrorResult(throwable, readAfter)
  } */

  override def serialize(result: ErrorResult): ErrorResultProto = ??? /* {
    val builder = ErrorResultProto.newBuilder
    val errorType = result.error match {
      // case _: DuplicateVersionException => ErrorResultProto.Type.DUPLICATE_VERSION
      case e: OutdatedVersionException =>
        builder.setEntityReference(toProto(e.ref))
        ErrorResultProto.Type.OUTDATED_VERSION
      case _: NonLeaderBrokerException => ErrorResultProto.Type.NOT_LEAD_BROKER
      case _: NonReaderBrokerException => ErrorResultProto.Type.NOT_READER_BROKER
      case e: DuplicateStoredKeyException =>
        builder.setEntityReference(toProto(e.ref))
        e.inputEntityRef.foreach(r => builder.setExistingEntityReference(toProto(r)))
        e.key.foreach(k => builder.setExistingKey(toProto(k)))
        ErrorResultProto.Type.DUPLICATE_KEY
      case e: DuplicateUniqueIndexException =>
        builder.setEntityReference(toProto(e.ref))
        e.inputEntityRef.foreach(r => builder.setExistingEntityReference(toProto(r)))
        e.key.foreach(k => builder.setExistingKey(toProto(k)))
        ErrorResultProto.Type.DUPLICATE_UNIQUE_INDEX
      case e: PubSubStreamCreationException =>
        builder.setPubsubStreamUuid(e.streamId)
        ErrorResultProto.Type.CREATE_PUBSUB_STREAM_ERROR
      case e: PubSubStreamClosureException =>
        builder.setPubsubStreamUuid(e.streamId)
        ErrorResultProto.Type.CLOSE_PUBSUB_STREAM_ERROR
      case e: PubSubSubscriptionChangeException =>
        builder.setPubsubStreamUuid(e.streamId)
        builder.setChangeSubscriptionRequestId(e.changeRequestId)
        ErrorResultProto.Type.SUBSCRIPTION_CHANGE_ERROR
      case e: PubSubTransientException =>
        builder.setPubsubStreamUuid(e.streamId)
        ErrorResultProto.Type.PUBSUB_TRANSIENT_ERROR
      case e: CreatePubSubStreamMultiPartitionException =>
        builder.setPubsubStreamUuid(e.streamId)
        builder.setPartitionToSubMap(PubSubPartitionToSubIdsMapSerializer.serialize(e.partitionToSubMap))
        ErrorResultProto.Type.PUBSUB_MULTI_PARTITION_ERROR
      case e: ChangeSubscriptionMultiPartitionException =>
        builder.setPubsubStreamUuid(e.streamId)
        builder.setChangeSubscriptionRequestId(e.changeRequestId)
        builder.setPartitionToSubMap(PubSubPartitionToSubIdsMapSerializer.serialize(e.partitionToSubMap))
        ErrorResultProto.Type.PUBSUB_MULTI_PARTITION_ERROR
      case e: MessagesStreamCreationException =>
        builder.setMessagesStreamUuid(e.streamId)
        ErrorResultProto.Type.CREATE_MESSAGES_STREAM_ERROR
      case e: MessagesSubscriptionChangeException =>
        builder.setMessagesStreamUuid(e.streamId)
        builder.setMessagesChangeSubscriptionRequestId(e.changeRequestId)
        ErrorResultProto.Type.CHANGE_MESSAGES_SUBSCRIPTION_ERROR
      case e: MessagesStreamClosureException =>
        builder.setMessagesStreamUuid(e.streamId)
        ErrorResultProto.Type.CLOSE_MESSAGES_STREAM_ERROR
      case e: MessagesTransientException =>
        builder.setMessagesStreamUuid(e.streamId)
        ErrorResultProto.Type.MESSAGES_TRANSIENT_ERROR
      case e: MessagesPublishException      => ErrorResultProto.Type.PUBLISH_MESSAGES_ERROR
      case _: EntitlementCheckFailed        => ErrorResultProto.Type.ENTITLEMENT_FAILURE
      case _: AssertValidException          => ErrorResultProto.Type.ASSERT_VALID_FAILURE
      case _: RetryableProxySubException    => ErrorResultProto.Type.RETRYABLE_PROXY_SUB
      case _: NonRetryableProxySubException => ErrorResultProto.Type.NON_RETRYABLE_PROXY_SUB
      case _: DsiSpecificTransientError     => ErrorResultProto.Type.DSI_TRANSIENT_ERROR
      case _: SessionEstablishmentException => ErrorResultProto.Type.SESSION_ESTABLISHMENT
      case e: IncompleteWriteRequestException =>
        builder.setRedirectionInfo(toProto(e.redirectionInfo))
        ErrorResultProto.Type.INCOMPLETE_WRITE_REQUEST
      case e: DSIHaltException                    => ErrorResultProto.Type.WM_HALT_ERROR
      case _: DuplicateAppEventReferenceException => ErrorResultProto.Type.DUPLICATE_APP_EVENT_REFERENCE
      case _: ProtocolViolationException          => ErrorResultProto.Type.INTERNAL
      case _: StreamsACLsCommandException         => ErrorResultProto.Type.STREAMS_ACLS_COMMAND_ERROR
      case ex =>
        log.error("Server exception:", ex)
        ErrorResultProto.Type.INTERNAL
      // TODO(OPTIMUS-20204): Add the serialization logic for PubSubExceptions
    }
    val message = result.error.getMessage
    builder.setType(errorType)
    if (message != null) builder.setMessage(message)
    if (result.readAfter ne null) builder.setReadAfter(toProto(result.readAfter))
    builder.build
  } */
}

object CommandSerializer extends CommandProtoSerializationBase with ProtoSerializer[Command, CommandProto] {
  def isWriteCommand(c: CommandProto): Boolean = ??? /* {
    c.getType match {
      case CommandProto.Type.PUT                            => true
      case CommandProto.Type.INVALIDATE_AFTER               => true
      case CommandProto.Type.INVALIDATE_ALL_CURRENT         => true
      case CommandProto.Type.INVALIDATE_ALL_CURRENT_BY_REFS => true
      case CommandProto.Type.PUT_APPLICATION_EVENT          => true
      case CommandProto.Type.OBLITERATE                     => true
      case CommandProto.Type.ASSERT_VALID                   => true
      case CommandProto.Type.RESOLVE_KEYS                   => true
      case CommandProto.Type.ACCMETADATA_COMMAND            => true
      case CommandProto.Type.CREATE_SLOTS                   => true
      case CommandProto.Type.FILL_SLOT                      => true
      case CommandProto.Type.SYSTEM                         => true
      case _                                                => false
    }
  } */

  override def serialize(command: Command): CommandProto = ??? /* {
    command match {
      case s: Select => CommandProto.newBuilder.setSelect(toProto(s)).setType(CommandProto.Type.SELECT).build
      case p: Put    => CommandProto.newBuilder.setPut(toProto(p)).setType(CommandProto.Type.PUT).build
      case a: AssertValid =>
        CommandProto.newBuilder.setAssertValid(toProto(a)).setType(CommandProto.Type.ASSERT_VALID).build
      case ia: InvalidateAfter =>
        CommandProto.newBuilder.setInvalidateAfter(toProto(ia)).setType(CommandProto.Type.INVALIDATE_AFTER).build
      case iac: InvalidateAllCurrent =>
        CommandProto.newBuilder
          .setInvalidateAllCurrent(toProto(iac))
          .setType(CommandProto.Type.INVALIDATE_ALL_CURRENT)
          .build
      case ic: InvalidateAllCurrentByRefs =>
        CommandProto.newBuilder
          .setInvalidateAllCurrentByRefs(toProto(ic))
          .setType(CommandProto.Type.INVALIDATE_ALL_CURRENT_BY_REFS)
          .build
      case s: SystemCommand => CommandProto.newBuilder.setSystem(toProto(s)).setType(CommandProto.Type.SYSTEM).build
      case r: ResolveKeys =>
        CommandProto.newBuilder.setResolveKeys(toProto(r)).setType(CommandProto.Type.RESOLVE_KEYS).build
      case e: PutApplicationEvent =>
        CommandProto.newBuilder
          .setPutApplicationEvent(toProto(e))
          .setType(CommandProto.Type.PUT_APPLICATION_EVENT)
          .build
      case ek: GetBusinessEventByKey =>
        CommandProto.newBuilder
          .setSelectBusinessEventByKey(toProto(ek))
          .setType(CommandProto.Type.SELECT_BUSINESS_EVENT_BY_KEY)
          .build
      case er: GetBusinessEvent =>
        CommandProto.newBuilder
          .setSelectBusinessEventByEventRef(toProto(er))
          .setType(CommandProto.Type.SELECT_BUSINESS_EVENT_BY_EVENT_REF)
          .build
      case ecl: GetBusinessEventsByClass =>
        CommandProto.newBuilder
          .setSelectBusinessEventByEventClass(toProto(ecl))
          .setType(CommandProto.Type.SELECT_BUSINESS_EVENT_BY_CLASS)
          .build
      case eer: GetEntityEventTimeline =>
        CommandProto.newBuilder
          .setSelectBusinessEventByEntityRef(toProto(eer))
          .setType(CommandProto.Type.SELECT_BUSINESS_EVENT_BY_ENTITY_REF)
          .build
      case ett: GetEventTransactions =>
        CommandProto.newBuilder
          .setGetEventTransactions(toProto(ett))
          .setType(CommandProto.Type.GET_EVENT_TRANSACTIONS)
          .build
      case gie: GetInitiatingEvent =>
        CommandProto.newBuilder
          .setGetInitiatingEvent(toProto(gie))
          .setType(CommandProto.Type.GET_INITIATING_EVENT)
          .build
      case eevlt: GetEntityEventValidTimeline =>
        CommandProto.newBuilder
          .setSelectEntityEventValidTimeline(toProto(eevlt))
          .setType(CommandProto.Type.SELECT_ENTITY_EVENT_VALID_TIMELINE)
          .build
      case evtl: GetEventTimeline =>
        CommandProto.newBuilder
          .setSelectEventTimeline(toProto(evtl))
          .setType(CommandProto.Type.SELECT_EVENT_TIMELINE)
          .build
      case eae: GetAssociatedEntities =>
        CommandProto.newBuilder
          .setSelectBusinessEventAssociatedEntities(toProto(eae))
          .setType(CommandProto.Type.SELECT_BUSINESS_EVENT_ASSOCIATED_ENTITIES)
          .build
      case e: EnumerateKeys =>
        CommandProto.newBuilder.setEnumerateKeys(toProto(e)).setType(CommandProto.Type.ENUMERATE_KEYS).build
      case e: EnumerateIndices =>
        CommandProto.newBuilder.setEnumerateIndices(toProto(e)).setType(CommandProto.Type.ENUMERATE_INDICES).build
      case o: Obliterate =>
        CommandProto.newBuilder.setObliterate(toProto(o)).setType(CommandProto.Type.OBLITERATE).build
      case g: GetInfo => CommandProto.newBuilder.setType(CommandProto.Type.GET_INFO).build
      case t: TemporalityQueryCommand =>
        CommandProto.newBuilder.setTemporalityQuery(toProto(t)).setType(CommandProto.Type.TEMPORALITY_QUERY).build
      case s: SessionTokenRequest =>
        CommandProto.newBuilder.setType(CommandProto.Type.SESSION_TOKEN).setSessionToken(toProto(s)).build
      case h: Heartbeat => CommandProto.newBuilder.setType(CommandProto.Type.HEARTBEAT).setHeartbeat(toProto(h)).build
      case eai: GetAuditInfo =>
        CommandProto.newBuilder.setGetAuditinfo(toProto(eai)).setType(CommandProto.Type.GET_AUDITINFO).build
      case g: GetProtoFileProperties =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.GET_PROTOFILE_PROPERTIES)
          .setGetProtofileProperties(toProto(g))
          .build
      case c: RoleMembershipQuery =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.ROLE_MEMBERSHIP_QUERY)
          .setRoleMembershipQuery(toProto(c))
          .build
      case c: CountGroupings =>
        CommandProto.newBuilder.setCountGroupings(toProto(c)).setType(CommandProto.Type.COUNT_GROUPINGS).build
      case c: Count => CommandProto.newBuilder.setCount(toProto(c)).setType(CommandProto.Type.COUNT).build
      case cs: CreateSlots =>
        CommandProto.newBuilder.setType(CommandProto.Type.CREATE_SLOTS).setCreateSlots(toProto(cs)).build
      case gs: GetSlots => CommandProto.newBuilder.setType(CommandProto.Type.GET_SLOTS).setGetSlots(toProto(gs)).build
      case grnas: GetRefsNotAtSlot =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.GET_REFS_NOT_AT_SLOT)
          .setGetRefsNotAtSlot(toProto(grnas))
          .build
      case fs: FillSlot => CommandProto.newBuilder.setType(CommandProto.Type.FILL_SLOT).setFillSlot(toProto(fs)).build
      case m: QueryEntityMetadata =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.QUERY_ENTITY_METADATA)
          .setQueryEntityMetadata(toProto(m))
          .build
      case s: SelectSpace =>
        CommandProto.newBuilder.setSelectSpace(toProto(s)).setType(CommandProto.Type.SELECT_SPACE).build
      case _: ErrorCommand => ??? // must not get here.  XXX Better hierarchy: SerializableCommand <: Command
      case ex: ExpressionQueryCommand =>
        CommandProto.newBuilder.setExpressionQuery(toProto(ex)).setType(CommandProto.Type.EXPRESSION_QUERY).build
      case cpa: CanPerformAction =>
        CommandProto.newBuilder.setType(CommandProto.Type.CAN_PERFORM_ACTION).setCanPerformAction(toProto(cpa)).build
      case acc: AccMetadataCommand =>
        CommandProto.newBuilder.setAccMetadataCommand(toProto(acc)).setType(CommandProto.Type.ACCMETADATA_COMMAND).build
      case c: CreatePubSubStream =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CREATE_PUBSUB_STREAM)
          .setCreatePubsubStream(toProto(c))
          .build
      case c: ChangeSubscription =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CHANGE_SUBSCRIPTION)
          .setChangeSubscription(toProto(c))
          .build
      case c: ClosePubSubStream =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CLOSE_PUBSUB_STREAM)
          .setClosePubsubStream(toProto(c))
          .build
      case cns: CreateNewSession =>
        CommandProto.newBuilder.setCreateNewSession(toProto(cns)).setType(CommandProto.Type.CREATE_NEW_SESSION).build
      case ds: DiscoverServices =>
        CommandProto.newBuilder.setDiscoverServices(toProto(ds)).setType(CommandProto.Type.DISCOVER_SERVICES).build
      case ire: IsEntitled =>
        CommandProto.newBuilder.setType(CommandProto.Type.IS_ENTITLED).setIsEntitled(toProto(ire)).build
      case c: MessagesPublishCommand =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.PUBLISH_MESSAGES)
          .setPublishMessages(toProto(c))
          .build
      case c: MessagesPublishTransactionCommand =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.PUBLISH_TRANSACTION)
          .setPublishTransaction(toProto(c))
          .build
      case c: CreateMessagesStream =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CREATE_MESSAGES_STREAM)
          .setCreateMessagesStream(toProto(c))
          .build
      case c: ChangeMessagesSubscription =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CHANGE_MESSAGES_SUBSCRIPTION)
          .setChangeMessagesSubscription(toProto(c))
          .build
      case c: CloseMessagesStream =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.CLOSE_MESSAGES_STREAM)
          .setCloseMessagesStream(toProto(c))
          .build
      case c: CommitMessagesStream =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.COMMIT_MESSAGES_STREAM)
          .setCommitMessagesStream(toProto(c))
          .build
      case c: StreamsACLsCommand =>
        CommandProto.newBuilder
          .setType(CommandProto.Type.STREAMS_ACLS_COMMAND)
          .setStreamsAclsCommand(toProto(c))
          .build
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported command in CommandProto serialization: $command.")
    }
  } */

  override def deserialize(proto: CommandProto): Command = ??? /* {
    proto.getType() match {
      case CommandProto.Type.SELECT                              => fromProto(proto.getSelect())
      case CommandProto.Type.PUT                                 => fromProto(proto.getPut)
      case CommandProto.Type.ASSERT_VALID                        => fromProto(proto.getAssertValid)
      case CommandProto.Type.INVALIDATE_AFTER                    => fromProto(proto.getInvalidateAfter)
      case CommandProto.Type.INVALIDATE_ALL_CURRENT              => fromProto(proto.getInvalidateAllCurrent)
      case CommandProto.Type.INVALIDATE_ALL_CURRENT_BY_REFS      => fromProto(proto.getInvalidateAllCurrentByRefs)
      case CommandProto.Type.SYSTEM                              => fromProto(proto.getSystem)
      case CommandProto.Type.RESOLVE_KEYS                        => fromProto(proto.getResolveKeys)
      case CommandProto.Type.GET_INITIATING_EVENT                => fromProto(proto.getGetInitiatingEvent)
      case CommandProto.Type.SELECT_BUSINESS_EVENT_BY_KEY        => fromProto(proto.getSelectBusinessEventByKey)
      case CommandProto.Type.SELECT_BUSINESS_EVENT_BY_EVENT_REF  => fromProto(proto.getSelectBusinessEventByEventRef)
      case CommandProto.Type.SELECT_BUSINESS_EVENT_BY_ENTITY_REF => fromProto(proto.getSelectBusinessEventByEntityRef)
      case CommandProto.Type.SELECT_ENTITY_EVENT_VALID_TIMELINE  => fromProto(proto.getSelectEntityEventValidTimeline)
      case CommandProto.Type.SELECT_EVENT_TIMELINE               => fromProto(proto.getSelectEventTimeline)
      case CommandProto.Type.GET_EVENT_TRANSACTIONS              => fromProto(proto.getGetEventTransactions)
      case CommandProto.Type.SELECT_BUSINESS_EVENT_ASSOCIATED_ENTITIES =>
        fromProto(proto.getSelectBusinessEventAssociatedEntities)
      case CommandProto.Type.OBLITERATE                     => fromProto(proto.getObliterate)
      case CommandProto.Type.ENUMERATE_KEYS                 => fromProto(proto.getEnumerateKeys)
      case CommandProto.Type.ENUMERATE_INDICES              => fromProto(proto.getEnumerateIndices)
      case CommandProto.Type.GET_INFO                       => GetInfo()
      case CommandProto.Type.PUT_APPLICATION_EVENT          => fromProto(proto.getPutApplicationEvent)
      case CommandProto.Type.TEMPORALITY_QUERY              => fromProto(proto.getTemporalityQuery)
      case CommandProto.Type.SESSION_TOKEN                  => fromProto(proto.getSessionToken)
      case CommandProto.Type.SELECT_BUSINESS_EVENT_BY_CLASS => fromProto(proto.getSelectBusinessEventByEventClass)
      case CommandProto.Type.HEARTBEAT                      => fromProto(proto.getHeartbeat)
      case CommandProto.Type.HISTORIC_QUERY                 => ???
      case CommandProto.Type.GET_AUDITINFO                  => fromProto(proto.getGetAuditinfo)
      case CommandProto.Type.GET_PROTOFILE_PROPERTIES       => fromProto(proto.getGetProtofileProperties)
      case CommandProto.Type.ROLE_MEMBERSHIP_QUERY          => fromProto(proto.getRoleMembershipQuery)
      case CommandProto.Type.COUNT_GROUPINGS                => fromProto(proto.getCountGroupings)
      case CommandProto.Type.COUNT                          => fromProto(proto.getCount)
      case CommandProto.Type.PROXY_SUBSCRIBE =>
        throw new UnsupportedOperationException("ProxySubscribe command support has been removed")
      case CommandProto.Type.CREATE_SLOTS                 => fromProto(proto.getCreateSlots)
      case CommandProto.Type.GET_SLOTS                    => fromProto(proto.getGetSlots)
      case CommandProto.Type.GET_REFS_NOT_AT_SLOT         => fromProto(proto.getGetRefsNotAtSlot)
      case CommandProto.Type.FILL_SLOT                    => fromProto(proto.getFillSlot)
      case CommandProto.Type.QUERY_ENTITY_METADATA        => fromProto(proto.getQueryEntityMetadata)
      case CommandProto.Type.SELECT_SPACE                 => fromProto(proto.getSelectSpace())
      case CommandProto.Type.EXPRESSION_QUERY             => fromProto(proto.getExpressionQuery())
      case CommandProto.Type.CAN_PERFORM_ACTION           => fromProto(proto.getCanPerformAction())
      case CommandProto.Type.ACCMETADATA_COMMAND          => fromProto(proto.getAccMetadataCommand())
      case CommandProto.Type.CREATE_PUBSUB_STREAM         => fromProto(proto.getCreatePubsubStream)
      case CommandProto.Type.CHANGE_SUBSCRIPTION          => fromProto(proto.getChangeSubscription)
      case CommandProto.Type.CLOSE_PUBSUB_STREAM          => fromProto(proto.getClosePubsubStream)
      case CommandProto.Type.CREATE_NEW_SESSION           => fromProto(proto.getCreateNewSession)
      case CommandProto.Type.DISCOVER_SERVICES            => fromProto(proto.getDiscoverServices)
      case CommandProto.Type.IS_ENTITLED                  => fromProto(proto.getIsEntitled)
      case CommandProto.Type.PUBLISH_MESSAGES             => fromProto(proto.getPublishMessages)
      case CommandProto.Type.CREATE_MESSAGES_STREAM       => fromProto(proto.getCreateMessagesStream)
      case CommandProto.Type.CHANGE_MESSAGES_SUBSCRIPTION => fromProto(proto.getChangeMessagesSubscription)
      case CommandProto.Type.CLOSE_MESSAGES_STREAM        => fromProto(proto.getCloseMessagesStream)
      case CommandProto.Type.COMMIT_MESSAGES_STREAM       => fromProto(proto.getCommitMessagesStream)
      case CommandProto.Type.PUBLISH_TRANSACTION          => fromProto(proto.getPublishTransaction)
      case CommandProto.Type.STREAMS_ACLS_COMMAND         => fromProto(proto.getStreamsAclsCommand)
    }
  } */
}

object ResultSerializer extends CommandProtoSerializationBase with ProtoSerializer[Result, ResultProto] {

  override def serialize(result: Result): ResultProto = ??? /* {
    val resultBuilder = result match {
      case s: SelectResult => ResultProto.newBuilder.setSelectResult(toProto(s)).setType(ResultProto.Type.SELECT)
      case p: PartialSelectResult =>
        ResultProto.newBuilder.setPartialSelectResult(toProto(p)).setType(ResultProto.Type.SELECT_PARTIAL)
      case s: PutResult => ResultProto.newBuilder.setPutResult(toProto(s)).setType(ResultProto.Type.PUT)
      case a: AssertValidResult =>
        ResultProto.newBuilder.setAssertValidResult(toProto(a)).setType(ResultProto.Type.ASSERT_VALID)
      case ia: InvalidateAfterResult =>
        ResultProto.newBuilder.setInvalidateAfterResult(toProto(ia)).setType(ResultProto.Type.INVALIDATE_AFTER)
      case iac: InvalidateAllCurrentResult =>
        ResultProto.newBuilder
          .setInvalidateAllCurrentResult(toProto(iac))
          .setType(ResultProto.Type.INVALIDATE_ALL_CURRENT)
      case e: ErrorResult => ResultProto.newBuilder.setErrorResult(toProto(e)).setType(ResultProto.Type.ERROR)
      case s: SystemCommandResult =>
        ResultProto.newBuilder.setSystemResult(toProto(s)).setType(ResultProto.Type.SYSTEM)
      case r: ResolveKeysResult =>
        ResultProto.newBuilder.setResolveKeysResult(toProto(r)).setType(ResultProto.Type.RESOLVE_KEYS)
      case r: PutApplicationEventResult =>
        ResultProto.newBuilder
          .setPutApplicationEventResult(toProto(r))
          .setType(ResultProto.Type.PUT_APPLICATION_EVENT)
      case ber: GetBusinessEventResult =>
        ResultProto.newBuilder
          .setSelectBusinessEventResult(toProto(ber))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT)
      case pber: PartialGetBusinessEventResult =>
        ResultProto.newBuilder
          .setPartialSelectBusinessEventResult(toProto(pber))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_PARTIAL)
      case ber: GetBusinessEventWithTTToResult =>
        ResultProto.newBuilder
          .setSelectBusinessEventWithTttoResult(toProto(ber))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_WITH_TTTO)
      case betr: GetEntityEventTimelineResult =>
        ResultProto.newBuilder
          .setSelectBusinessEventTimelineResult(toProto(betr))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_TIMELINE)
      case betr: PartialGetEntityEventTimelineResult =>
        ResultProto.newBuilder
          .setPartialSelectBusinessEventTimelineResult(toProto(betr))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_TIMELINE_PARTIAL)
      case betr: GetEntityEventValidTimelineResult =>
        ResultProto.newBuilder
          .setSelectBusinessEventValidTimelineResult(toProto(betr))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_VALID_TIMELINE)
      case gier: GetInitiatingEventResult =>
        ResultProto.newBuilder
          .setGetInitiatingEventResult(toProto(gier))
          .setType(ResultProto.Type.GET_INITIATING_EVENT)
      case r: EnumerateKeysResult =>
        ResultProto.newBuilder.setEnumerateKeysResult(toProto(r)).setType(ResultProto.Type.ENUMERATE_KEYS)
      case r: EnumerateIndicesResult =>
        ResultProto.newBuilder.setEnumerateIndicesResult(toProto(r)).setType(ResultProto.Type.ENUMERATE_INDICES)
      case pr: PartialEnumerateKeysResult =>
        ResultProto.newBuilder
          .setPartialEnumerateKeysResult(toProto(pr))
          .setType(ResultProto.Type.ENUMERATE_KEYS_PARTIAL)
      case pr: PartialEnumerateIndicesResult =>
        ResultProto.newBuilder
          .setPartialEnumerateIndicesResult(toProto(pr))
          .setType(ResultProto.Type.ENUMERATE_INDICES_PARTIAL)
      case g: GetInfoResult =>
        ResultProto.newBuilder.setGetInfoResult(toProto(g)).setType(ResultProto.Type.GET_INFO)
      case s: SessionTokenResult =>
        ResultProto.newBuilder.setSessionTokenResult(toProto(s)).setType(ResultProto.Type.SESSION_TOKEN)
      case h: HeartbeatResult =>
        ResultProto.newBuilder.setHeartbeatResult(toProto(h)).setType(ResultProto.Type.HEARTBEAT)
      case g: GetProtoFilePropertiesResult =>
        ResultProto.newBuilder
          .setGetProtofilePropertiesResult(toProto(g))
          .setType(ResultProto.Type.GET_PROTOFILE_PROPERTIES_RESULT)
      case air: GetAuditInfoResult =>
        ResultProto.newBuilder.setGetAuditinfoResult(toProto(air)).setType(ResultProto.Type.GET_AUDITINFO)
      case pair: PartialGetAuditInfoResult =>
        ResultProto.newBuilder
          .setPartialGetAuditinfoResult(toProto(pair))
          .setType(ResultProto.Type.GET_AUDITINFO_PARTIAL)
      case VoidResult =>
        ResultProto.newBuilder.setVoidResult(VoidResultProto.newBuilder.build).setType(ResultProto.Type.VOID)
      case q: QueryReferenceResult =>
        ResultProto.newBuilder.setQueryReferenceResult(toProto(q)).setType(ResultProto.Type.REFERENCE_QUERY)
      case q: PartialQueryReferenceResult =>
        ResultProto.newBuilder
          .setPartialQueryReferenceResult(toProto(q))
          .setType(ResultProto.Type.REFERENCE_QUERY_PARTIAL)
      case q: RoleMembershipResult =>
        ResultProto.newBuilder
          .setRoleMembershipResult(toProto(q))
          .setType(ResultProto.Type.ROLE_MEMBERSHIP_RESULT)
      case c: CountResult => ResultProto.newBuilder.setCountResult(toProto(c)).setType(ResultProto.Type.COUNT)
      case cs: CreateSlotsResult =>
        ResultProto.newBuilder.setCreateSlotsResult(toProto(cs)).setType(ResultProto.Type.CREATE_SLOTS_RESULT)
      case gs: GetSlotsResult =>
        ResultProto.newBuilder.setGetSlotsResult(toProto(gs)).setType(ResultProto.Type.GET_SLOTS_RESULT)
      case grnas: GetRefsNotAtSlotResult =>
        ResultProto.newBuilder
          .setGetRefsNotAtSlotResult(toProto(grnas))
          .setType(ResultProto.Type.GET_REFS_NOT_AT_SLOT_RESULT)
      case m: QueryEntityMetadataResult =>
        ResultProto.newBuilder
          .setQueryEntityMetadataResult(toProto(m))
          .setType(ResultProto.Type.QUERY_ENTITY_METADATA_RESULT)
      case ssr: SelectSpaceResult =>
        ResultProto.newBuilder.setSelectSpaceResult(toProto(ssr)).setType(ResultProto.Type.SELECT_SPACE_RESULT)
      case ssr: PartialSelectSpaceResult =>
        ResultProto.newBuilder
          .setPartialSelectSpaceResult(toProto(ssr))
          .setType(ResultProto.Type.PARTIAL_SELECT_SPACE_RESULT)
      case qr: QueryResult => ResultProto.newBuilder.setQueryResult(toProto(qr)).setType(ResultProto.Type.QUERY_RESULT)
      case qr: PartialQueryResult =>
        ResultProto.newBuilder.setPartialQueryResult(toProto(qr)).setType(ResultProto.Type.PARTIAL_QUERY_RESULT)
      case bevt: GetEntityEventValidTimelineLazyLoadResult =>
        ResultProto.newBuilder
          .setSelectBusinessEventValidTimelineLazyLoadResult(toProto(bevt))
          .setType(ResultProto.Type.SELECT_BUSINESS_EVENT_VALID_TIMELINE_LAZY_LOAD)
      case br: BooleanResult =>
        ResultProto.newBuilder.setBooleanResult(toProto(br)).setType(ResultProto.Type.BOOLEAN_RESULT)
      case acc: AccTableResult =>
        ResultProto.newBuilder.setAccTableResult(toProto(acc)).setType(ResultProto.Type.ACC_TABLE_RESULT)
      case c: CreatePubSubStreamSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CREATE_PUBSUB_STREAM)
          .setCreatePubsubStreamResult(toProto(c))
      case c: ChangeSubscriptionSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CHANGE_SUBSCRIPTION)
          .setChangeSubscriptionResult(toProto(c))
      case c: ClosePubSubStreamSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CLOSE_PUBSUB_STREAM)
          .setClosePubsubStreamResult(toProto(c))
      case c: PubSubSowResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_SOW_RESULT)
          .setPubsubSowResult(toProto(c))
      case c: PubSubNotificationResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_NOTIFICATION)
          .setPubsubNotificationResult(toProto(c))
      case c: PubSubNotificationPartialResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_NOTIFICATION_PARTIAL_RESULT)
          .setPubsubNotificationPartialResultProto(toProto(c))
      case c: PubSubHeartbeatResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_HEARTBEAT)
          .setPubsubHeartbeatResult(toProto(c))
      case c: PubSubStreamResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_STREAM_EVENT)
          .setPubsubStreamEventResult(toProto(c))
      case c: PubSubGlobalResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBSUB_GLOBAL_EVENT)
          .setPubsubGlobalEventResult(toProto(c))
      case cnsr: CreateNewSessionResult =>
        ResultProto.newBuilder.setType(ResultProto.Type.CREATE_NEW_SESSION).setCreateNewSessionResult(toProto(cnsr))
      case dsr: DiscoverServicesResult =>
        ResultProto.newBuilder.setType(ResultProto.Type.DISCOVER_SERVICES).setDiscoverServicesResult(toProto(dsr))
      case DalPrcRedirectionResult(reason) =>
        ResultProto.newBuilder.setType(ResultProto.Type.PRC_REDIRECTION).setRedirectionReason(reasonToProto(reason))
      case pps: PrcPartialSelectResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PRC_SELECT_RESULT_PARTIAL)
          .setPrcPartialSelectResult(toProto(pps))
      case ier: IsEntitledResult =>
        ResultProto.newBuilder.setType(ResultProto.Type.IS_ENTITLED_RESULT).setIsEntitledResult(toProto(ier))
      case c: MessagesPublishSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.PUBLISH_MESSAGES)
          .setPublishMessagesResult(toProto(c))
      case c: MessagesCreateStreamSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CREATE_MESSAGES_STREAM)
          .setCreateMessagesStreamResult(toProto(c))
      case c: MessagesChangeSubscriptionSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CHANGE_MESSAGES_SUBSCRIPTION)
          .setChangeMessagesSubscriptionResult(toProto(c))
      case c: MessagesCloseStreamSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.CLOSE_MESSAGES_STREAM)
          .setCloseMessagesStreamResult(toProto(c))
      case c: MessagesNotificationResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.MESSAGES_NOTIFICATION)
          .setMessagesNotificationResult(toProto(c))
      case c: StreamsACLsCommandSuccessResult =>
        ResultProto.newBuilder
          .setType(ResultProto.Type.STREAMS_ACLS_COMMAND)
          .setStreamsAclsCommandResult(toProto(c))
      case _ => throw new UnsupportedOperationException(s"Unsupported result in ResultProto serialization: $result.")
    }
    result match {
      case resWithStats: HasResultStats[_] =>
        resWithStats.getResultStats().foreach(stats => resultBuilder.setResultStats(toProto(stats)))
      case _ =>
    }
    resultBuilder.build
  } */

  override def deserialize(proto: ResultProto): Result = ??? /* {
    var result = proto.getType match {
      case ResultProto.Type.SELECT                          => fromProto(proto.getSelectResult)
      case ResultProto.Type.SELECT_PARTIAL                  => fromProto(proto.getPartialSelectResult)
      case ResultProto.Type.PUT                             => fromProto(proto.getPutResult)
      case ResultProto.Type.ASSERT_VALID                    => fromProto(proto.getAssertValidResult)
      case ResultProto.Type.INVALIDATE_AFTER                => fromProto(proto.getInvalidateAfterResult)
      case ResultProto.Type.INVALIDATE_ALL_CURRENT          => fromProto(proto.getInvalidateAllCurrentResult)
      case ResultProto.Type.VOID                            => VoidResult
      case ResultProto.Type.ERROR                           => fromProto(proto.getErrorResult)
      case ResultProto.Type.SYSTEM                          => fromProto(proto.getSystemResult)
      case ResultProto.Type.RESOLVE_KEYS                    => fromProto(proto.getResolveKeysResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT           => fromProto(proto.getSelectBusinessEventResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_PARTIAL   => fromProto(proto.getPartialSelectBusinessEventResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_WITH_TTTO => fromProto(proto.getSelectBusinessEventWithTttoResult)
      case ResultProto.Type.GET_INITIATING_EVENT            => fromProto(proto.getGetInitiatingEventResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_TIMELINE  => fromProto(proto.getSelectBusinessEventTimelineResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_TIMELINE_PARTIAL =>
        fromProto(proto.getPartialSelectBusinessEventTimelineResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_VALID_TIMELINE =>
        fromProto(proto.getSelectBusinessEventValidTimelineResult)
      case ResultProto.Type.ENUMERATE_KEYS            => fromProto(proto.getEnumerateKeysResult)
      case ResultProto.Type.ENUMERATE_INDICES         => fromProto(proto.getEnumerateIndicesResult)
      case ResultProto.Type.ENUMERATE_KEYS_PARTIAL    => fromProto(proto.getPartialEnumerateKeysResult)
      case ResultProto.Type.ENUMERATE_INDICES_PARTIAL => fromProto(proto.getPartialEnumerateIndicesResult)
      case ResultProto.Type.GET_INFO                  => fromProto(proto.getGetInfoResult)
      case ResultProto.Type.PUT_APPLICATION_EVENT     => fromProto(proto.getPutApplicationEventResult)
      case ResultProto.Type.SESSION_TOKEN =>
        fromProto(proto.getSessionTokenResult)
      case ResultProto.Type.REFERENCE_QUERY =>
        fromProto(proto.getQueryReferenceResult)
      case ResultProto.Type.REFERENCE_QUERY_PARTIAL =>
        fromProto(proto.getPartialQueryReferenceResult)
      case ResultProto.Type.HEARTBEAT                       => fromProto(proto.getHeartbeatResult)
      case ResultProto.Type.GET_PROTOFILE_PROPERTIES_RESULT => fromProto(proto.getGetProtofilePropertiesResult)
      case ResultProto.Type.GET_AUDITINFO                   => fromProto(proto.getGetAuditinfoResult)
      case ResultProto.Type.GET_AUDITINFO_PARTIAL           => fromProto(proto.getPartialGetAuditinfoResult)
      case ResultProto.Type.ROLE_MEMBERSHIP_RESULT          => fromProto(proto.getRoleMembershipResult)
      case ResultProto.Type.COUNT                           => fromProto(proto.getCountResult)
      case ResultProto.Type.PROXY_SUBSCRIBE_RESULT =>
        throw new UnsupportedOperationException("ProxySubscribe command support has been removed")
      case ResultProto.Type.CREATE_SLOTS_RESULT          => fromProto(proto.getCreateSlotsResult)
      case ResultProto.Type.GET_SLOTS_RESULT             => fromProto(proto.getGetSlotsResult)
      case ResultProto.Type.GET_REFS_NOT_AT_SLOT_RESULT  => fromProto(proto.getGetRefsNotAtSlotResult)
      case ResultProto.Type.QUERY_ENTITY_METADATA_RESULT => fromProto(proto.getQueryEntityMetadataResult)
      case ResultProto.Type.SELECT_SPACE_RESULT          => fromProto(proto.getSelectSpaceResult)
      case ResultProto.Type.PARTIAL_SELECT_SPACE_RESULT  => fromProto(proto.getPartialSelectSpaceResult)
      case ResultProto.Type.QUERY_RESULT                 => fromProto(proto.getQueryResult)
      case ResultProto.Type.PARTIAL_QUERY_RESULT         => fromProto(proto.getPartialQueryResult)
      case ResultProto.Type.SELECT_BUSINESS_EVENT_VALID_TIMELINE_LAZY_LOAD =>
        fromProto(proto.getSelectBusinessEventValidTimelineLazyLoadResult)
      case ResultProto.Type.BOOLEAN_RESULT       => fromProto(proto.getBooleanResult)
      case ResultProto.Type.ACC_TABLE_RESULT     => fromProto(proto.getAccTableResult)
      case ResultProto.Type.CREATE_PUBSUB_STREAM => fromProto(proto.getCreatePubsubStreamResult)
      case ResultProto.Type.CHANGE_SUBSCRIPTION  => fromProto(proto.getChangeSubscriptionResult)
      case ResultProto.Type.CLOSE_PUBSUB_STREAM  => fromProto(proto.getClosePubsubStreamResult)
      case ResultProto.Type.PUBSUB_SOW_RESULT    => fromProto(proto.getPubsubSowResult)
      case ResultProto.Type.PUBSUB_NOTIFICATION  => fromProto(proto.getPubsubNotificationResult)
      case ResultProto.Type.PUBSUB_NOTIFICATION_PARTIAL_RESULT =>
        fromProto(proto.getPubsubNotificationPartialResultProto)
      case ResultProto.Type.PUBSUB_HEARTBEAT    => fromProto(proto.getPubsubHeartbeatResult)
      case ResultProto.Type.PUBSUB_STREAM_EVENT => fromProto(proto.getPubsubStreamEventResult)
      case ResultProto.Type.PUBSUB_GLOBAL_EVENT => fromProto(proto.getPubsubGlobalEventResult)
      case ResultProto.Type.CREATE_NEW_SESSION  => fromProto(proto.getCreateNewSessionResult)
      case ResultProto.Type.DISCOVER_SERVICES   => fromProto(proto.getDiscoverServicesResult)
      case ResultProto.Type.PRC_REDIRECTION     => DalPrcRedirectionResult(reasonFromProto(proto.getRedirectionReason))
      case ResultProto.Type.PRC_SELECT_RESULT_PARTIAL    => fromProto(proto.getPrcPartialSelectResult)
      case ResultProto.Type.IS_ENTITLED_RESULT           => fromProto(proto.getIsEntitledResult)
      case ResultProto.Type.PUBLISH_MESSAGES             => fromProto(proto.getPublishMessagesResult)
      case ResultProto.Type.CREATE_MESSAGES_STREAM       => fromProto(proto.getCreateMessagesStreamResult)
      case ResultProto.Type.CHANGE_MESSAGES_SUBSCRIPTION => fromProto(proto.getChangeMessagesSubscriptionResult)
      case ResultProto.Type.CLOSE_MESSAGES_STREAM        => fromProto(proto.getCloseMessagesStreamResult)
      case ResultProto.Type.MESSAGES_NOTIFICATION        => fromProto(proto.getMessagesNotificationResult)
      case ResultProto.Type.STREAMS_ACLS_COMMAND         => fromProto(proto.getStreamsAclsCommandResult)
    }
    if (proto.hasResultStats) {
      result match {
        case resWithStats: HasResultStats[_] =>
          result = resWithStats.withResultStats(Some(fromProto(proto.getResultStats)))
        case _ =>
      }
    }
    result
  } */

  /* private def reasonFromProto(proto: ResultProto.RedirectionReasonType): RedirectionReason = proto match {
    case ResultProto.RedirectionReasonType.UNKNOWN_REDIRECTION_REASON => RedirectionReason.Unknown
    case ResultProto.RedirectionReasonType.UNSUPPORTED_QUERY_TYPE     => RedirectionReason.QueryTypeNotSupported
    case ResultProto.RedirectionReasonType.NO_READ_THROUGH            => RedirectionReason.PrcReadThroughNotEnabled
    case ResultProto.RedirectionReasonType.KEY_SERDE_FAILURE          => RedirectionReason.PrcKeySerdeFailure
    case ResultProto.RedirectionReasonType.PAYLOAD_RETRIEVAL_FAILURE  => RedirectionReason.PayloadRetrievalFailure
    case ResultProto.RedirectionReasonType.UNSUPPORTED_COMMAND_TYPE   => RedirectionReason.CommandTypeNotSupported
    case ResultProto.RedirectionReasonType.UNSUPPORTED_TEMPORALITY_TYPE =>
      RedirectionReason.TemporalityTypeNotSupported
    case ResultProto.RedirectionReasonType.ENTITLEMENTS_REQUIRE_FURTHER_CHECKS =>
      RedirectionReason.EntitlementsRequireFurtherChecks
    case ResultProto.RedirectionReasonType.SESSION_LOADING_FAILURE => RedirectionReason.DhtSessionRetrievalFailure
    case ResultProto.RedirectionReasonType.READ_THROUGH_FAILURE    => RedirectionReason.PrcReadThroughFailed
    case ResultProto.RedirectionReasonType.PRC_NO_BROKERS_FAILURE  => RedirectionReason.PrcNoBrokersFailure
    case ResultProto.RedirectionReasonType.PRC_READ_THROUGH_TIMEOUT_FAILURE =>
      RedirectionReason.PrcReadThroughTimeoutFailure
  }

  private def reasonToProto(reason: RedirectionReason): ResultProto.RedirectionReasonType = reason match {
    case RedirectionReason.Unknown                  => ResultProto.RedirectionReasonType.UNKNOWN_REDIRECTION_REASON
    case RedirectionReason.QueryTypeNotSupported    => ResultProto.RedirectionReasonType.UNSUPPORTED_QUERY_TYPE
    case RedirectionReason.PrcReadThroughNotEnabled => ResultProto.RedirectionReasonType.NO_READ_THROUGH
    case RedirectionReason.PrcKeySerdeFailure       => ResultProto.RedirectionReasonType.KEY_SERDE_FAILURE
    case RedirectionReason.PayloadRetrievalFailure  => ResultProto.RedirectionReasonType.PAYLOAD_RETRIEVAL_FAILURE
    case RedirectionReason.CommandTypeNotSupported  => ResultProto.RedirectionReasonType.UNSUPPORTED_COMMAND_TYPE
    case RedirectionReason.TemporalityTypeNotSupported =>
      ResultProto.RedirectionReasonType.UNSUPPORTED_TEMPORALITY_TYPE
    case RedirectionReason.EntitlementsRequireFurtherChecks =>
      ResultProto.RedirectionReasonType.ENTITLEMENTS_REQUIRE_FURTHER_CHECKS
    case RedirectionReason.DhtSessionRetrievalFailure => ResultProto.RedirectionReasonType.SESSION_LOADING_FAILURE
    case RedirectionReason.PrcReadThroughFailed =>
      ResultProto.RedirectionReasonType.READ_THROUGH_FAILURE
    case RedirectionReason.PrcNoBrokersFailure => ResultProto.RedirectionReasonType.PRC_NO_BROKERS_FAILURE
    case RedirectionReason.PrcReadThroughTimeoutFailure =>
      ResultProto.RedirectionReasonType.PRC_READ_THROUGH_TIMEOUT_FAILURE
  } */
}
