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

import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.PartitionHelper
import optimus.dsi.pubsub.ExpressionQueryHolder
import optimus.dsi.pubsub.HeartbeatQueryHolder
import optimus.dsi.pubsub.QueryHolder
import optimus.dsi.pubsub.Subscription
import optimus.platform.dsi.bitemporal.PubSubRepLagOverlimitResult
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.expressions.proto.ExpressionSerializer

import scala.jdk.CollectionConverters._

trait PubSubProtoSerialization extends ProtoSerialization {

  implicit val createPubSubStreamSerializer: CreatePubSubStreamCommandSerializer.type =
    CreatePubSubStreamCommandSerializer
  implicit val changeSubscriptionSerializer: ChangeSubscriptionCommandSerializer.type =
    ChangeSubscriptionCommandSerializer
  implicit val closePubSubStreamSerializer: ClosePubSubStreamCommandSerializer.type = ClosePubSubStreamCommandSerializer

  implicit val createStreamResSerializer: CreatePubSubStreamResultSerializer.type = CreatePubSubStreamResultSerializer
  implicit val changeSubResSerializer: ChangeSubscriptionResultSerializer.type = ChangeSubscriptionResultSerializer
  implicit val sowResSerializer: PubSubSowResultSerializer.type = PubSubSowResultSerializer
  implicit val notificationResSerializer: PubSubNotificationResultSerializer.type = PubSubNotificationResultSerializer
  implicit val notificationPartialResSerializer: PubSubNotificationPartialResultSerializer.type =
    PubSubNotificationPartialResultSerializer
  implicit val notificationHeartbeatSerializer: PubSubHeartbeatResultSerializer.type = PubSubHeartbeatResultSerializer
  implicit val closeStreamResSerializer: ClosePubSubStreamResultSerializer.type = ClosePubSubStreamResultSerializer
  implicit val pubSubStreamEventSerializer: PubSubStreamEventSerializer.type = PubSubStreamEventSerializer
  implicit val pubSubGlobalEventSerializer: PubSubGlobalEventSerializer.type = PubSubGlobalEventSerializer
}

trait PubSubCommandProtoSerializationBase extends BasicProtoSerialization {
  implicit val subscriptionSerializer: SubscriptionSerializer.type = SubscriptionSerializer
  implicit val exprSerializer: ExpressionSerializer.type = ExpressionSerializer
  implicit val queryHolderSerializer: QueryHolderSerializer.type = QueryHolderSerializer
}

object CreatePubSubStreamCommandSerializer
    extends PubSubCommandProtoSerializationBase
    with ProtoSerializer[CreatePubSubStream, CreatePubSubStreamProto] {

  override def serialize(cmd: CreatePubSubStream): CreatePubSubStreamProto = ??? /* {
    val builder = CreatePubSubStreamProto.newBuilder
      .setStreamUuid(cmd.streamId)
      .addAllSubscriptions(cmd.subs.map(toProto(_)).asJava)
    cmd.startTime.foreach(st => builder.setStartTime(toProto(st)))
    cmd.endTime.foreach(et => builder.setEndTime(toProto(et)))
    cmd.vtFilterInterval.foreach(filter => builder.setVtFilterInterval(toProto(filter)))
    builder.build()
  } */

  override def deserialize(proto: CreatePubSubStreamProto): CreatePubSubStream = ??? /* {
    val streamId = proto.getStreamUuid
    val subscriptions = proto.getSubscriptionsList.asScala.map(fromProto(_))
    val startTime = if (proto.hasStartTime) Some(fromProto(proto.getStartTime)) else None
    val endTime = if (proto.hasEndTime) Some(fromProto(proto.getEndTime)) else None
    val vtFilterInterval = if (proto.hasVtFilterInterval) Some(fromProto(proto.getVtFilterInterval)) else None
    CreatePubSubStream(streamId, subscriptions, startTime, endTime, vtFilterInterval)
  } */
}

object ChangeSubscriptionCommandSerializer
    extends PubSubCommandProtoSerializationBase
    with ProtoSerializer[ChangeSubscription, ChangeSubscriptionProto] {

  override def serialize(cmd: ChangeSubscription): ChangeSubscriptionProto = ??? /* {
    ChangeSubscriptionProto.newBuilder
      .setStreamUuid(cmd.streamId)
      .setChangeRequestId(cmd.changeRequestId)
      .addAllAddSubscriptions(cmd.newSubs.map(toProto(_)).asJava)
      .addAllRemoveSubscriptionIds(cmd.removeSubs.map(Integer.valueOf).asJava)
      .build
  } */

  override def deserialize(proto: ChangeSubscriptionProto): ChangeSubscription = ??? /* {
    val streamId = proto.getStreamUuid
    val changeReqId = proto.getChangeRequestId
    val addSubs = proto.getAddSubscriptionsList.asScala.map(fromProto(_))
    val remSubs = proto.getRemoveSubscriptionIdsList.asScala.map(_.intValue).toSeq
    ChangeSubscription(streamId, changeReqId, addSubs, remSubs)
  } */
}

object ClosePubSubStreamCommandSerializer
    extends PubSubCommandProtoSerializationBase
    with ProtoSerializer[ClosePubSubStream, ClosePubSubStreamProto] {

  override def serialize(cmd: ClosePubSubStream): ClosePubSubStreamProto = ??? /* {
    ClosePubSubStreamProto.newBuilder
      .setStreamUuid(cmd.streamId)
      .build
  } */

  override def deserialize(proto: ClosePubSubStreamProto): ClosePubSubStream = ??? /* {
    ClosePubSubStream(proto.getStreamUuid)
  } */
}

object SubscriptionSerializer
    extends PubSubCommandProtoSerializationBase
    with ProtoSerializer[Subscription, SubscriptionProto] {

  override def serialize(sub: Subscription): SubscriptionProto = ??? /* {
    SubscriptionProto.newBuilder
      .setSubId(sub.subId)
      .setQuery(toProto(sub.query))
      .setIncludeSow(sub.includeSow)
      .setEntitledOnly(sub.entitledOnly)
      .build
  } */

  override def deserialize(proto: SubscriptionProto): Subscription = ??? /* {
    val subId = proto.getSubId
    val query: QueryHolder = fromProto(proto.getQuery)
    val includeSow = proto.getIncludeSow
    val entitledOnly = if (proto.hasEntitledOnly) proto.getEntitledOnly else false
    Subscription(subId, query, includeSow, entitledOnly)
  } */
}

object QueryHolderSerializer
    extends PubSubCommandProtoSerializationBase
    with ProtoSerializer[QueryHolder, QueryHolderProto] {

  override def serialize(obj: QueryHolder): QueryHolderProto = ??? /* {
    val builder = QueryHolderProto.newBuilder
    obj match {
      case ExpressionQueryHolder(query, clientFilter) =>
        // clientFilter should not be serialized; it is a client only field
        builder.setType(QueryHolderProto.Type.EXPRESSION_QUERY).setQuery(toProto(query))
      case HeartbeatQueryHolder(partition, interval) =>
        builder.setType(QueryHolderProto.Type.HEARTBEAT_QUERY)
        builder.setPartition(partition.name)
        builder.setHeartbeatInterval(interval)
    }
    builder.build
  } */

  override def deserialize(proto: QueryHolderProto): QueryHolder = ??? /* {
    proto.getType match {
      case QueryHolderProto.Type.EXPRESSION_QUERY => ExpressionQueryHolder(fromProto(proto.getQuery), None)
      case QueryHolderProto.Type.HEARTBEAT_QUERY =>
        val partition =
          if (proto.hasPartition) PartitionHelper.getPartitionForString(proto.getPartition) else DefaultPartition
        if (proto.hasHeartbeatInterval)
          HeartbeatQueryHolder(partition, proto.getHeartbeatInterval)
        else HeartbeatQueryHolder(partition)
    }
  } */
}

trait NotificationEntrySerialization extends BasicProtoSerialization {
  implicit val notificationEntrySerializer: NotificationEntrySerializer.type = NotificationEntrySerializer
}

trait PubSubResultProtoSerializationBase extends NotificationEntrySerialization {
  implicit val queryResultSerializer: QueryResultSerializer.type = QueryResultSerializer
  implicit val partialqueryResultSerializer: PartialQueryResultSerializer.type = PartialQueryResultSerializer
}

object CreatePubSubStreamResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[CreatePubSubStreamSuccessResult, CreatePubSubStreamSuccessProto] {

  override def serialize(res: CreatePubSubStreamSuccessResult): CreatePubSubStreamSuccessProto = ??? /* {
    CreatePubSubStreamSuccessProto.newBuilder
      .setStreamUuid(res.streamId)
      .setTxTime(toProto(res.txTime))
      .build
  } */
  override def deserialize(proto: CreatePubSubStreamSuccessProto): CreatePubSubStreamSuccessResult = ??? /* {
    CreatePubSubStreamSuccessResult(proto.getStreamUuid, fromProto(proto.getTxTime))
  } */
}

object ChangeSubscriptionResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[ChangeSubscriptionSuccessResult, ChangeSubscriptionSuccessProto] {

  override def serialize(res: ChangeSubscriptionSuccessResult): ChangeSubscriptionSuccessProto = ??? /* {
    ChangeSubscriptionSuccessProto.newBuilder
      .setStreamUuid(res.streamId)
      .setChangeRequestId(res.changeRequestId)
      .setTxTime(toProto(res.txTime))
      .build
  } */

  override def deserialize(proto: ChangeSubscriptionSuccessProto): ChangeSubscriptionSuccessResult = ??? /* {
    val streamId = proto.getStreamUuid
    val changeRequestId = proto.getChangeRequestId
    ChangeSubscriptionSuccessResult(streamId, changeRequestId, fromProto(proto.getTxTime))
  } */
}

object ClosePubSubStreamResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[ClosePubSubStreamSuccessResult, ClosePubSubStreamSuccessProto] {

  override def serialize(res: ClosePubSubStreamSuccessResult): ClosePubSubStreamSuccessProto = ??? /* {
    ClosePubSubStreamSuccessProto.newBuilder
      .setStreamUuid(res.streamId)
      .build
  } */
  override def deserialize(proto: ClosePubSubStreamSuccessProto): ClosePubSubStreamSuccessResult = ??? /* {
    ClosePubSubStreamSuccessResult(proto.getStreamUuid)
  } */
}

object PubSubSowResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubSowResult, PubSubSowResultProto] {

  override def serialize(res: PubSubSowResult): PubSubSowResultProto = ??? /* {
    val builder = PubSubSowResultProto.newBuilder
      .setStreamUuid(res.streamId)
      .setSubId(res.subId)
      .setTxTime(toProto(res.txTime))
    res match {
      case r: PubSubSowFullResult =>
        builder
          .setType(PubSubSowResultProto.Type.QUERY_RESULT)
          .setQueryResult(toProto(r.result))
      case r: PubSubSowPartialResult =>
        builder
          .setType(PubSubSowResultProto.Type.PARTIAL_QUERY_RESULT)
          .setPartialQueryResult(toProto(r.parRes))
    }
    builder.build
  } */

  override def deserialize(proto: PubSubSowResultProto): PubSubSowResult = ??? /* {
    val streamId = proto.getStreamUuid
    val subId = proto.getSubId
    val txTime = fromProto(proto.getTxTime)
    proto.getType match {
      case PubSubSowResultProto.Type.QUERY_RESULT =>
        val queryResult = fromProto(proto.getQueryResult)
        PubSubSowFullResult(streamId, subId, txTime, queryResult)
      case PubSubSowResultProto.Type.PARTIAL_QUERY_RESULT =>
        val parQueryResult = fromProto(proto.getPartialQueryResult)
        PubSubSowPartialResult(streamId, subId, txTime, parQueryResult)
    }
  } */
}

object PubSubNotificationResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubNotificationResult, PubSubNotificationProto] {

  override def serialize(res: PubSubNotificationResult): PubSubNotificationProto = ??? /* {
    val builder = PubSubNotificationProto.newBuilder
      .setStreamUuid(res.streamId)
      .setTxTime(toProto(res.txTime))
    res.writeReqId.foreach(builder.setWriteRequestId)
    res match {
      case cres: DirectPubSubNotificationResult =>
        val notificationEntryProtos = cres.entryMap.map { case (subId, entries) =>
          PubSubNotificationForSubscriptionProto.newBuilder
            .setSubId(subId)
            .addAllNotificationEntries(entries.map(toProto(_)).asJava)
            .build
        }
        builder.addAllNotifications(notificationEntryProtos.asJava)
      case rres: ReferentialPubSubNotificationResult =>
        val notifications = rres.entries
          .groupBy(_.subId)
          .toSeq
          .map({ case (subId, refs) =>
            val references = refs.map(ref =>
              PubSubNotificationForSubscriptionProto.EntryReference
                .newBuilder()
                .setIndex(ref.index)
                .build)

            PubSubNotificationForSubscriptionProto.newBuilder
              .setSubId(subId)
              .addAllNotificationEntryReferences(references.asJava)
              .build
          })
        builder
          .addAllNotifications(notifications.asJava)
          .setNumberOfOutOfLineNotificationEntries(rres.numberOfOutOfLineEntries)
    }
    builder.build
  } */

  override def deserialize(proto: PubSubNotificationProto): PubSubNotificationResult = ??? /* {
    val streamId = proto.getStreamUuid
    val txTime = fromProto(proto.getTxTime)
    val writeReqId = if (proto.hasWriteRequestId) Some(proto.getWriteRequestId) else None
    if (proto.getNumberOfOutOfLineNotificationEntries == 0) {
      val entries = proto.getNotificationsList.asScala.flatMap { subproto =>
        val subId = subproto.getSubId
        val nes = subproto.getNotificationEntriesList.asScala.map(fromProto(_))
        nes.map(ne => PubSubNotificationResult.SimpleEntry(subId, ne))
      }
      DirectPubSubNotificationResult(streamId, writeReqId, txTime, entries)
    } else {
      val entries = proto.getNotificationsList.asScala.flatMap { subproto =>
        val subId = subproto.getSubId
        subproto.getNotificationEntryReferencesList.asScala.map(r =>
          PubSubNotificationResult.EntryReference(subId, r.getIndex))
      }
      ReferentialPubSubNotificationResult(
        streamId,
        writeReqId,
        txTime,
        entries,
        proto.getNumberOfOutOfLineNotificationEntries)
    }
  } */
}

object PubSubNotificationPartialResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubNotificationPartialResult, PubSubNotificationPartialResultProto] {

  override def serialize(res: PubSubNotificationPartialResult): PubSubNotificationPartialResultProto = ??? /* {
    val builder = PubSubNotificationPartialResultProto.newBuilder
      .setStreamUuid(res.streamId)
      .setTxTime(toProto(res.txTime))
      .setIsLast(res.isLast)
    res.writeReqId.foreach(builder.setWriteRequestId)
    res match {
      case cres: DirectPubSubNotificationPartialResult =>
        val notificationEntryProtos = cres.entryMap.map { case (subId, entries) =>
          PubSubNotificationForSubscriptionProto.newBuilder
            .setSubId(subId)
            .addAllNotificationEntries(entries.map(toProto(_)).asJava)
            .build
        }
        builder.addAllNotifications(notificationEntryProtos.asJava)
      case rres: ReferentialPubSubNotificationPartialResult =>
        val notifications = rres.entries
          .groupBy(_.subId)
          .toSeq
          .map({ case (subId, refs) =>
            val references = refs.map(ref =>
              PubSubNotificationForSubscriptionProto.EntryReference
                .newBuilder()
                .setIndex(ref.index)
                .build)

            PubSubNotificationForSubscriptionProto.newBuilder
              .setSubId(subId)
              .addAllNotificationEntryReferences(references.asJava)
              .build
          })
        builder
          .addAllNotifications(notifications.asJava)
          .setNumberOfOutOfLineNotificationEntries(rres.numberOfOutOfLineEntries)
    }
    builder.build
  } */

  override def deserialize(proto: PubSubNotificationPartialResultProto): PubSubNotificationPartialResult = ??? /* {
    val streamId = proto.getStreamUuid
    val isLast = proto.getIsLast
    val txTime = fromProto(proto.getTxTime)
    val writeReqId = if (proto.hasWriteRequestId) Some(proto.getWriteRequestId) else None
    if (proto.getNumberOfOutOfLineNotificationEntries == 0) {
      val entries = proto.getNotificationsList.asScala.flatMap { subproto =>
        val subId = subproto.getSubId
        val nes = subproto.getNotificationEntriesList.asScala.map(fromProto(_))
        nes.map(ne => PubSubNotificationResult.SimpleEntry(subId, ne))
      }
      DirectPubSubNotificationPartialResult(streamId, writeReqId, txTime, entries, isLast)
    } else {
      val entries = proto.getNotificationsList.asScala.flatMap { subproto =>
        val subId = subproto.getSubId
        subproto.getNotificationEntryReferencesList.asScala.map(r =>
          PubSubNotificationResult.EntryReference(subId, r.getIndex))
      }
      ReferentialPubSubNotificationPartialResult(
        streamId,
        writeReqId,
        txTime,
        entries,
        isLast,
        proto.getNumberOfOutOfLineNotificationEntries)
    }
  } */
}

object PubSubHeartbeatResultSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubHeartbeatResult, PubSubHeartbeatProto] {

  override def serialize(res: PubSubHeartbeatResult): PubSubHeartbeatProto = ??? /* {
    val builder = PubSubHeartbeatProto.newBuilder
      .setStreamUuid(res.streamId)
      .setTxTime(toProto(res.txTime))
    builder.build
  } */

  override def deserialize(proto: PubSubHeartbeatProto): PubSubHeartbeatResult = ??? /* {
    val streamId = proto.getStreamUuid
    val txTime = fromProto(proto.getTxTime)
    PubSubHeartbeatResult(streamId, txTime)
  } */
}

object PubSubPartitionToSubIdsMapSerializer extends ProtoSerializer[Map[String, Seq[Int]], PartitionToSubIdsMapProto] {

  override def serialize(partitionMap: Map[String, Seq[Int]]): PartitionToSubIdsMapProto = ??? /* {
    val builder = PartitionToSubIdsMapProto.newBuilder
    val entries = partitionMap.map { case (par, subIds) =>
      val entryBuilder = PartitionToSubIdsEntryProto.newBuilder
      entryBuilder.setPartition(par)
      subIds.foreach(entryBuilder.addSubIds)
      entryBuilder.build
    }
    entries.foreach(builder.addEntries)
    builder.build
  } */

  override def deserialize(proto: PartitionToSubIdsMapProto): Map[String, Seq[Int]] = ??? /* {
    proto.getEntriesList.asScala.map { entry =>
      val partition = entry.getPartition
      val subIds = entry.getSubIdsList.asScala.map(_.toInt).toList
      partition -> subIds
    } toMap
  } */
}

object PubSubStreamEventSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubStreamResult, PubSubStreamEventProto] {
  override def deserialize(proto: PubSubStreamEventProto): PubSubStreamResult = ??? /* {
    proto.getType match {
      case PubSubStreamEventProto.Type.PUBSUB_SOW_START =>
        PubSubSowStartResult(proto.getStreamUuid, fromProto(proto.getTxTime))
      case PubSubStreamEventProto.Type.PUBSUB_SOW_END =>
        PubSubSowEndResult(proto.getStreamUuid, fromProto(proto.getTxTime))
      case PubSubStreamEventProto.Type.CATCHUP_START =>
        PubSubCatchupStartResult(proto.getStreamUuid, fromProto(proto.getTxTime))
      case PubSubStreamEventProto.Type.CATCHUP_COMPLETE =>
        PubSubCatchupCompleteResult(proto.getStreamUuid, fromProto(proto.getTxTime))
    }
  } */
  override def serialize(res: PubSubStreamResult): PubSubStreamEventProto = ??? /* {
    res match {
      case r: PubSubSowStartResult =>
        PubSubStreamEventProto.newBuilder
          .setType(PubSubStreamEventProto.Type.PUBSUB_SOW_START)
          .setStreamUuid(r.streamId)
          .setTxTime(toProto(r.txTime))
          .build
      case r: PubSubSowEndResult =>
        PubSubStreamEventProto.newBuilder
          .setType(PubSubStreamEventProto.Type.PUBSUB_SOW_END)
          .setStreamUuid(r.streamId)
          .setTxTime(toProto(r.txTime))
          .build
      case r: PubSubCatchupStartResult =>
        PubSubStreamEventProto.newBuilder
          .setType(PubSubStreamEventProto.Type.CATCHUP_START)
          .setStreamUuid(r.streamId)
          .setTxTime(toProto(r.txTime))
          .build
      case r: PubSubCatchupCompleteResult =>
        PubSubStreamEventProto.newBuilder
          .setType(PubSubStreamEventProto.Type.CATCHUP_COMPLETE)
          .setStreamUuid(r.streamId)
          .setTxTime(toProto(r.txTime))
          .build
    }
  } */
}

object PubSubGlobalEventSerializer
    extends PubSubResultProtoSerializationBase
    with ProtoSerializer[PubSubGlobalResult, PubSubGlobalEventProto] {
  override def deserialize(proto: PubSubGlobalEventProto): PubSubGlobalResult = ??? /* {
    proto.getType match {
      case PubSubGlobalEventProto.Type.REPLAG_UNDERLIMIT_RESULT =>
        PubSubRepLagUnderlimitResult(proto.getStreamUuid, proto.getLagTime)
      case PubSubGlobalEventProto.Type.REPLAG_OVERLIMIT_RESULT =>
        PubSubRepLagOverlimitResult(proto.getStreamUuid, proto.getLagTime)
      case PubSubGlobalEventProto.Type.UPSTREAM_CHANGED_RESULT => PubSubUpstreamChangedResult(proto.getStreamUuid)
    }
  } */
  override def serialize(res: PubSubGlobalResult): PubSubGlobalEventProto = ??? /* {
    res match {
      case r: PubSubRepLagUnderlimitResult =>
        PubSubGlobalEventProto.newBuilder
          .setType(PubSubGlobalEventProto.Type.REPLAG_UNDERLIMIT_RESULT)
          .setStreamUuid(r.streamId)
          .setLagTime(r.lagTime)
          .build
      case r: PubSubRepLagOverlimitResult =>
        PubSubGlobalEventProto.newBuilder
          .setType(PubSubGlobalEventProto.Type.REPLAG_OVERLIMIT_RESULT)
          .setStreamUuid(r.streamId)
          .setLagTime(r.lagTime)
          .build
      case r: PubSubUpstreamChangedResult =>
        PubSubGlobalEventProto.newBuilder
          .setType(PubSubGlobalEventProto.Type.UPSTREAM_CHANGED_RESULT)
          .setStreamUuid(r.streamId)
          .build
      case cn: PubSubBrokerConnect    => throw new IllegalArgumentException(s"$cn not expected in request or response")
      case dn: PubSubBrokerDisconnect => throw new IllegalArgumentException(s"$dn not expected in request or response")
    }
  } */
}
