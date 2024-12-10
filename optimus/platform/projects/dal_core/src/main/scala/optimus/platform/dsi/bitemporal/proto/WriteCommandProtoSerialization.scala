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

import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform._
import optimus.platform.storable._
import scala.jdk.CollectionConverters._
import java.lang.Boolean

private[proto] trait WriteCommandProtoSerialization extends BasicProtoSerialization {
  implicit val resolveKeysSerializer: ResolveKeysSerializer.type = ResolveKeysSerializer
  implicit val resolveKeysResultSerializer: ResolveKeysResultSerializer.type = ResolveKeysResultSerializer
  implicit val assertValidSerializer: AssertValidSerializer.type = AssertValidSerializer
  implicit val assertValidResultSerializer: AssertValidResultSerializer.type = AssertValidResultSerializer

  implicit val putSerializer: PutSerializer.type = PutSerializer
  implicit val putResultSerializer: PutResultSerializer.type = PutResultSerializer
  implicit val putApplicationEventSerializer: PutApplicationEventSerializer.type = PutApplicationEventSerializer
  implicit val putApplicationEventResultSerializer: PutApplicationEventResultSerializer.type =
    PutApplicationEventResultSerializer

  implicit val writeBusinessEventPutSerializer: WriteBusinessEventPutSerializer.type = WriteBusinessEventPutSerializer
  implicit val writeBusinessEventPutSlotsSerializer: WriteBusinessEventPutSlotsSerializer.type =
    WriteBusinessEventPutSlotsSerializer
  implicit val entityReferenceTupleSerializer: EntityReferenceTupleSerializer.type = EntityReferenceTupleSerializer
  implicit val writeBusinessEventSerializer: WriteBusinessEventSerializer.type = WriteBusinessEventSerializer
  implicit val writeBusinessEventResultSerializer: WriteBusinessEventResultSerializer.type =
    WriteBusinessEventResultSerializer

  implicit val invalidateAfterSerializer: InvalidateAfterSerializer.type = InvalidateAfterSerializer
  implicit val invalidateAfterResultSerializer: InvalidateAfterResultSerializer.type = InvalidateAfterResultSerializer
  implicit val invalidateAllCurrentSerializer: InvalidateAllCurrentSerializer.type = InvalidateAllCurrentSerializer
  implicit val invalidateAllCurrentResultSerializer: InvalidateAllCurrentResultSerializer.type =
    InvalidateAllCurrentResultSerializer
  implicit val invalidateAllCurrentByRefsSerializer: InvalidateAllCurrentByRefsSerializer.type =
    InvalidateAllCurrentByRefsSerializer

  implicit val createSlotsSerializer: CreateSlotsSerializer.type = CreateSlotsSerializer
  implicit val createSlotsResultSerializer: CreateSlotsResultSerializer.type = CreateSlotsResultSerializer
  implicit val fillSlotSerializer: FillSlotSerializer.type = FillSlotSerializer

  implicit val accTableCreateCommandSerializer: AccMetadataCommandSerializer.type = AccMetadataCommandSerializer
  implicit val accTableResultSerializer: AccTableResultSerializer.type = AccTableResultSerializer

  final def toProto(flag: EventStateFlag): String = {
    flag.char.toString
  }

  final def fromProto(proto: String): EventStateFlag = {
    if (proto.charAt(0) == 'i') {
      throw new DSISpecificError("Event reinstate API was decommissioned. Please update your client.")
    }

    EventStateFlag.fromChar(proto.charAt(0))
  }
}

object AssertValidSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[AssertValid, AssertValidProto] {

  override def serialize(av: AssertValid): AssertValidProto = ??? /* {
    val builder = AssertValidProto.newBuilder
    av.refs foreach { e =>
      builder.addReferences(toProto(e))
    }
    builder.setValidTime(toProto(av.validTime))
    builder.build
  } */

  override def deserialize(proto: AssertValidProto): AssertValid = ??? /* {
    val references = proto.getReferencesList.asScala.map {
      fromProto(_)
    }
    AssertValid(references.toSeq, fromProto(proto.getValidTime))
  } */
}

object AssertValidResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[AssertValidResult, AssertValidResultProto] {

  override def deserialize(proto: AssertValidResultProto): AssertValidResult = ??? /* {
    AssertValidResult()
  } */

  override def serialize(result: AssertValidResult): AssertValidResultProto = ??? /* {
    val builder = AssertValidResultProto.newBuilder
    builder.build
  } */
}

object PutSerializer extends WriteCommandProtoSerialization with ProtoSerializer[Put, PutProto] {

  override def serialize(put: Put): PutProto = ??? /* {
    val builder = PutProto.newBuilder
      .setAfterTxTime(toProto(TimeInterval.NegInfinity))
      .setValidTime(toProto(put.validTime))
      .setSerializedEntity(toProto(put.value))

    if (put.lockToken.isDefined)
      builder.setLockToken(put.lockToken.get)

    builder.build
  } */

  override def deserialize(proto: PutProto): Put = ??? /* {
    val lockToken =
      if (proto.hasLockToken)
        Some(proto.getLockToken)
      else None
    Put(fromProto(proto.getSerializedEntity), lockToken, fromProto(proto.getValidTime))
  } */
}

object PutResultSerializer extends WriteCommandProtoSerialization with ProtoSerializer[PutResult, PutResultProto] {

  override def deserialize(proto: PutResultProto): PutResult = ??? /* {
    val txTimeTo = if (proto.hasTxTimeTo) Some(fromProto(proto.getTxTimeTo)) else None
    PutResult(
      fromProto(proto.getPermReference),
      fromProto(proto.getVersionedReference),
      proto.getLockToken,
      fromProto(proto.getValidTimeInterval),
      txTimeTo,
      fromProto(proto.getTxTime)
    )
  } */

  override def serialize(result: PutResult): PutResultProto = ??? /* {
    val builder = PutResultProto.newBuilder
      .setPermReference(toProto(result.permRef))
      .setVersionedReference(toProto(result.versionedRef))
      .setLockToken(result.lockToken)
      .setValidTimeInterval(toProto(result.vtInterval))
      .setTxTime(toProto(result.txTime))

    if (result.txTimeTo.isDefined) builder.setTxTimeTo(toProto(result.txTimeTo.get))

    builder.build
  } */
}

object AccMetadataCommandSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[AccMetadataCommand, AccMetadataCommandProto] {

  override def serialize(acc: AccMetadataCommand): AccMetadataCommandProto = ??? /* {
    val bld =
      AccMetadataCommandProto.newBuilder.setClsName(acc.clsName).setSlot(acc.slot).setAction(acc.action.toString)
    acc.accTtOpt.foreach(accTt => bld.setAccTxTime(toProto(accTt)))
    acc.accInfoOpt.foreach(info => bld.setAccInfo(toProto(info)))
    bld.setForceCreateIndex(acc.forceCreateIndex)
    bld.build
  } */

  override def deserialize(proto: AccMetadataCommandProto): AccMetadataCommand = ??? /* {
    val accTtOpt = if (proto.hasAccTxTime) Some(fromProto(proto.getAccTxTime)) else None
    val accInfoOpt = if (proto.hasAccInfo) Some(fromProto(proto.getAccInfo)) else None
    val forceCreateIndex = if (proto.hasForceCreateIndex) proto.getForceCreateIndex else false
    AccMetadataCommand(
      proto.getClsName,
      proto.getSlot,
      AccMetadataOp.withName(proto.getAction),
      accTtOpt,
      accInfoOpt,
      forceCreateIndex)
  } */
}

object AccTableResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[AccTableResult, AccTableResultProto] {

  override def deserialize(proto: AccTableResultProto): AccTableResult = ??? /* {
    AccTableResult(proto.getClsName, proto.getSlot)
  } */

  override def serialize(result: AccTableResult): AccTableResultProto = ??? /* {
    AccTableResultProto.newBuilder
      .setClsName(result.clsName)
      .setSlot(result.slot)
      .build
  } */
}

object ResolveKeysSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[ResolveKeys, ResolveKeysProto] {

  override def serialize(resolve: ResolveKeys): ResolveKeysProto = ??? /* {
    val builder = ResolveKeysProto.newBuilder
      .addAllSerializedKey(resolve.keys.map {
        toProto(_)
      }.asJava)
    builder.build
  } */

  override def deserialize(proto: ResolveKeysProto): ResolveKeys = ??? /* {
    val keys: Seq[SerializedKey] = proto.getSerializedKeyList.asScala.iterator.map {
      fromProto(_)
    }.toIndexedSeq
    new ResolveKeys(keys)
  } */
}

object ResolveKeysResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[ResolveKeysResult, ResolveKeysResultProto] {

  override def serialize(resolve: ResolveKeysResult): ResolveKeysResultProto = ??? /* {
    val found = resolve.refs.map { r =>
      Boolean.valueOf(r.isDefined)
    }
    val entities = resolve.refs flatMap {
      _ map {
        toProto(_)
      }
    }
    val builder = ResolveKeysResultProto.newBuilder
      .addAllFound(found.asJava)
      .addAllEntity(entities.asJava)
    builder.build
  } */

  override def deserialize(proto: ResolveKeysResultProto): ResolveKeysResult = ??? /* {
    val entities = proto.getEntityList
    val foundList = proto.getFoundList.asScala
    var x = 0
    val temp = for {
      found <- foundList
    } yield {
      if (found) {
        val e = entities.get(x)
        x += 1
        Some(fromProto(e))
      } else {
        None
      }
    }
    new ResolveKeysResult(temp.toSeq)
  } */
}

object WriteBusinessEventSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[WriteBusinessEvent, WriteBusinessEventProto] {

  override def serialize(writeEvent: WriteBusinessEvent): WriteBusinessEventProto = ??? /* {
    val builder = WriteBusinessEventProto.newBuilder
      .setSerializedEvent(toProto(writeEvent.evt))
      .setState(toProto(writeEvent.state))
    if (writeEvent.asserts.nonEmpty) builder.addAllAsserts(writeEvent.asserts.map {
      toProto(_)
    }.asJava)
    if (writeEvent.puts.nonEmpty) builder.addAllPuts(writeEvent.puts.map {
      toProto(_)
    }.asJava)
    if (writeEvent.putSlots.nonEmpty) builder.addAllPutSlots(writeEvent.putSlots.map {
      toProto(_)
    }.asJava)
    if (writeEvent.invalidates.nonEmpty)
      builder.addAllInvalidates(
        writeEvent.invalidates
          .map(i => (i.er, i.lt))
          .map {
            toProto(_)
          }
          .asJava)
    if (writeEvent.reverts.nonEmpty)
      builder.addAllReverts(
        writeEvent.reverts
          .map(i => (i.er, i.lt))
          .map {
            toProto(_)
          }
          .asJava)
    builder.build
  } */

  override def deserialize(proto: WriteBusinessEventProto): WriteBusinessEvent = ??? /* {

    val asserts = proto.getAssertsList().asScala map {
      fromProto(_)
    }
    val puts = if (proto.getPutsCount() > 0) proto.getPutsList().asScala map {
      fromProto(_)
    } toSeq
    else Nil
    val putSlots = if (proto.getPutSlotsCount() > 0) proto.getPutSlotsList.asScala map {
      fromProto(_)
    } toSeq
    else Nil
    val invalidates =
      if (proto.getInvalidatesCount() > 0)
        proto
          .getInvalidatesList()
          .asScala
          .map {
            fromProto(_)
          } toSeq
      else Seq[(EntityReference, Long)]()
    val reverts = if (proto.getRevertsCount() > 0) proto.getRevertsList.asScala.map {
      fromProto(_)
    } toSeq
    else Seq[(EntityReference, Long)]()
    WriteBusinessEvent(
      fromProto(proto.getSerializedEvent()),
      fromProto(proto.getState()),
      asserts,
      puts,
      putSlots,
      invalidates.map(i => InvalidateRevertProps(i._1, i._2, "")),
      reverts.map(i => InvalidateRevertProps(i._1, i._2, ""))
    )
  } */
}

object WriteBusinessEventPutSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[WriteBusinessEvent.Put, SerializedEntityTupleProto] {

  override def serialize(wbePut: WriteBusinessEvent.Put): SerializedEntityTupleProto = ??? /* {
    val builder = SerializedEntityTupleProto.newBuilder
      .setSerEntity(toProto(wbePut.ent))
    if (wbePut.lockToken.isDefined) builder.setLockToken(wbePut.lockToken.get)
    // if (wbePut.verRef.isDefined) builder.setVref(toProto(wbePut.verRef.get))
    builder.build
  } */

  override def deserialize(proto: SerializedEntityTupleProto): WriteBusinessEvent.Put = ??? /* {
    val lockToken: Option[Long] = if (proto.hasLockToken()) Some(proto.getLockToken()) else None
    val vref: Option[VersionedReference] = if (proto.hasVref) Some(fromProto(proto.getVref)) else None
    val ser = fromProto(proto.getSerEntity())
    WriteBusinessEvent.Put(ser, lockToken)
  } */
}

object WriteBusinessEventPutSlotsSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[WriteBusinessEvent.PutSlots, PutSlotsProto] {
  override def serialize(putSlots: WriteBusinessEvent.PutSlots): PutSlotsProto = ??? /* {
    val builder = PutSlotsProto.newBuilder
    putSlots.lockToken foreach builder.setLockToken
    builder.setOnlyLinkagesChanged(false)
    builder.addAllSerializedEntities((putSlots.ents.entities: Set[SerializedEntity]).map(e => toProto(e)).asJava)
    builder.build
  } */

  override def deserialize(proto: PutSlotsProto): WriteBusinessEvent.PutSlots = ??? /* {
    val lockToken = if (proto.hasLockToken) Some(proto.getLockToken) else None
    val serializedEntities = proto.getSerializedEntitiesList.asScala.map {
      fromProto(_)
    }.toSeq
    WriteBusinessEvent.PutSlots(MultiSlotSerializedEntity(serializedEntities), lockToken)
  } */
}

object EntityReferenceTupleSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[Tuple2[EntityReference, Long], EntityReferenceTupleProto] {

  override def serialize(entityRefTuple: (EntityReference, Long)): EntityReferenceTupleProto = ??? /* {
    EntityReferenceTupleProto.newBuilder
      .setEntityRef(toProto(entityRefTuple._1))
      .setLockToken(entityRefTuple._2)
      .build
  } */

  override def deserialize(proto: EntityReferenceTupleProto): (EntityReference, Long) = ??? /* {
    (fromProto(proto.getEntityRef()), proto.getLockToken())
  } */
}

object WriteBusinessEventResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[WriteBusinessEventResult, WriteBusinessEventResultProto] {

  override def deserialize(proto: WriteBusinessEventResultProto): WriteBusinessEventResult = ??? /* {
    WriteBusinessEventResult(
      proto.getPutResultsList().asScala map {
        fromProto(_)
      } toSeq,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      fromProto(proto.getTxTime()))
  } */

  override def serialize(result: WriteBusinessEventResult): WriteBusinessEventResultProto = ??? /* {
    val builder = WriteBusinessEventResultProto.newBuilder
      .addAllPutResults(result.putResults.map {
        toProto(_)
      }.asJava)
      .setTxTime(toProto(result.txTime))
    builder.build
  } */
}

object PutApplicationEventResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[PutApplicationEventResult, PutApplicationEventResultProto] {

  override def serialize(in: PutApplicationEventResult): PutApplicationEventResultProto = ??? /* {

    val builder = PutApplicationEventResultProto.newBuilder
      .setAppEvent(toProto(in.appEvent))
      .addAllBusinessEventResults((in.beResults.map {
        toProto(_)
      }).asJava)
      .setTxTime(toProto(in.appEvent.tt))
    builder.build
  } */

  override def deserialize(proto: PutApplicationEventResultProto): PutApplicationEventResult = ??? /* {
    PutApplicationEventResult(
      fromProto(proto.getAppEvent),
      proto.getBusinessEventResultsList().asScala.iterator.map(fromProto(_)).toIndexedSeq)
  } */
}

object PutApplicationEventSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[PutApplicationEvent, PutApplicationEventProto] {

  override def serialize(
      in: PutApplicationEvent
  ): PutApplicationEventProto = ??? /* {
    val builder = PutApplicationEventProto.newBuilder
      .addAllBusinessEvents(in.bes.map(toProto(_)).asJava)
      .setApplication(in.application)
      .setContentOwner(in.contentOwner)

    in.clientTxTime.foreach(clientTxTime => builder.setClientTxTime(toProto(clientTxTime)))
    in.elevatedForUser.foreach(onBehalfOf => builder.setElevatedForUser(onBehalfOf))
    in.appEvtId.foreach { appEvtId =>
      builder.setAppEventRef(toProto(appEvtId))
      builder.setRetry(in.retry)
    }
    builder.build
  } */

  override def deserialize(
      proto: PutApplicationEventProto
  ): PutApplicationEvent = ??? /* {
    import scala.jdk.CollectionConverters._

    val clientTx = if (proto.hasClientTxTime) Some(fromProto(proto.getClientTxTime)) else None
    val elevatedForUser = if (proto.hasElevatedForUser) Some(proto.getElevatedForUser) else None
    val appEvtId = if (proto.hasAppEventRef) Some(fromProto(proto.getAppEventRef)) else None
    val retry = if (proto.hasRetry) proto.getRetry else false

    PutApplicationEvent(
      bes = proto.getBusinessEventsList().asScala.iterator.map(fromProto(_)).toIndexedSeq,
      application = proto.getApplication,
      contentOwner = proto.getContentOwner,
      clientTxTime = clientTx,
      elevatedForUser = elevatedForUser,
      appEvtId = appEvtId,
      retry = retry
    )
  } */
}

object InvalidateAfterSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[InvalidateAfter, InvalidateAfterProto] {

  override def deserialize(proto: InvalidateAfterProto): InvalidateAfter = ??? /* {
    val lt = if (proto.hasLockToken) proto.getLockToken else 0L
    val eref = if (proto.hasEntityReference) fromProto(proto.getEntityReference) else null
    InvalidateAfter(eref, fromProto(proto.getVersionedReference), lt, fromProto(proto.getValidTime), "")
  } */

  override def serialize(invalidate: InvalidateAfter): InvalidateAfterProto = ??? /* {
    val builder = InvalidateAfterProto.newBuilder
    if (invalidate.entityRef != null) builder.setEntityReference(toProto(invalidate.entityRef))

    builder
      .setVersionedReference(toProto(invalidate.versionedRef))
      .setAfterTxTime(toProto(TimeInterval.NegInfinity))
      .setValidTime(toProto(invalidate.validTime))
      .setLockToken(invalidate.lockToken)
      .build
  } */
}

object InvalidateAfterResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[InvalidateAfterResult, InvalidateAfterResultProto] {

  override def deserialize(proto: InvalidateAfterResultProto): InvalidateAfterResult = ??? /* {
    import scala.jdk.CollectionConverters._

    val types = proto.getTypesList.asScala.toSeq
    val keys = proto.getKeysList.asScala map {
      fromProto(_)
    } toSeq

    InvalidateAfterResult(fromProto(proto.getEntityReference), types, keys, fromProto(proto.getTxTime))
  } */

  override def serialize(invalidate: InvalidateAfterResult): InvalidateAfterResultProto = ??? /* {
    val builder = InvalidateAfterResultProto.newBuilder
      .setVersionedReference(toProto(VersionedReference.Nil))
      .setTxTime(toProto(invalidate.txTime))
      .addAllTypes(invalidate.types.asJava)
      .addAllKeys(invalidate.keys.map {
        toProto(_)
      }.asJava)
      .setEntityReference(toProto(invalidate.permRef))

    builder.build
  } */
}

object InvalidateAllCurrentSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[InvalidateAllCurrent, InvalidateAllCurrentProto] {

  override def deserialize(proto: InvalidateAllCurrentProto): InvalidateAllCurrent = ??? /* {
    InvalidateAllCurrent(proto.getClassName, if (proto.hasCount) Some(proto.getCount) else None)
  } */

  override def serialize(invalidate: InvalidateAllCurrent): InvalidateAllCurrentProto = ??? /* {
    val builder = InvalidateAllCurrentProto.newBuilder
      .setClassName(invalidate.clazzName)
    invalidate.count.foreach(c => builder.setCount(c))
    builder.build
  } */
}

object InvalidateAllCurrentResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[InvalidateAllCurrentResult, InvalidateAllCurrentResultProto] {

  override def deserialize(proto: InvalidateAllCurrentResultProto): InvalidateAllCurrentResult = ??? /* {
    val txTime = if (proto.hasTxTime) Some(InstantSerializer.deserialize(proto.getTxTime)) else None
    new InvalidateAllCurrentResult(proto.getCount, txTime)
  } */

  override def serialize(invalidate: InvalidateAllCurrentResult): InvalidateAllCurrentResultProto = ??? /* {
    val builder = InvalidateAllCurrentResultProto.newBuilder
      .setCount(invalidate.count)
    invalidate.txTime.foreach(tx => builder.setTxTime(InstantSerializer.serialize(tx)))
    builder.build
  } */
}

object InvalidateAllCurrentByRefsSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[InvalidateAllCurrentByRefs, InvalidateAllCurrentByRefsProto] {

  override def deserialize(proto: InvalidateAllCurrentByRefsProto): InvalidateAllCurrentByRefs = ??? /* {
    InvalidateAllCurrentByRefs(
      proto.getEntityReferencesList.asScala.map(EntityReferenceSerializer.deserialize(_)),
      proto.getClassName)
  } */

  override def serialize(command: InvalidateAllCurrentByRefs): InvalidateAllCurrentByRefsProto = ??? /* {
    InvalidateAllCurrentByRefsProto.newBuilder
      .addAllEntityReferences(command.entityReferences.map(EntityReferenceSerializer.serialize(_)).asJava)
      .setClassName(command.clazzName)
      .build
  } */
}

object CreateSlotsSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[CreateSlots, CreateSlotsProto] {
  override def deserialize(proto: CreateSlotsProto): CreateSlots = ??? /* {
    val className = proto.getClassName
    val slots = proto.getSlotsList.asScala.map(_.toInt).toSet
    new CreateSlots(className, slots)
  } */

  override def serialize(cmd: CreateSlots): CreateSlotsProto = ??? /* {
    val builder = CreateSlotsProto.newBuilder
    builder.setClassName(cmd.fqcn)
    builder.addAllSlots(cmd.slots.toSeq.map(Integer.valueOf(_)) asJava)
    builder.build
  } */
}

object CreateSlotsResultSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[CreateSlotsResult, CreateSlotsResultProto] {
  override def deserialize(proto: CreateSlotsResultProto): CreateSlotsResult = ??? /* {
    val alreadyExistingSlots = proto.getAlreadyExistingSlotsList.asScala.map(_.toInt).toSet
    new CreateSlotsResult(alreadyExistingSlots)
  } */

  override def serialize(res: CreateSlotsResult): CreateSlotsResultProto = ??? /* {
    val builder = CreateSlotsResultProto.newBuilder
    if (res.alreadyExistingSlots.nonEmpty)
      builder.addAllAlreadyExistingSlots(res.alreadyExistingSlots.toSeq.map(Integer.valueOf(_)).asJava)
    builder.build()
  } */
}

object FillSlotSerializer extends WriteCommandProtoSerialization with ProtoSerializer[FillSlot, FillSlotProto] {
  override def deserialize(proto: FillSlotProto): FillSlot = ??? /* {
    val fqcn = proto.getFqcn
    val entityRef = fromProto(proto.getEntityReference)
    val newEntityVersions = {
      val vrefs = proto.getVersionedReferencesList.asScala.map(r => fromProto(r))
      val ses = proto.getSerializedEntitiesList.asScala.map(e => fromProto(e))
      vrefs zip ses
    }
    FillSlot(fqcn, entityRef, newEntityVersions.toSeq)
  } */

  override def serialize(fs: FillSlot): FillSlotProto = ??? /* {
    val builder = FillSlotProto.newBuilder
    builder.setFqcn(fs.fqcn)
    builder.setEntityReference(toProto(fs.entityReference))
    val (vrefs, ses) = fs.newEntityVersions.unzip
    builder.addAllVersionedReferences(vrefs.map(toProto(_)).asJava)
    builder.addAllSerializedEntities(ses.map(toProto(_)).asJava)
    builder.build
  } */
}
