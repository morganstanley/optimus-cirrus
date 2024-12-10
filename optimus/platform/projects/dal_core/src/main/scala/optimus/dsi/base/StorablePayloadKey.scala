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
package optimus.dsi.base

import java.time.Instant

import optimus.platform.dsi.bitemporal.proto.BusinessEventReferenceSerializer
import optimus.platform.dsi.bitemporal.proto.InstantSerializer
import optimus.platform.dsi.bitemporal.proto.VersionedReferenceSerializer
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.SerializedBusinessEvent
import optimus.platform.dsi.bitemporal.proto.Dsi.EntityBusinessEventComboProto
import optimus.platform.dsi.bitemporal.proto.Dsi.EntityBusinessEventWithTTToComboProto
import optimus.platform.dsi.bitemporal.proto.Dsi.GetInitiatingEventResultProto
import optimus.platform.dsi.bitemporal.proto.Dsi.PersistentEntityProto
import optimus.platform.dsi.bitemporal.proto.Dsi.SerializedBusinessEventProto
import optimus.platform.dsi.bitemporal.proto.Dsi.SerializedBusinessEventWithTTToProto

object StorablePayloadKey {

  def apply(pe: PersistentEntity, cmdTt: Instant): StorablePayloadKey = {
    val se = pe.serialized
    val vref = SlottedVersionedReference(pe.versionedRef, se.slot)
    val className = se.className
    EntityStorablePayloadKey(vref, className, cmdTt)
  }

  def apply(sbe: SerializedBusinessEvent, cmdTt: Instant): StorablePayloadKey = {
    val className = sbe.className
    EventStorablePayloadKey(sbe.id, sbe.slot, sbe.versionId, className, sbe.tt, cmdTt)
  }

  def apply(pe: PersistentEntityProto, cmdTt: Instant): StorablePayloadKey = ??? /* {
    val se = pe.getSerializedEntity
    val vref = SlottedVersionedReference(VersionedReferenceSerializer.deserialize(pe.getVersionedReference), se.getSlot)
    val className = se.getClassName
    EntityStorablePayloadKey(vref, className, cmdTt)
  } */

  def apply(sbe: SerializedBusinessEventProto, cmdTt: Instant): StorablePayloadKey = ??? /* {
    val bref = BusinessEventReferenceSerializer.deserialize(sbe.getEventRef)
    val slot = if (sbe.hasSlot) sbe.getSlot else 0
    val className = sbe.getClassName
    EventStorablePayloadKey(
      bref,
      slot,
      sbe.getVersionId,
      className,
      InstantSerializer.deserialize(sbe.getTxTime),
      cmdTt)
  } */

  def apply(sbett: SerializedBusinessEventWithTTToProto, cmdTt: Instant): StorablePayloadKey = ??? /* {
    val sbe = sbett.getBusinessEvent
    StorablePayloadKey(sbe, cmdTt)
  } */

  def extract(combo: EntityBusinessEventComboProto, cmdTt: Instant): Seq[StorablePayloadKey] = ??? /* {
    if (combo.hasPersistentEntity) {
      val sbe = combo.getBusinessEvent
      val pe = combo.getPersistentEntity
      Seq(StorablePayloadKey(pe, cmdTt), StorablePayloadKey(sbe, cmdTt))
    } else {
      val sbe = combo.getBusinessEvent
      Seq(StorablePayloadKey(sbe, cmdTt))
    }
  } */

  def extract(combott: EntityBusinessEventWithTTToComboProto, cmdTt: Instant): Seq[StorablePayloadKey] = ??? /* {
    val peSeq = if (combott.hasPersistentEntity) {
      val pe = combott.getPersistentEntity
      Seq(StorablePayloadKey(pe, cmdTt))
    } else Seq.empty
    val sbettSeq = if (combott.hasBusinessEvent) {
      val sbett = combott.getBusinessEvent
      Seq(StorablePayloadKey(sbett, cmdTt))
    } else Seq.empty
    peSeq ++ sbettSeq
  } */

  def extract(gie: GetInitiatingEventResultProto, cmdTt: Instant): Seq[StorablePayloadKey] = ??? /* {
    val peSeq = if (gie.hasPersistentEntity) {
      val pe = gie.getPersistentEntity
      Seq(StorablePayloadKey(pe, cmdTt))
    } else Seq.empty
    val sbeSeq = if (gie.hasBusinessEvent) {
      val sbe = gie.getBusinessEvent
      Seq(StorablePayloadKey(sbe, cmdTt))
    } else Seq.empty
    peSeq ++ sbeSeq
  } */
}

sealed trait StorablePayloadKey {
  val className: String
  def cmdTt: Instant
}

final case class EventStorablePayloadKey(
    ref: BusinessEventReference,
    slot: Int,
    vid: Long,
    className: String,
    tt: Instant,
    override val cmdTt: Instant)
    extends StorablePayloadKey
final case class EntityStorablePayloadKey(
    vref: SlottedVersionedReference,
    className: String,
    override val cmdTt: Instant)
    extends StorablePayloadKey
