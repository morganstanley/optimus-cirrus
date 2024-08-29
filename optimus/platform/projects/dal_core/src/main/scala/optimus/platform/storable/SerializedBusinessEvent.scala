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

import java.time.Instant
import optimus.platform.dsi.bitemporal.MicroPrecisionInstant
import optimus.platform.dsi.bitemporal.ValidTimeHolder

sealed trait OptionalBusinessEvent extends ValidTimeHolder {
  def id: BusinessEventReference
  def versionId: Int
}

final case class SyntheticBusinessEvent(vt: MicroPrecisionInstant) extends OptionalBusinessEvent {
  def id: BusinessEventReference = null
  def versionId = -1
}

object SerializedBusinessEvent {
  def apply(
      id: BusinessEventReference,
      cmid: Option[CmReference],
      className: String,
      properties: Map[String, Any],
      keys: Seq[SerializedKey],
      types: Seq[String],
      validTime: Instant,
      versionId: Int,
      appEventId: AppEventReference,
      tt: Instant,
      lockToken: Long,
      isCancel: Boolean,
      slot: Int = 0,
      vrefOpt: Option[VersionedReference] = None): SerializedBusinessEvent =
    new SerializedBusinessEvent(
      id,
      cmid,
      className,
      properties,
      keys,
      types,
      versionId,
      MicroPrecisionInstant.ofInstant(validTime),
      appEventId,
      tt,
      lockToken,
      isCancel,
      slot,
      vrefOpt)
}

final case class SerializedBusinessEvent private (
    id: BusinessEventReference,
    cmid: Option[CmReference],
    className: String,
    properties: Map[String, Any],
    keys: Seq[SerializedKey],
    types: Seq[String],
    versionId: Int,
    vt: MicroPrecisionInstant,
    appEventId: AppEventReference,
    tt: Instant,
    lockToken: Long,
    isCancel: Boolean,
    slot: Int,
    vrefOpt: Option[VersionedReference])
    extends SerializedStorable
    with OptionalBusinessEvent {
  override def storableRef: StorableReference = id
}

final case class SerializedBusinessEventWithTTTo(sbe: SerializedBusinessEvent, ttTo: Instant)

sealed abstract class EventStateFlag(val char: Char)

object EventStateFlag {
  // TODO (OPTIMUS-13443): rename these to conform to naming standards
  case object NEW_EVENT extends EventStateFlag('n')
  case object AMEND extends EventStateFlag('a')
  case object RESTATE extends EventStateFlag('r')
  case object CANCEL extends EventStateFlag('c')

  def fromChar(c: Char) = c match {
    case 'n' => NEW_EVENT
    case 'a' => AMEND
    case 'r' => RESTATE
    case 'c' => CANCEL
  }
}

object SerializedContainedEvent {
  sealed trait ContainedEntity
  final case class AppliedHeapEntity(entity: SerializedEntity) extends ContainedEntity
  final case class UniqueHeapEntity(entity: SerializedEntity) extends ContainedEntity
  final case class StoredEntity(vt: Instant, tt: Instant) extends ContainedEntity
}

final case class SerializedContainedEvent(
    sbe: SerializedBusinessEvent,
    entityMap: Map[EntityReference, SerializedContainedEvent.ContainedEntity]
)
