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
package optimus.platform.bitemporal

import java.time.Instant
import optimus.platform._
import optimus.platform.dal._
import optimus.platform.AsyncImplicits._
import optimus.platform.storable._

// A timeline is a partial mapping from Instants to values of type A
private[platform] trait Timeline[+A] extends ReadFence //extends PartialFunction[Instant, A]

private[platform] trait TransactionTimeline[+A] extends Timeline[A] {
  def transactionTimes: Seq[Instant]
}

private[platform] trait ValidTimeline[+A] extends Timeline[A] {
  def validTimes: Seq[Instant]
}

private[platform] trait ReadFence {
  // can not read the data from DAL beyond read transaction time (rtt)
  val rtt: Instant
}

private[platform] trait DiscreteTimeline[+A] extends Timeline[A]

//EntityRectangle represents a timeslice inside DAL
trait EntityRectangle[E <: Entity] extends ReadFence {
  def vtFrom: Instant
  def vtTo: Instant
  def ttFrom: Instant
  def ttTo: Instant
  private[optimus /*platform*/ ] def vref: VersionedReference
  private[optimus /*platform*/ ] def eref: EntityReference

  @scenarioIndependent @node private[optimus /*platform*/ ] def peOption: Option[PersistentEntity]
  @scenarioIndependent @node private[optimus /*platform*/ ] def pe: PersistentEntity
  @scenarioIndependent @node def entityOptionAt(vt: Instant, tt: Instant, entitledOnly: Boolean = false): Option[E]
  @scenarioIndependent @node def entityAt(vt: Instant, tt: Instant): E
  @scenarioIndependent @node def entity: E

  private def infFmt(i: Instant) =
    if (i == TimeInterval.Infinity) "infinity"
    else if (i == TimeInterval.NegInfinity) "-infinity"
    else i.toString

  override def toString: String =
    s"${getClass.getName}(vt=[${infFmt(vtFrom)},${infFmt(vtTo)}), tt=[${infFmt(ttFrom)},${infFmt(ttTo)}))"
}

//EntityBitemporalSpace represents the complete bitemporal space of an entity as stored inside DAL
trait EntityBitemporalSpace[E <: Entity] extends ReadFence {
  private[optimus /*platform*/ ] def eref: EntityReference

  @scenarioIndependent @node def all: Seq[EntityRectangle[E]]
  @scenarioIndependent @node def onTxTime(tt: Instant): Seq[EntityRectangle[E]]
  @scenarioIndependent @node def onValidTime(vt: Instant): Seq[EntityRectangle[E]]
  @scenarioIndependent @node def atOption(vt: Instant, tt: Instant): Option[EntityRectangle[E]]
  @scenarioIndependent @node def at(vt: Instant, tt: Instant): EntityRectangle[E]
}

// A ValidTimeline explicitly represents a slice of a bitemporal space at specific tt context
// supplied instants are implicitly valid times.
private[platform] trait BitemporalValidTimeline[E <: Entity] extends ValidTimeline[E] {
  val ttc: TransactionTimeContext
  // TODO (OPTIMUS-13414): remove this def asap. Currently needed for edge build compile (25/03/2015)
  def tt: Instant = ttc.unsafeTxTime
}

// A TransactionTimeline explicitly represents a slice of a bitemporal space at a fixed vt
// supplied instants are implicitly transaction times.
private[platform] trait BitemporalTransactionTimeline[E <: Entity] extends TransactionTimeline[E] {
  val vtc: ValidTimeContext
  // TODO (OPTIMUS-13414): remove this def asap. Currently needed for edge build compile (25/03/2015)
  def vt: Instant = vtc.unsafeValidTime
}

// Valid Timeline of Business events associated to an entity at a given transaction time
trait EntityBusinessEventTimeline[E <: Entity] extends BitemporalValidTimeline[E] {
  def entityAtVT(vt: Instant): Option[E]
  // def eventAtVT(vt: Instant): Option[BusinessEvent]
  def entityEventPairAtVT(vt: Instant): Option[(BusinessEvent, Option[E])]

  /**
   * Provide the iterator giving "start valid times (vt)" of different events that caused changes to the entity in
   * ascending order of time.
   */
  def validTimes: Seq[Instant]

  val events: Seq[BusinessEvent]
}

//handle to a specific version of an event
trait EventVersionHandle extends ReadFence {
  val validTime: Instant

  val ttFrom: Option[Instant] = None

  protected val ttTo: Option[Instant] = None

  /*
   * to load this event version at the given tt
   * if event version is not valid at that tt it will return None or Throw exception
   */

  @scenarioIndependent @node def eventOptionAtTT(tt: Instant): Option[BusinessEvent]

  @scenarioIndependent @node def eventAtTT(tt: Instant): BusinessEvent
}

//to model the actual business event version handle
private[optimus /*platform*/ ] final case class BusinessEventVersionHandle(
    private val ser: SerializedBusinessEventWithTTTo,
    override val rtt: Instant)
    extends EventVersionHandle {

  override val validTime = ser.sbe.validTime

  override val ttFrom = Some(ser.sbe.tt)

  override protected val ttTo = Some(ser.ttTo)

  private val txInterval = TimeInterval(ttFrom.get, ttTo.get)

  @scenarioIndependent @node override def eventOptionAtTT(tt: Instant): Option[BusinessEvent] = {
    if (tt.isAfter(rtt)) None
    else if (txInterval.contains(tt))
      Some(EventSerializer.deserializeBusinessEvent(FixedTransactionTimeContext(tt))(ser.sbe))
    else None
  }

  @scenarioIndependent @node override def eventAtTT(tt: Instant): BusinessEvent = {
    if (tt.isAfter(rtt)) throw new OutOfBoundEventRead(ser.sbe.id, tt)
    else if (txInterval.contains(tt))
      EventSerializer.deserializeBusinessEvent(FixedTransactionTimeContext(tt))(ser.sbe)
    else throw new EventVersionNotFoundByRefException(ser.sbe.id, ser.sbe.versionId, ser.sbe.validTime, tt)
  }
}

// to represent a none-event handle to business event version, it will just have validTime
// event version can't be loaded using none-event handle
private[optimus /*platform*/ ] final case class NoneEventVersionHandle(
    override val validTime: Instant,
    entityRef: EntityReference,
    override val rtt: Instant)
    extends EventVersionHandle {

  @scenarioIndependent @node override def eventOptionAtTT(tt: Instant): Option[BusinessEvent] = None

  @scenarioIndependent @node override def eventAtTT(tt: Instant): BusinessEvent = {
    throw new EventCantBeLoadedUsingNoneEventHandle(
      s"entity eref: ${entityRef} is impure wrt Business events, so could not load events in valid time line")
  }
}

final case class TimelineInterval(from: Instant, to: Instant)

// Valid Timeline of Business events associated to an entity at a given transaction time
@entity trait EntityValidTimeline[E <: Entity] extends BitemporalValidTimeline[E] {
  private[platform] val eref: EntityReference

  @scenarioIndependent @node def entityOptionAtVT(vt: Instant): Option[E]

  @scenarioIndependent @node def entityAtVT(vt: Instant): E

  @scenarioIndependent @node def entityEventPairAtVT(vt: Instant): Option[(BusinessEvent, Option[E])]

  /** Returns entities for all VTs available in this timeline. */
  @scenarioIndependent @node final def allEntities: Seq[E] = validTimes flatMap entityOptionAtVT

  /**
   * Provide the iterator giving "start valid times (vt)" of different events that caused changes to the entity in
   * ascending order of time.
   */
  @scenarioIndependent @node def validTimes: Seq[Instant]

  /**
   * Provide handles to the business event version which created the concerned entity versions
   */
  @scenarioIndependent @node def eventVersionHandles: Seq[EventVersionHandle]

  @scenarioIndependent @node def events: Seq[BusinessEvent]

  /**
   * Provide the gaps in the valid time line
   */
  @scenarioIndependent @node def gaps: Seq[TimelineInterval]
}

//transaction Timeline of Entity at a given valid time
trait EntityTransactionTimeline[E <: Entity] extends BitemporalTransactionTimeline[E] {
  private[platform] val eref: EntityReference

  def entityOptionAtTT(tt: Instant): Option[E]

  def entityAtTT(tt: Instant): E

  /** Returns entities for all TTs available in this timeline. */
  final def allEntities: Seq[E] = transactionTimes flatMap entityOptionAtTT

  /**
   * Provide the gaps in the transaction time line
   */
  val gaps: Seq[TimelineInterval]

  /**
   * Provide the iterator giving "start transaction times (tt)" of the entity in ascending order of time.
   */
  val transactionTimes: Seq[Instant]
}

/**
 * Provides a timeline of a Business Event including all the versions.
 */
trait BusinessEventTimeline[B <: BusinessEvent] extends TransactionTimeline[B] {

  /**
   * Provide the business event at transaction time tt as per the timeline.
   */
  def eventAtTT(tt: Instant): B

  def eventOptionAtTT(tt: Instant): Option[B]

  /**
   * Given an event, provide the transaction time of previous version of the event in case there is one.
   */
  def priorVersionTT(event: B): Instant

  /**
   * Provide the iterator giving "start transaction times (tt)" of all versions of the event in ascending order of time.
   */
  override def transactionTimes: Seq[Instant]

  private[platform] val eventref: BusinessEventReference
}
