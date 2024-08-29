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
package optimus.platform

import java.time.Instant

import msjava.base.util.uuid.MSUuid
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.dal.DALEventInfo
import optimus.platform.dal.DALImpl
import optimus.platform.dal.EntityEventModule
import optimus.platform.dal.EventLinkResolutionException
import optimus.platform.dal.LocalDALEventInfo
import optimus.platform.pickling._
import optimus.platform.storable.AppEventReference
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EventInfo
import optimus.platform.storable.Storable
import optimus.platform.storable.StorablePropertyMapOutputStream

final case class ApplicationEvent(id: AppEventReference, tt: Instant)

object BusinessEvent {
  implicit def ops[A <: BusinessEvent](e: A): EntityEventModule.BusinessEventOps[A] =
    new EntityEventModule.BusinessEventOps(e)
  def eventunpickler[T <: BusinessEvent]: Unpickler[T] = BusinessEventUnpickler.asInstanceOf[Unpickler[T]]
  def eventpickler[T <: BusinessEvent]: Pickler[T] = BusinessEventPickler.asInstanceOf[Pickler[T]]
}

class TemporaryEventException(val s: BusinessEvent)
    extends IllegalArgumentException("unexpected temporary event: %s".format(s))

object BusinessEventPickler extends Pickler[BusinessEvent] {
  override def pickle(s: BusinessEvent, visitor: PickledOutputStream): Unit = {
    visitor match {
      case v: StorablePropertyMapOutputStream =>
        v.writeRawObject(s) // for toMap method, we don't need to reload the event from DAL if we already know it
      case v =>
        if (s.dal$eventRef eq null)
          throw new TemporaryEventException(s)
        else { v.writeRawObject(s.dal$eventRef) }
    }
  }
}

object BusinessEventUnpickler extends Unpickler[BusinessEvent] {
  @node def unpickle(s: Any, ctxt: PickledInputStream): BusinessEvent = {
    s match {
      case ref: BusinessEventReference =>
        val x = DALImpl.resolver.getBusinessEvent(ref, ctxt.temporalContext.ttContext) getOrElse {
          throw new EventLinkResolutionException(ref, ctxt.temporalContext)
        }
        x
      case event: BusinessEvent =>
        event // for toMap method, corresponding with the BE Pickler (StorablePropertyMapOutputStream case)
      case o => throw new UnexpectedPickledTypeException(implicitly[Manifest[BusinessEvent]], o.getClass)
    }
  }
}

// Keep in sync with intellij.only.DoNotUseIJCompileTimeEventInjectedImplementation
trait BusinessEvent extends Storable {
  type InfoType = EventInfo

  override def $info: EventInfo
  override def $permReference: EventInfo#PermRefType = dal$eventRef

  def validTime: Instant = needsPlugin // not deferred so that jetfire doesn't need to special-case it

  /** Returns None if this event has not been loaded from the DAL */
  // returns the tt at which event was created, not the tt at which it was loaded from DAL
  final def loadTransactionTime: Option[Instant] = Option(dal$eventInfo.txTime)

  var dal$eventInfo: DALEventInfo = LocalDALEventInfo
  var dal$eventRef: BusinessEventReference = _
  var dal$loadTT: TransactionTimeContext = _

  private[this] final var _dal$cmid: Option[MSUuid] = _
  private[optimus] def dal$cmid_=(cmid: Option[MSUuid]): Unit = {
    require(
      _dal$cmid == null,
      "This setter should not be called more than once (that once should be during hydration).")
    require(cmid != null, "CMID should have a value during hydration. Setting to null would be unreasonable.")
    _dal$cmid = cmid
  }
  def cmid: Option[MSUuid] = {
    require(
      _dal$cmid != null,
      "CMID should be assigned automatically on hydration, and it shouldn't be possible to call this getter before hydration.")
    _dal$cmid
  }

  final override def dal$isTemporary: Boolean = dal$loadTT eq null

  final override def equals(that: Any): Boolean = that match {
    case be: BusinessEvent =>
      (this eq be) || ((this.getClass == be.getClass) && (dal$eventRef ne null) && (this.dal$eventRef == be.dal$eventRef) && (this.dal$loadTT == be.dal$loadTT && (this.dal$loadTT ne null)))
    case _ => false
  }

  /**
   * Used to determine if this event represents the same entity version as that event. If either or both events has(ve)
   * never been persisted (ie the DAL event ref is null), this method returns None as a reliable determination cannot be
   * made.
   */
  def representsSameVersionAs(that: BusinessEvent): Option[Boolean] =
    if ((this.dal$eventRef eq null) || (that.dal$eventRef eq null)) None
    else Some(this.dal$eventRef == that.dal$eventRef)
}

abstract class BusinessEventImpl(
    is: PickledInputStream,
    forceUnpickle: Boolean,
    info: DALEventInfo,
    eref: BusinessEventReference)
    extends BusinessEvent {
  def this() = this(null, false, LocalDALEventInfo, null)

  override final def pickle(out: PickledOutputStream): Unit = ReflectiveEventPickling.pickle(this, out)

  initEvent(info, eref)

  private[optimus] def initEvent(i: DALEventInfo, ref: BusinessEventReference): Unit = {
    dal$eventInfo = i
    dal$eventRef = ref
  }
}

trait ContainedEvent
