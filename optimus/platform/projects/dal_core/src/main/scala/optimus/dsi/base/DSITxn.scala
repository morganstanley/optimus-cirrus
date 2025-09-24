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
import optimus.dsi.base.actions._
import optimus.core.Collections
import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.bitemporal.AccTableResult
import optimus.platform.dsi.bitemporal.CreateSlotsResult
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.dsi.bitemporal.GeneratedAppEventResult
import optimus.platform.dsi.bitemporal.InvalidateAllCurrentResult
import optimus.platform.dsi.bitemporal.PutApplicationEventResult
import optimus.platform.dsi.bitemporal.PrepareMonoTemporalResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.VoidResult
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.storable.SerializedAppEvent

object DSITxn {
  type Actions = Seq[TxnAction]
  def sequence[A](txns: Seq[DSITxn[A]]): DSITxn[Seq[A]] = {
    val resultBuilder = Vector.newBuilder[A]
    val actionBuilder = Vector.newBuilder[TxnAction]

    val it = txns.iterator
    while (it.hasNext) {
      val txn = it.next()
      resultBuilder += txn.result
      Collections.inline_++=(actionBuilder, txn.actions)
    }

    DSITxn(resultBuilder.result(), actionBuilder.result())
  }

  def pure[A](a: A) = DSITxn(a, Vector.empty)
  def actions(acts: Actions) = DSITxn((), acts)

  private def updateAppEvent(appEvent: SerializedAppEvent, tt: Instant) = {
    if (appEvent.dalTT.isDefined) appEvent.copy(dalTT = Some(tt)) else appEvent.copy(tt = tt)
  }

  def replaceDalTt(txn: DSITxn[Seq[Result]], tt: Instant): DSITxn[Seq[Result]] = {
    val updatedResults: Seq[Result] = txn.result.map {
      case pae: PutApplicationEventResult => pae.copy(appEvent = updateAppEvent(pae.appEvent, tt))
      case ga: GeneratedAppEventResult    => ga.copy(appEvent = updateAppEvent(ga.appEvent, tt))
      case x                              => x
    }
    DSITxn(updatedResults, txn.actions)
  }

  def replaceTtAndDalTt(txn: DSITxn[Seq[Result]], tt: Instant): DSITxn[Seq[Result]] = {
    val ttLong = DateTimeSerialization.fromInstant(tt)
    val updatedResults: Seq[Result] = txn.result.map {
      case ga: GeneratedAppEventResult =>
        val putResults = ga.putResults.map(_.copy(txTime = tt, lockToken = ttLong))
        val invResults = ga.invResults.map(_.copy(txTime = tt))
        GeneratedAppEventResult(updateAppEvent(ga.appEvent, tt), ga.assertResults, putResults, invResults)
      case pae: PutApplicationEventResult =>
        val bes = pae.beResults.map { be =>
          val puts = be.putResults.map(_.copy(txTime = tt, lockToken = ttLong))
          val invalids = be.invResults.map(_.copy(txTime = tt))
          val reverts = be.revertResults.map(_.copy(txTime = tt, lockToken = ttLong))
          be.copy(putResults = puts, revertResults = reverts, invResults = invalids, txTime = tt)
        }
        pae.copy(appEvent = updateAppEvent(pae.appEvent, tt), beResults = bes)
      case ic: InvalidateAllCurrentResult => ic
      case pmt: PrepareMonoTemporalResult => pmt
      case cs: CreateSlotsResult          => cs
      case atr: AccTableResult            => atr
      case VoidResult                     => VoidResult
      /*
       * We shouldn't see any other results at this point in execution
       */
      case ns => throw new IllegalArgumentException(s"replaceTt called with unsupported result type: $ns")
    }
    DSITxn(updatedResults, txn.actions)
  }
  private def lift(act: TxnAction): Seq[TxnAction] = act :: Nil
}

final class DSITxnActionsConfigValues {
  private val includeTxnActionSizeDefault =
    DiagnosticSettings.getBoolProperty("optimus.dsi.base.resultSender.includeTxnActionSize", true)
  private val includeTxnActionSizeBySourceDefault =
    DiagnosticSettings.getBoolProperty("optimus.dsi.base.resultSender.includeTxnActionSizeBySource", false)
  private[dsi] var includeTxnActionSize = includeTxnActionSizeDefault
  private[dsi] var includeTxnActionSizeBySource = includeTxnActionSizeBySourceDefault
}

object DSITxnActionsConfig extends SimpleStateHolder(() => new DSITxnActionsConfigValues) {

  def setConfigValue(includeTxnActionSizeFlag: Boolean, includeTxnActionSizeBySourceFlag: Boolean) =
    getState.synchronized {
      getState.includeTxnActionSize = includeTxnActionSizeFlag
      getState.includeTxnActionSizeBySource = includeTxnActionSizeBySourceFlag
    }

  def getIncludeTxnActionSize: Boolean = getState.synchronized { getState.includeTxnActionSize }
  def getIncludeTxnActionSizeBySource: Boolean = getState.synchronized { getState.includeTxnActionSizeBySource }
}

final case class DSITxn[+A](result: A, actions: Seq[TxnAction]) {
  def map[B](f: A => B): DSITxn[B] = DSITxn(f(result), actions)

  override def equals(other: Any) = other match {
    case otherDsiTxn: DSITxn[_] =>
      result == otherDsiTxn.result && actions.toSet == otherDsiTxn.actions.toSet
    case _ => false
  }

  def actionsSizeBySourceInfo(): Map[String, Int] = {
    var r = Map[String, Int]()
    if (DSITxnActionsConfig.getIncludeTxnActionSize) {
      r ++= Map("tx_acs" -> actions.size)
      if (DSITxnActionsConfig.getIncludeTxnActionSizeBySource) {
        r ++= actions.groupBy(actionToSourceGroup).map { case (k, v) =>
          k.toString -> v.size
        }
      }
    }

    r
  }
  private def actionToSourceGroup(action: TxnAction): TxnActionGroup = {
    action match {
      case _: AccAction                 => OtherGroup // other
      case _: PutTemporalKey            => EntityKeyGroup // entityKey
      case _: PutRegisteredIndexEntry   => EntityRegisteredIndexGroup // entityRegisteredIndex
      case _: CloseRegisteredIndexEntry => EntityRegisteredIndexGroup // entityRegisteredIndex
      case _: PutIndexEntry             => EntityIndexGroup // entityIndex
      case _: CloseIndexEntry           => EntityIndexGroup // entityIndex
      case _: PutLinkageEntry           => EntityLinkageGroup // entityLinkage
      case _: CloseLinkageEntry         => EntityLinkageGroup // entityLinkage
      case _: PutAppEvent               => AppEventGroup // appEvent
      case _: PutBusinessEventKey       => EventKeyGroup // eventKey
      case _: PutObliterateEffect       => OtherGroup // other
      case _: ExecuteObliterate         => ObliterateGroup // obliterate
      case _: DBRawAction               => DbRawGroup // dbRaw
      case _: PutDBRawEffect            => DbRawGroup // dbRaw
      case _: AddWriteSlots             => OtherGroup // other
      case _: FillWriteSlot             => OtherGroup // other
      case _: PutClassIdMapping         => OtherGroup // other
      case _: WorklogOnlyTxnAction      => OtherGroup // other
      case _: PutClassInfoEntry         => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: UpdateClassInfoEntry      => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: PutEntityGrouping         => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: UpdateEntityGrouping      => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: PutUniqueIndexGrouping    => EntityUniqueIndexGroupingTimesliceGroup // entityUniqueIndexGroupingTimeslice
      case _: UpdateUniqueIndexGrouping => EntityUniqueIndexGroupingTimesliceGroup // entityUniqueIndexGroupingTimeslice
      case _: PutBusinessEventGrouping  => EventGroupingTimeslice // eventGroupingTimeslice
      case _: UpdateBusinessEventGrouping => EventGroupingTimeslice // eventGroupingTimeslice
      case _: PutEntityTimeSlice          => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: CloseEntityTimeSlice        => EntityGroupingTimesliceGroup // entityGroupingTimeslice
      case _: PutUniqueIndexTimeSlice   => EntityUniqueIndexGroupingTimesliceGroup // entityUniqueIndexGroupingTimeslice
      case _: CloseUniqueIndexTimeSlice => EntityUniqueIndexGroupingTimesliceGroup // entityUniqueIndexGroupingTimeslice
      case _: PutBusinessEventTimeSlice => EventGroupingTimeslice // eventGroupingTimeslice
      case _: CloseBusinessEventTimeSlice => EventGroupingTimeslice // eventGroupingTimeslice
      case _: PutCmReference              => MetadataGroup // metadata
      case _: PutBusinessEventIndexEntry  => EventIndexGroup // eventIndex
    }
  }

  def actionsSizeBySource(): Map[TxnActionGroup, Int] = {
    actions.groupBy(actionToSourceGroup).map { case (k, v) => (k, v.size) }
  }
}

sealed trait TxnActionGroup
case object OtherGroup extends TxnActionGroup {
  override def toString: String = "tx_o"
}
case object EntityKeyGroup extends TxnActionGroup {
  override def toString: String = "tx_enk"
}
case object EntityRegisteredIndexGroup extends TxnActionGroup {
  override def toString: String = "tx_enri"
}
case object EntityIndexGroup extends TxnActionGroup {
  override def toString: String = "tx_eni"
}
case object EntityLinkageGroup extends TxnActionGroup {
  override def toString: String = "tx_enl"
}
case object AppEventGroup extends TxnActionGroup {
  override def toString: String = "tx_ae"
}
case object EventKeyGroup extends TxnActionGroup {
  override def toString: String = "tx_evk"
}
case object ObliterateGroup extends TxnActionGroup {
  override def toString: String = "tx_ob"
}
case object DbRawGroup extends TxnActionGroup {
  override def toString: String = "tx_dbr"
}
case object EntityGroupingTimesliceGroup extends TxnActionGroup {
  override def toString: String = "tx_engts"
}
case object EntityUniqueIndexGroupingTimesliceGroup extends TxnActionGroup {
  override def toString: String = "tx_enuigts"
}
case object EventGroupingTimeslice extends TxnActionGroup {
  override def toString: String = "tx_evgts"
}
case object MetadataGroup extends TxnActionGroup {
  override def toString: String = "tx_m"
}
case object EventIndexGroup extends TxnActionGroup {
  override def toString: String = "tx_evi"
}
