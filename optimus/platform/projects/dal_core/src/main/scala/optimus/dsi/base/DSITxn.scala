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
import optimus.platform.dsi.bitemporal.AccTableResult
import optimus.platform.dsi.bitemporal.CreateSlotsResult
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.dsi.bitemporal.GeneratedAppEventResult
import optimus.platform.dsi.bitemporal.InvalidateAllCurrentResult
import optimus.platform.dsi.bitemporal.PutApplicationEventResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.VoidResult
import optimus.platform.storable.SerializedAppEvent

object DSITxn {
  private def lift(act: TxnAction): Seq[TxnAction] = act :: Nil

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

  type Actions = Seq[TxnAction]

  def replaceTt(txn: DSITxn[Seq[Result]], tt: Instant): DSITxn[Seq[Result]] = {
    val ttLong = DateTimeSerialization.fromInstant(tt)
    def updateAppEvent(appEvent: SerializedAppEvent) = {
      if (appEvent.dalTT.isDefined) appEvent.copy(dalTT = Some(tt)) else appEvent.copy(tt = tt)
    }

    val updatedResults: Seq[Result] = txn.result.map {
      case ga: GeneratedAppEventResult =>
        val putResults = ga.putResults.map(_.copy(txTime = tt, lockToken = ttLong))
        val invResults = ga.invResults.map(_.copy(txTime = tt))
        GeneratedAppEventResult(updateAppEvent(ga.appEvent), ga.assertResults, putResults, invResults)
      case pae: PutApplicationEventResult =>
        val bes = pae.beResults.map { be =>
          val puts = be.putResults.map(_.copy(txTime = tt, lockToken = ttLong))
          val invalids = be.invResults.map(_.copy(txTime = tt))
          val reverts = be.revertResults.map(_.copy(txTime = tt, lockToken = ttLong))
          be.copy(putResults = puts, revertResults = reverts, invResults = invalids, txTime = tt)
        }
        pae.copy(appEvent = updateAppEvent(pae.appEvent), beResults = bes)
      case ic: InvalidateAllCurrentResult => ic
      case cs: CreateSlotsResult          => cs
      case atr: AccTableResult            => atr
      case VoidResult                     => VoidResult
      /*
       * We shouldn't see any other results at this point in execution
       */
      case ns => throw new IllegalArgumentException(s"replaceTt called with unsupported result type: $ns")
    }
    val updatedTxnActions = txn.actions.map {
      case poe: PutObliterateEffect          => PutObliterateEffect(poe.effect.copy(txTime = tt))
      case pbeie: PutBusinessEventIndexEntry => pbeie.copy(txTimeOpt = Some(tt))
      case pae: PutAppEvent                  => pae.dalTT.map(_ => pae.copy(dalTT = Some(tt))).getOrElse(pae)
      case pk: PutTemporalKey                => pk.copy(key = pk.key.copy(lockToken = ttLong))
      // UpdateEntityGrouping do have existing lock token but that doesn't required tt replacement
      case eg: EntityGroupingTxnAction     => eg
      case pie: PutIndexEntry              => pie
      case cie: CloseIndexEntry            => cie
      case ptie: PutRegisteredIndexEntry   => ptie
      case ctie: CloseRegisteredIndexEntry => ctie
      case lnk: PutLinkageEntry            => lnk
      case clnk: CloseLinkageEntry         => clnk
      case pts: PutBusinessEventTimeSlice  => pts
      //  UpdateBusinessEventGrouping do have existing lock token but that doesn't required tt replacement
      case grp: BusinessEventGroupingTxnAction => grp
      case rgrp: ReuseBusinessEvent            => rgrp
      case cbets: CloseBusinessEventTimeSlice  => cbets
      case pbek: PutBusinessEventKey           => pbek
      // tt interval is only passed by tombstone backfiller admin script and that doesn't required tt replacement
      case pets: PutEntityTimeSlice   => pets
      case cets: CloseEntityTimeSlice => cets
      case pcr: PutCmReference        => pcr
      //  UpdateUniqueIndexGrouping do have existing lock token but that doesn't required tt replacement
      case uita: UniqueIndexGroupingTxnAction   => uita
      case uitsa: UniqueIndexTimeSliceTxnAction => uitsa
      case eo: ExecuteObliterate                => eo
      case pdre: PutDBRawEffect                 => pdre
      case dra: DBRawAction                     => dra
      case ass: AddWriteSlots                   => ass
      case fws: FillWriteSlot                   => fws
      // tt is coming from acc cmd so don't required tt replacement
      case acc: AccAction         => acc
      case pcm: PutClassIdMapping => pcm
    }
    DSITxn(updatedResults, updatedTxnActions)
  }
}

final case class DSITxn[+A](result: A, actions: Seq[TxnAction]) {
  def map[B](f: A => B): DSITxn[B] = DSITxn(f(result), actions)

  override def equals(other: Any) = other match {
    case otherDsiTxn: DSITxn[_] =>
      result == otherDsiTxn.result && actions.toSet == otherDsiTxn.actions.toSet
    case _ => false
  }
}
