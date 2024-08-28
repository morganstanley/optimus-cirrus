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
package optimus.graph.diagnostics.sampling

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.platform.util.Version
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.Awaitable
import optimus.graph.DiagnosticSettings
import optimus.graph.Launchable
import optimus.graph.TestableClock
import optimus.platform.util.Log
import optimus.utils.OptimusStringUtils

import java.lang.ref.WeakReference
import java.util.Objects
import java.util.concurrent.atomic.AtomicReference

object TaskTracker extends Log with OptimusStringUtils {

  // it's important that this code doesn't run when samplingProfiler is not enabled, because otherwise we will
  // accumulate without bound data in state (we need SamplingProfiler to call cycle() periodically)

  @volatile private[diagnostics] var overrideEnabled: Option[Boolean] = None

  private def enabled = overrideEnabled.getOrElse(SamplingProfiler.stillPulsing())

  private val maxReportedActiveIds = DiagnosticSettings.getIntProperty("optimus.sampling.task.tracker.max.active", 10)

  final case class AppInstance(root: String, appId: String) extends Ordered[AppInstance] {
    override def toString: String = s"[$appId:$root]"
    override def compare(that: AppInstance): Int = {
      var ret = root.compare(that.root)
      if (ret == 0) ret = appId.compare(that.appId)
      ret
    }
    def rootId = ChainedID(root)
  }

  private[sampling] class NRef(ntsk: Awaitable) extends WeakReference[Awaitable](ntsk) {
    override def toString: String = {
      val n = this.get()
      if (Objects.isNull(n)) "null" else n.toString
    }
  }

  // The tasks itself is in a weak reference.  Absent bugs, that reference should never be null, because the
  // TaskInfo will have been removed before the Identifiable goes out of scope.
  private[sampling] final case class TInfo(
      id: Int,
      ref: NRef,
      chainedID: ChainedID,
      name: String,
      appId: String,
      scope: String,
      tStarted: Long,
      tEnded: Long) {
    def lost: Boolean = ref.get() eq null
    def deactivated: TInfo = copy(tEnded = TestableClock.nanoTime)
    def duration(t: Long): Long = if (tEnded < tStarted) (t - tStarted) else (tEnded - tStarted)
    def appInstance: AppInstance = AppInstance(chainedID.base, appId.getOrElse(SamplingProfiler.initialAppId))
  }

  private object TInfo extends OptimusStringUtils {
    def apply(ntsk: Launchable): TInfo =
      TInfo(
        id = ntsk.getId,
        ref = new NRef(ntsk),
        chainedID = ntsk.ID,
        name = ntsk.toSimpleName,
        appId = ntsk.getAppId.getOrElse("").abbrev,
        scope = ntsk.tagString.getOrElse("").abbrev,
        tStarted = TestableClock.nanoTime(),
        tEnded = 0L
      )
  }

  final case class Cycle(
      tStarted: Long,
      tEnded: Long,
      // As state, timeAnyActiveInCycle does not include current activity...
      timeAnyActiveInCycle: Long,
      // ... which is the time since lastTransitionToActive
      lastTransitionToActive: Long,
      activeAtStart: Map[Int, TInfo],
      activeInCycle: Map[Int, TInfo],
      deactivatedInCycle: Map[Int, TInfo],
      activeNow: Map[Int, TInfo],
      numActivated: Int,
      numDeactivated: Int,
      numLost: Int
  ) {

    lazy val appInstances: Seq[AppInstance] =
      activeInCycle.values.map(_.appInstance).toSeq.sorted.distinct.take(maxReportedActiveIds)

    lazy val activeAppIds: Seq[String] = appInstances.map(_.appId).distinct

    lazy val activeRoots: Seq[String] = appInstances.map(_.root).distinct

    lazy val activeChainedIDs: Seq[ChainedID] =
      activeInCycle.values.map(_.chainedID).toSeq.distinct.sortBy(_.toString).take(maxReportedActiveIds)

    lazy val activeScopes: Seq[String] = activeInCycle.values.flatMap(_.scope.emptyOrSome).toSeq.sorted.distinct

    def cycleTime = tEnded - tStarted

    def avgDuration: Long =
      if (activeInCycle.isEmpty) 0 else activeInCycle.values.map(_.duration(tEnded)).sum / activeInCycle.size

    def debugString =
      s"$toString, inst=$appInstances, ids=$activeChainedIDs, roots=$activeRoots, apps=$activeAppIds, scopes=$activeScopes, duration=$avgDuration, cycleTime=$cycleTime"

  }

  /*To track all As that were active during the interval, we maintain a running list of everything
  currently active but not yet deactivated, as well as all activations that occurred during the cycle,
  whether or not they were subsequently deactivated during the same interval.
  We care about everything that was currently registered at the start of the interval, plus all subsequent
  registrations during that interval.*/

  private val state = new AtomicReference(
    Cycle(
      tStarted = TestableClock.nanoTime(),
      tEnded = -1,
      timeAnyActiveInCycle = 0,
      lastTransitionToActive = 0,
      activeAtStart = Map.empty,
      activeInCycle = Map.empty,
      deactivatedInCycle = Map.empty,
      activeNow = Map.empty,
      numActivated = 0,
      numDeactivated = 0,
      numLost = 0
    ))

  def activate(ntsk: Launchable): Unit = if (enabled) {
    val ti = TInfo(ntsk)
    state.updateAndGet { c =>
      val ret = c.copy(
        lastTransitionToActive = if (c.activeNow.isEmpty) TestableClock.nanoTime() else c.lastTransitionToActive,
        activeNow = c.activeNow + (ti.id -> ti),
        activeInCycle = c.activeInCycle + (ti.id -> ti),
        numActivated = c.numActivated + 1
      )
      log.info(s"Activating $ti -> state=${ret.toString.abbrev}")
      ret
    }
  }

  // Returns the deactivated item, if indeed there was one.
  def deactivate(ntsk: Launchable): Unit = deactivate(ntsk.getId)
  private def deactivate(i: Int): Unit = if (enabled) {
    state
      .updateAndGet { c =>
        // After the deactivation, will there be nothing left?
        val ti = c.activeNow.get(i).map(_.deactivated)
        val fullyDeactivating = (c.activeNow.size == 1 && ti.nonEmpty && c.lastTransitionToActive > 0)
        val ret = c.copy(
          // Flush currently active time to committed
          timeAnyActiveInCycle = c.timeAnyActiveInCycle +
            (if (fullyDeactivating) TestableClock.nanoTime() - c.lastTransitionToActive else 0),
          lastTransitionToActive = if (fullyDeactivating) 0 else c.lastTransitionToActive,
          activeNow = c.activeNow - i,
          activeInCycle = ti.fold(c.activeInCycle)(ti => c.activeInCycle + (i -> ti)),
          deactivatedInCycle = ti.fold(c.deactivatedInCycle)(ti => c.deactivatedInCycle + (i -> ti)),
          numDeactivated = c.numDeactivated + 1
        )
        log.info(s"Deactivating $i, state=${c.toString.abbrev}")
        ret
      }
      .deactivatedInCycle
      .get(i)
  }

  def cycle(): Cycle = {
    val t = TestableClock.nanoTime()

    // This should always be a no-op, but we'll run it occasionally just in case dist or interop fails
    // to clean up properly.
    val lost = state.get().activeNow.filter(_._2.lost)
    lost.foreach { case (i, ti) =>
      deactivate(i)
      Breadcrumbs.warn(
        ChainedID.root,
        PropertiesCrumb(_, ProfilerSource, Properties.logMsg -> s"Lost track of $ti" :: Version.properties)
      )
    }

    // Capture the state before we prepare it for the next cycle
    val prev = state.getAndUpdate { c =>
      c.copy(
        tStarted = t,
        activeAtStart = c.activeNow,
        activeInCycle = c.activeNow,
        deactivatedInCycle = Map.empty,
        lastTransitionToActive = if (c.activeNow.nonEmpty) t else 0,
        timeAnyActiveInCycle = 0,
        numActivated = 0,
        numDeactivated = 0
      )
    }
    // We want to report the total active time, including committed and current
    prev.copy(
      numLost = lost.size,
      tEnded = t,
      timeAnyActiveInCycle =
        if (prev.lastTransitionToActive > 0) t - prev.lastTransitionToActive else prev.timeAnyActiveInCycle)
  }
}
