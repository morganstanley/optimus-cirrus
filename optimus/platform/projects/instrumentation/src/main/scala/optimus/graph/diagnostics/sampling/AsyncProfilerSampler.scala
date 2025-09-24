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
import SamplingProfiler.SamplerTrait
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.graph.AsyncProfilerIntegration
import optimus.utils.PropertyUtils
import optimus.breadcrumbs.crumbs.Properties._
import optimus.scalacompat.collection._
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.ap.SampledTimers
import optimus.graph.diagnostics.ap.StackAnalysis.StackData
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.graph.diagnostics.ap.StackAnalysis.StacksAndTimers
import optimus.graph.diagnostics.ap.StackType._
import optimus.graph.diagnostics.sampling.SamplingProfiler.NANOSPERMILLI
import optimus.graph.diagnostics.sampling.SamplingProfiler.periodicSamplesSource
import optimus.graph.diagnostics.sampling.SamplingProfiler.stackDataSource

import optimus.platform.util.Log
import optimus.utils.FileUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils

import java.nio.file.Path

private[sampling] final case class StacksAndCounts(rawDump: String, preSplit: Iterable[TimedStack], dtStopped: Long)

final case class TimedStack(stack: Array[String], time: Long)

trait StackSampler {
  def start(sp: SamplingProfiler): Unit
  def shutdown(): Unit
  def drainToIterable(sp: SamplingProfiler): Iterable[TimedStack]

  // Domain-specific frame formatting provided through service loader
  def classToFrame(clz: Class[_], method: String): String = ""

  def cleanClassName(clz: String): String = ""

}

class AsyncProfilerSampler(override val sp: SamplingProfiler, extraStackSamplers: Iterable[StackSampler])
    extends SamplerTrait[StacksAndCounts, StacksAndTimers]
    with Log
    with OptimusStringUtils {
  import AsyncProfilerSampler._

  val cstack = sp.propertyUtils.get("optimus.sampling.ap.cstack", "dwarf")
  val disableAsyncProfilerOnShutdown = sp.propertyUtils.get("optimus.sampling.ap.disable.on.shutdown", true)

  val stackAnalysis = new StackAnalysis(sp)

  override def toString: String = s"${super.toString}(async-profiler)"

  private val inactiveThreshNs = sp.propertyUtils.get("optimus.sampling.inactive.cpu.thresh.ms", 1000) * 1000L * 1000L

  // Disable automatic shutdown attempt on VM exit, using flag added to
  // async-profiler by MS.
  // This prevents possibly catastrophic simultaneous shutdown via SP and via
  // AP's VMDeath hook.
  AsyncProfilerIntegration.command(s"status,globals=1,loglevel=${apLogLevel}")

  Option(System.getenv("MALLOC_CONF")).foreach(e => log.info(s"MALLOC_CONF=$e"))

  extraStackSamplers.foreach(_.start(sp))

  var runningJfr: Option[Path] = None
  var jfrFileSize: Long = 0
  var profiling = false

  override def shutdown(): Unit = {
    log.info(s"Shutting down async profiler sampling")
    if (disableAsyncProfilerOnShutdown)
      AsyncProfilerIntegration.shutdownPermanently()
    extraStackSamplers.foreach(_.shutdown())
  }

  override protected def snap(firstTime: Boolean): StacksAndCounts = {
    profiling &= !firstTime
    try {
      // Harvest stacks only every apPeriod, unless we're shutting down.
      jfrFileSize = 0
      val tStop = System.nanoTime()
      val apDump = if (profiling && ((sp.nSnaps % apPeriod) == 0 || !sp.stillPulsing)) {
        // Specify the jfr dump file iff we specified one at start
        val apDump = if (continuous) {
          // Not restarting, but still clearing stack storage, so we may need to re-store some node stacks.
          AsyncProfilerIntegration.resetSavedStacks()
          // With "dbuf", we'll switch a-p to new, empty stack storage and return the contents of the old
          AsyncProfilerIntegration.execute("collapsed,memoframes,total,dbuf")
        } else {
          val stopCmd = runningJfr.fold("stop")(jfr => s"stop,file=$jfr") + s",loglevel=${apLogLevel}"
          AsyncProfilerIntegration.command(stopCmd)
          // Retain frame name cache for 10 cycles
          AsyncProfilerIntegration.execute("collapsed,memoframes,total,mcache=10")
        }
        apDump
      } else ""

      // Start stacks every apPeriod, unless shutting down
      if (!sp.stillPulsing)
        runningJfr = None
      else if (!profiling || (sp.nSnaps % apPeriod) == 0) {
        runningJfr = if (saveJfr) {
          val jfr = FileUtils.tmpPath("ap", "jfr")
          AsyncProfilerIntegration.command(s"$apStartCmd,file=$jfr")
          Some(jfr)
        } else {
          if (!continuous || !profiling)
            AsyncProfilerIntegration.command(apStartCmd)
          None
        }
        profiling = true
      }

      // Total time spent from right before stopping AP until it's been restarted.
      val dtStopped = System.nanoTime() - tStop

      val preSplit = extraStackSamplers.flatMap(_.drainToIterable(sp)) ++ ChildProcessSampling.reapChildStacks()
      StacksAndCounts(apDump, preSplit, dtStopped)
    } catch {
      // Make sure we stop accruing stacks!
      case e: Exception =>
        extraStackSamplers.foreach(_.shutdown())
        throw e
    }
  }
  override protected def transform(prev: Option[StacksAndCounts], curr: StacksAndCounts): StacksAndTimers = {
    prev match {
      // On the first snap, prev is None and curr is the empty string because AP has only just been turned on so there
      // is no recorded data yet. Subsequent snaps will have prev be non empty, however we don't care about the data
      // in it.
      case None => StacksAndTimers.empty

      case Some(_) =>
        val parsed =
          stackAnalysis
            .extractInterestingStacks(sp.publishRootIds, curr.rawDump, curr.preSplit, numPrunedStacks)
            .copy(dtStopped = curr.dtStopped)
        if (parsed.timers.samples.values.sum > inactiveThreshNs)
          sp.recordActivity()
        parsed.applyIf(!stacksToCrumbs)(_.copy(stacks = Nil))
    }
  }

  private def stackToElems(sds: Seq[StackData]): Elems =
    Elems(profStacks -> sds.map(sd => Elems(pTpe -> sd.tpe, pTot -> sd.total, pSlf -> sd.self, pSID -> sd.hashId)))

  private def timersToElems(sd: SampledTimers): Elems = {
    Elems(
      smplTimes -> sd.samples.mapValuesNow(_ / NANOSPERMILLI)
    )
  }
  override protected def elemss(data: StacksAndTimers, id: ChainedID): Map[Source, List[Elems]] = {

    val rootElems: Iterable[Elem[Map[String, Long]]] =
      data.rootSamples.groupBy(kv => FlameTreatment.smplKey(kv._1)).map { case (key, map) =>
        key -> map.mapValuesNow(key.meta.units.fromStackCount)
      }

    val profElems = {
      apInternalStats -> AsyncProfilerIntegration
        .internalStats() :: numSamplesPublished -> data.stacks.size :: samplingPauseTime -> data.dtStopped / NANOSPERMILLI :: timersToElems(
        data.timers) ++ rootElems
    }
    Map(
      periodicSamplesSource -> (profElems :: Nil),
      stackDataSource -> data.stacks.grouped(40).map(stackToElems).toList
    )
  }
  // Compose settings from AP defaults, overridden with SP defaults, finally overridden with user defaults.
  lazy private val settings = PropertyUtils.propertyMap(
    AsyncProfilerIntegration.defaultSettings,
    spDefaults,
    AsyncProfilerIntegration.userSettings,
    sp.propertyUtils.get("optimus.sampling.async.profiler.settings", "")
  )

  // etypeframe - multiple event types with collapsed output
  // memoframes - memoize frames with integer identifier
  lazy val apStartCmd: String = {
    val event = AsyncProfilerIntegration.containerSafeEvent(settings("event"))
    s"start,event=$event,cstack=$cstack,etypeframes,persist,jemalloc,memoframes,interval=${apInterval},loglevel=${apLogLevel}"
  }

}

object FlameTreatment {
  import optimus.breadcrumbs.crumbs.PropertyUnits._

  def units(source: String): Units = {
    val sourceWithoutRev = source.stripPrefix("Rev")
    if (
      sourceWithoutRev.startsWith(Alloc) ||
      sourceWithoutRev.startsWith(Free) ||
      sourceWithoutRev.startsWith(Live)
    ) Bytes
    else if (sourceWithoutRev.startsWith(CacheHit)) Count
    else Nanoseconds
  }

  def sumOverTime(source: String): Boolean = !source.startsWith(Live)
  def avgOverTime(source: String): Boolean = !sumOverTime(source)

  def smplKey(source: String): Key[Map[String, Long]] = units(source) match {
    case Bytes =>
      if (source.startsWith(Live)) flameLive
      else flameAlloc
    case Count => flameCounts
    case _     => flameTimes
  }

}

object AsyncProfilerSampler extends Log {
  val apInterval = PropertyUtils.get("optimus.sampling.stacks.interval", "10ms")
  private val apPeriod = PropertyUtils.get("optimus.sampling.stacks.nsnaps", 1)
  val stacksToCrumbs = PropertyUtils.get("optimus.sampling.stacks.splunk", true)
  val numPrunedStacks = PropertyUtils.get("optimus.sampling.stacks", 100)
  private val saveJfr = PropertyUtils.get("optimus.sampling.jfr", false)
  val apLogLevel = PropertyUtils.get("optimus.sampling.async.profiler.log.level", "ERROR")
  val childProfilingFrequency = PropertyUtils.get("optimus.sampling.profile.child.frequency", 5)
  val allowChildProfiling = PropertyUtils.get("optimus.sampling.profile.child", false)
  val cstack = PropertyUtils.get("optimus.sampling.ap.cstack", "dwarf")
  private val continuous = PropertyUtils.get("optimus.sampling.ap.continuous", false)
  assert(!(saveJfr && continuous), "Continuous sampling incompatible with saving jfr.")

  private val spDefaults =
    s"await=${DiagnosticSettings.awaitStacks}" +
      ":event=cpu,alloc=10m,lock=99m,live"
}
