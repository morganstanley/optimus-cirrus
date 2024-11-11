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
import optimus.breadcrumbs.crumbs.Crumb
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
import optimus.graph.diagnostics.sampling.SamplingProfiler.NANOSPERMILLI
import optimus.graph.diagnostics.sampling.SamplingProfiler.periodSamplesSource
import optimus.graph.diagnostics.sampling.SamplingProfiler.stackDataSource
import optimus.logging.Pid
import optimus.platform.util.Log
import optimus.utils.FileUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils

import java.net.URLEncoder
import java.nio.file.Files
import java.nio.file.Path

private[sampling] final case class StacksAndCounts(rawDump: String, preSplit: Iterable[TimedStack], dtStopped: Long)

final case class TimedStack(stack: Array[String], time: Long)

trait StackSampler {
  def start(sp: SamplingProfiler): Unit
  def shutdown(): Unit
  def drainToIterable(sp: SamplingProfiler): Iterable[TimedStack]
}

class AsyncProfilerSampler(override val sp: SamplingProfiler, extraStackSamplers: Iterable[StackSampler])
    extends SamplerTrait[StacksAndCounts, StacksAndTimers]
    with Log
    with OptimusStringUtils {
  import AsyncProfilerSampler._

  val stackAnalysis = new StackAnalysis(sp)

  // Disable automatic shutdown attempt on VM exit, using flag added to
  // async-profiler by MS.
  // This prevents possibly catastrophic simultaneous shutdown via SP and via
  // AP's VMDeath hook.
  AsyncProfilerIntegration.command(s"status,globals=1,loglevel=${apLogLevel}")

  extraStackSamplers.foreach(_.start(sp))

  var runningJfr: Option[Path] = None
  var jfrFileSize: Long = 0
  val pyroUploader = if (pyroUrl.isEmpty) None else Some(new PyroUploader(pyroUrl, pyroVersion))

  override def shutdown(): Unit = {
    log.info(s"Shutting down async profiler sampling")
    AsyncProfilerIntegration.shutdown()
    extraStackSamplers.foreach(_.shutdown())
    pyroUploader.foreach(_.shutdown())
  }

  override protected def snap(firstTime: Boolean): StacksAndCounts = {
    try {
      // Harvest stacks only every apPeriod, unless we're shutting down.
      jfrFileSize = 0
      val tStop = System.nanoTime()
      val apDump = if (!firstTime && ((sp.nSnaps % apPeriod) == 0 || !sp.stillPulsing)) {
        // Specify the jfr dump file iff we specified one at start
        val stopCmd = runningJfr.fold("stop")(jfr => s"stop,file=$jfr") + s",loglevel=${apLogLevel}"
        AsyncProfilerIntegration.command(stopCmd)
        val apDump = AsyncProfilerIntegration.dumpMemoized()
        runningJfr.foreach { jfr =>
          if (uploadJfr && Files.exists(jfr)) {
            jfrFileSize = Files.size(jfr)
            var canonical = true
            for {
              upload <- pyroUploader
              inst <- sp.publishAppInstances
            } {
              upload.jfr(
                sp.snappedMeta,
                inst,
                sp.previousSnapTime,
                sp.currentSnapTime,
                jfr,
                canonical,
                delete = !saveJfr)
              canonical = false
            }
          }
        }
        apDump
      } else ""

      // Start stacks every apPeriod, unless shutting down
      if (!sp.stillPulsing)
        runningJfr = None
      else if (firstTime || (sp.nSnaps % apPeriod) == 0) {
        runningJfr = if (uploadJfr || saveJfr) {
          val jfr = FileUtils.tmpPath("ap", "jfr")
          AsyncProfilerIntegration.command(s"$apStartCmd,file=$jfr")
          Some(jfr)
        } else {
          AsyncProfilerIntegration.command(apStartCmd)
          None
        }
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
            .extractInterestingStacks(sp.publishRootIds, curr.rawDump, curr.preSplit, numPrunedStacks, uploadFolded)
            .copy(dtStopped = curr.dtStopped)
        stackUpload(parsed.stacks)
        parsed.applyIf(!stacksToCrumbs)(_.copy(stacks = Nil))
    }
  }

  private def stackUpload(stacks: Seq[StackData]): Unit = {
    if (uploadFolded) stackPyroUpload(stacks)
  }

  private def stackPyroUpload(stacks: Seq[StackData]): Unit = {
    for {
      upload <- pyroUploader
      inst <- sp.publishAppInstances
    } {
      val byType = stacks.groupBy(_.tpe)
      byType.foreach { case (tpe, stacks) =>
        val foldedStacks = stacks.map(_.folded).mkString("\n")
        val pyroTpe = URLEncoder.encode(tpe, "UTF-8")
        upload.folded(
          sp.snappedMeta,
          inst,
          sp.previousSnapTime,
          sp.currentSnapTime,
          pyroTpe,
          foldedStacks,
          canonical = true)
      }
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
    val profElems =
      numSamplesPublished -> data.stacks.size :: samplingPauseTime -> data.dtStopped / NANOSPERMILLI :: timersToElems(
        data.timers)
    Map(
      periodSamplesSource -> (profElems :: Nil),
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
    s"start,event=$event,cstack=dwarf,etypeframes,memoframes,interval=${apInterval},loglevel=${apLogLevel}"
  }

}
object AsyncProfilerSampler extends Log {

  private val pyroUrl = PropertyUtils.get("optimus.sampling.pyro.url", "")
  // keeping it for compatibility reasons until we fully test the newest version of pyroscope
  private val pyroVersion = PropertyUtils.get("optimus.sampling.pyro.latest", false)
  val apInterval = PropertyUtils.get("optimus.sampling.stacks.interval", "10ms")
  private val apPeriod = PropertyUtils.get("optimus.sampling.stacks.nsnaps", 1)
  private val uploadJfr = pyroUrl.nonEmpty && PropertyUtils.get("optimus.sampling.pyro.jfr", false)
  val stacksToCrumbs = PropertyUtils.get("optimus.sampling.stacks.splunk", !pyroUrl.nonEmpty)
  val numPrunedStacks = PropertyUtils.get("optimus.sampling.stacks", 100)
  private val uploadFolded = pyroUrl.nonEmpty && !uploadJfr
  private val saveJfr = PropertyUtils.get("optimus.sampling.jfr", false)
  val apLogLevel = PropertyUtils.get("optimus.sampling.async.profiler.log.level", "ERROR")
  val childProfilingFrequency = PropertyUtils.get("optimus.sampling.profile.child.frequency", 5)
  val allowChildProfiling = PropertyUtils.get("optimus.sampling.profile.child", false)

  private val spDefaults =
    s"await=${DiagnosticSettings.awaitStacks}" +
      ":event=cpu,alloc=10m,lock=99m,live"

}
