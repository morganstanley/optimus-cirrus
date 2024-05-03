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
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Crumb.SamplingProfilerSource
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.graph.AsyncProfilerIntegration
import optimus.utils.PropertyUtils
import optimus.breadcrumbs.crumbs.Properties._
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.ap.SampledTimers
import optimus.graph.diagnostics.ap.StackAnalysis.StackData
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.graph.diagnostics.ap.StackAnalysis.StacksAndTimers
import optimus.graph.diagnostics.kafka.KafkaSPProducer
import optimus.graph.diagnostics.sampling.SamplingProfiler.NANOSPERMILLI
import optimus.platform.util.Log
import optimus.utils.FileUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.net.URLEncoder
import java.nio.file.Files
import java.nio.file.Path

private[sampling] final case class StacksAndCounts(rawDump: String, preSplit: Iterable[TimedStack], dtStopped: Long)

final case class TimedStack(stack: Array[String], time: Long)

trait StackSampler {
  def start(): Unit
  def shutdown(): Unit
  def drainToIterable(sp: SamplingProfiler): Iterable[TimedStack]
}

class AsyncProfilerSampler(override val sp: SamplingProfiler, extraStackSamplers: Iterable[StackSampler])
    extends SamplerTrait[StacksAndCounts, StacksAndTimers]
    with Log
    with OptimusStringUtils {
  import AsyncProfilerSampler._

  // Allocation every (1M - 1)B, contention every 99ms second. The choice of one less than
  // a power of 2 for allocation sampling is probably useless, based on the random algorithm in
  // https://github.com/openjdk/jdk/blob/b92de54a81a4037a5396509d41de57323212639c/src/hotspot/share/runtime/threadHeapSampler.cpp
  private val spDefaults =
    s"await=${DiagnosticSettings.awaitStacks}:event=cpu,etypeframes,memoframes,alloc=1048575b,lock=99m,live"

  // Disable automatic shutdown attempt on VM exit, using flag added to
  // async-profiler by MS.
  // This prevents possibly catastrophic simultaneous shutdown via SP and via
  // AP's VMDeath hook.
  AsyncProfilerIntegration.command(s"status,globals=1,loglevel=${apLogLevel}")

  extraStackSamplers.foreach(_.start())

  // Compose settings from AP defaults, overridden with SP defaults, finally overridden with user defaults.
  private val settings = PropertyUtils.propertyMap(
    AsyncProfilerIntegration.defaultSettings,
    spDefaults,
    AsyncProfilerIntegration.userSettings)

  lazy val apStartCmd = {
    val e0 = settings("event")
    val event = if (e0.contains("cpu") && !AsyncProfilerIntegration.cpuProfilingEnabled(apLogLevel)) {
      val e = e0.replaceAllLiterally("cpu", "itimer")
      log.warn(s"Changing event $e0 to $e")
      e
    } else e0
    s"start,event=$event,cstack=dwarf,interval=${apInterval},loglevel=${apLogLevel}"
  }
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

      val preSplit = extraStackSamplers.flatMap(_.drainToIterable(sp))
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
          StackAnalysis
            .extractInterestingStacks(sp.publishRootIds, curr.rawDump, curr.preSplit, numPrunedStacks)
            .copy(dtStopped = curr.dtStopped)
        stackUpload(parsed.stacks)
        parsed.applyIf(!stacksToSplunk)(_.copy(stacks = Nil))
    }
  }

  private def stackUpload(stacks: Seq[StackData]): Unit = {
    if (enableKafkaStackPublication) stackKafkaUpload(stacks)
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

  private def stackKafkaUpload(stacks: Seq[StackData]): Unit =
    for (kafkaUploader <- sp.spKafkaProducer) {
      val topic = kafkaUploader.topic
      val rootUUidKey = sp.publishRootIds.mkString(",")
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(stacks)
      oos.close()
      val kafkaStackRecord = new ProducerRecord(topic, rootUUidKey, stream.toByteArray)
      kafkaUploader.kafkaStackProducer.send(kafkaStackRecord, KafkaSPProducer.messageCallback)
    }

  private def stackToElems(sds: Seq[StackData]): Elems =
    Elems(profStacks -> sds.map(sd => Elems(pTpe -> sd.tpe, pTot -> sd.total, pSlf -> sd.self, pSID -> sd.hashId)))

  private def timersToElems(sd: SampledTimers): Elems = {
    import sd._
    Elems(
      smplEverything -> everything / NANOSPERMILLI,
      smplGraphTime -> graphTime / NANOSPERMILLI,
      smplSyncStackTime -> syncStackTime / NANOSPERMILLI,
      smplCacheTime -> cacheTime / NANOSPERMILLI,
      smplTweakLookupATime -> tweakLookupAsync / NANOSPERMILLI,
      smplTweakLookupSTime -> tweakLookupSync / NANOSPERMILLI,
      smplOHSampling -> overheadSampling / NANOSPERMILLI,
      smplOHInstrum -> overheadInstrum / NANOSPERMILLI
    )
  }
  override protected def elemss(data: StacksAndTimers, id: ChainedID): Map[Source, List[Elems]] = {
    val profElems = samplingPauseTime -> data.dtStopped / NANOSPERMILLI :: timersToElems(data.timers)
    Map(
      ProfilerSource -> (profElems :: Nil),
      SamplingProfilerSource -> data.stacks.grouped(40).map(stackToElems).toList
    )
  }

}
object AsyncProfilerSampler {

  private val pyroUrl = PropertyUtils.get("optimus.sampling.pyro.url", "")
  // keeping it for compatibility reasons until we fully test the newest version of pyroscope
  private val pyroVersion = PropertyUtils.get("optimus.sampling.pyro.latest", false)
  private val apInterval = PropertyUtils.get("optimus.sampling.stacks.interval", "10ms")
  private val apPeriod = PropertyUtils.get("optimus.sampling.stacks.nsnaps", 1)
  private val uploadJfr = pyroUrl.nonEmpty && PropertyUtils.get("optimus.sampling.pyro.jfr", false)
  val stacksToSplunk = PropertyUtils.get("optimus.sampling.stacks.splunk", !pyroUrl.nonEmpty)
  val numPrunedStacks = PropertyUtils.get("optimus.sampling.stacks", if (stacksToSplunk) 20 else 100)
  private val uploadFolded = pyroUrl.nonEmpty && !uploadJfr
  private val saveJfr = PropertyUtils.get("optimus.sampling.jfr", false)
  val enableKafkaStackPublication = PropertyUtils.get("optimus.sampling.kafka.upload", false)
  val apLogLevel = PropertyUtils.get("optimus.sampling.async.profiler.log.level", "ERROR")
}
