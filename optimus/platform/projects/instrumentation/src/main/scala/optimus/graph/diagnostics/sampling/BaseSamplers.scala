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
import com.sun.management.OperatingSystemMXBean
import com.sun.management.UnixOperatingSystemMXBean
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsKafkaPublisher
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties.Key
import optimus.graph.diagnostics.sampling.SamplingProfiler.SamplerTrait
import optimus.graph.diagnostics.sampling.SamplingProfiler._
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.breadcrumbs.crumbs.Properties._
import optimus.graph.DiagnosticSettings
import optimus.platform.util.Log
import optimus.scalacompat.collection._
import optimus.platform.util.Version
import org.apache.kafka.common.MetricName

import scala.jdk.CollectionConverters._
import sun.management.ManagementFactoryHelper

import java.io.BufferedReader
import java.io.InputStreamReader
import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryUsage
import java.util.concurrent.ConcurrentHashMap
import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger
import java.{util => jutil}
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.immutable.ArraySeq
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.util.Using

object BaseSamplers extends Log {
  // Very simple command executor.  No exception handling, since this is meant to be called from samplers,
  // which will auto-disable on any exception.
  private[optimus] final class ExecIterator(cmd: String*) extends Iterator[String] with AutoCloseable {
    private var p: Process = Runtime.getRuntime.exec(cmd.toArray)
    private var br: BufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream))
    private var onbase: String = br.readLine()
    val process = p.pid()
    if (Objects.isNull(onbase)) close()

    override def close(): Unit = {
      onbase = null
      if (Objects.nonNull(br))
        br.close()
      br = null
      if (Objects.nonNull(p))
        p.destroy()
      p = null
    }
    override def hasNext: Boolean = Objects.nonNull(onbase)
    override def next(): String = {
      val ret = onbase
      onbase = br.readLine()
      if (Objects.isNull(onbase)) close()
      ret
    }
  }

  private final case class StatsAccumulator private (n: Int, sum: Double, sumsq: Double) {
    def add(v: Double): StatsAccumulator = copy(n = n + 1, sum = sum + v, sumsq = sumsq + v * v)
    def avg = if (n <= 0) 0.0 else sum / n
    def sd: Double = if (n <= 0) 0.0 else Math.sqrt(sumsq / n - avg * avg)
    def toMap: Map[String, Double] = Map("n" -> n, "avg" -> avg, "sd" -> sd)
  }
  private object StatsAccumulator {
    def apply(v: Double): StatsAccumulator = StatsAccumulator(1, v, v * v)
  }

  private val counters = new ConcurrentHashMap[Key[Long], Long]
  private val gauges = new ConcurrentHashMap[Key[Long], Long]
  private val stats = new ConcurrentHashMap[String, StatsAccumulator]

  private[diagnostics] def accumulateStats[N: Numeric](key: String, value: N): Unit = {
    val d = implicitly[Numeric[N]].toDouble(value)
    stats.compute(
      key,
      {
        case (_, null) => StatsAccumulator(d)
        case (_, s)    => s.add(d)
      })
  }

  private def snapStats(): Map[String, StatsAccumulator] = {
    val ret = stats.asScala.toMap
    stats.clear()
    ret
  }

  def increment(key: Key[Long], incr: Long): Long =
    counters.compute(
      key,
      {
        case null   => incr
        case (_, c) => c + incr
      })

  def setCounter(key: Key[Long], value: Long): Unit = counters.put(key, value)

  def setGauge(key: Key[Long], value: Long): Unit = gauges.put(key, value)

  def snapCountersMap: Map[Key[Long], Long] = counters.asScala.toMap

  def snapGaugesMap: Map[Key[Long], Long] = gauges.asScala.toMap

  private[sampling] def clearCounters(): Unit = counters.clear()

  val treadmill: Map[String, String] = Try {
    val map = Files
      .list(Paths.get("/env"))
      .iterator()
      .asScala
      .collect {
        case f if f.getFileName.toString.startsWith("TREADMILL") =>
          f.getFileName.toString -> Files.readString(f).trim
      }
      .toMap
    Breadcrumbs.info(
      ChainedID.root,
      PropertiesCrumb(_, SamplingProfiler.periodicSamplesSource, treadmillEnv -> map :: Version.properties))
    map
  }.getOrElse(Map.empty)

  def isTreadmill: Boolean = treadmill.nonEmpty

}

//noinspection ScalaUnusedSymbol // ServiceLoader
class BaseSamplers extends SamplerProvider with Log {
  import BaseSamplers._
  import scala.util.Try

  override val priority: Int = 0

  private val osBeanMaybe = Try(ManagementFactory.getOperatingSystemMXBean().asInstanceOf[OperatingSystemMXBean])

  def provide(sp: SamplingProfiler): Seq[SamplerTrait[_, _]] = {
    val util = new Util(sp)
    import util._

    val ss = ArraySeq.newBuilder[SamplerTrait[_, _]]
    // Various OS bean stats.  Note that the snap might throw, in which case the
    // individual snapper will be disabled.
    // Since java 17, these are by default cgroup aware
    osBeanMaybe.foreach { osBean =>
      ss += Diff(_ => osBean.getProcessCpuTime / NANOSPERMILLI, profJvmCPUTime)
      ss += Snap(_ => osBean.getSystemCpuLoad, profSysCPULoad)
      ss += Snap(_ => osBean.getProcessCpuLoad, profJvmCPULoad)
      ss += Snap(_ => osBean.getSystemLoadAverage, profLoadAvg)
      ss += Snap(_ => osBean.getCommittedVirtualMemorySize / MILLION, profCommitedMB)
      ss += Snap(_ => osBean.getFreePhysicalMemorySize / MILLION, profSysFreeMem)
      ss += Snap(_ => osBean.getTotalPhysicalMemorySize / MILLION, profSysTotalMem)
      osBean match {
        case unix: UnixOperatingSystemMXBean =>
          ss += Snap(_ => unix.getOpenFileDescriptorCount, profOpenFD)
        case _ =>
      }
    }
    Option(ManagementFactory.getCompilationMXBean).filter(_.isCompilationTimeMonitoringSupported).foreach { bean =>
      ss += Diff(_ => bean.getTotalCompilationTime, profJitTime)
    }

    val cgroups = Paths.get("/sys/fs/cgroup")
    if (Files.isDirectory(cgroups) && Files.isReadable(cgroups)) {
      log.info(s"cgroups detected. treadmill=${isTreadmill}")
      def mem(metric: String): Long =
        Try(Files.readString(cgroups.resolve(s"memory/memory.${metric}_in_bytes")).trim.toLong / MILLION).getOrElse(-1)
      ss += Snap(_ => Seq("usage", "limit", "max_usage").map(k => k -> mem(k)).toMap, profCgroupMem)
    }

    val classLoadBean = ManagementFactoryHelper.getHotspotClassLoadingMBean
    ss += Diff(_ => classLoadBean.getClassLoadingTime, profClTime)

    // Total time spent in collections during epoch, according to gcBeans
    val gcBeans: jutil.List[GarbageCollectorMXBean] = ManagementFactory.getGarbageCollectorMXBeans
    ss += Diff[Long](_ => gcBeans.asScala.map(_.getCollectionTime * ALREADYMILLIS).sum, profGcTimeAll)

    val memBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    ss += new Sampler[MemoryUsage, MemoryUsage](
      sp,
      "GCMonitor heap",
      snapper = _ => memBean.getHeapMemoryUsage,
      process = LATEST,
      publish = (usage: MemoryUsage) =>
        Elems(
          profCurrHeap -> usage.getUsed / MILLION,
          profMaxHeap -> usage.getMax / MILLION
        )
    )
    ss += new Sampler[MemoryUsage, MemoryUsage](
      sp,
      "GCMonitor non-heap",
      snapper = _ => memBean.getNonHeapMemoryUsage,
      process = LATEST,
      publish = (usage: MemoryUsage) =>
        Elems(
          profCurrNonHeap -> usage.getUsed / MILLION,
          profMaxNonHeap -> usage.getMax / MILLION
        )
    )

    ss += Snap(_ => memBean.getObjectPendingFinalizationCount, gcFinalizerCount)

    val maxPsFailures = new AtomicInteger(10)

    if (!DiagnosticSettings.isWindows)
      ss += Snap(_ => {
        val ph = ProcessHandle.current()
        val directCount = ph.children().count().toInt
        if (directCount == 0) Elems(childProcessCount -> 0)
        else {
          val descendants = ProcessHandle.current().descendants().iterator().asScala.map(_.pid).mkString(",")
          Using(new ExecIterator("/usr/bin/ps", "-p", descendants, "--noheaders", "--format", "rss,pcpu")) { i =>
            var rss = 0
            var cpu = 0.0
            i.foreach { line =>
              line.trim.split("\\s+") match {
                case Array(r, c) =>
                  rss += r.toInt
                  cpu += c.toDouble
                case _ =>
                  SamplingProfiler.warn(s"Unexpected ps output: $line", new MatchError(line), maxPsFailures)
              }
            }
            Elems(childProcessCount -> directCount, childProcessRSS -> rss / 1024, childProcessCPU -> cpu / 100.0)
          } match {
            case Success(value) => value
            case Failure(e) =>
              if (maxPsFailures.getAndDecrement() <= 0) throw new Exception(s"Failure processing ps", e)
              Elems.Nil
          }
        }
      })

    // Diff of all bespoke counters
    ss += new Sampler[Map[Key[Long], Long], Map[Key[Long], Long]](
      sp,
      "Bespoke counters",
      snapper = _ => snapCountersMap,
      process = MINUS,
      publish = diff => Elems(diff.map(Elem(_)).toSeq: _*)
    )

    ss += new Sampler[Map[Key[Long], Long], Map[Key[Long], Long]](
      sp,
      "Bespoke gauges",
      snapper = _ => snapGaugesMap,
      process = LATEST,
      publish = es => Elems(es.map(Elem(_)).toSeq: _*)
    )

    // Internal stats
    ss += new Sampler[Map[String, StatsAccumulator], Map[String, StatsAccumulator]](
      sp,
      "Internal stats",
      snapper = _ => snapStats(),
      process = LATEST,
      publish = stats => {
        val smap: Map[String, Map[String, Double]] = stats.mapValuesNow(_.toMap)
        Elems(spInternalStats -> smap)
      }
    )

    ss.result()
  }
}
