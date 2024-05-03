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
import optimus.breadcrumbs.crumbs.Properties.Key
import optimus.graph.diagnostics.DefensiveManagementFactory
import optimus.graph.diagnostics.sampling.SamplingProfiler.SamplerTrait
import optimus.graph.diagnostics.sampling.SamplingProfiler._
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.breadcrumbs.crumbs.Properties._

import scala.jdk.CollectionConverters._
import sun.management.ManagementFactoryHelper

import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryUsage
import java.{util => jutil}
import scala.collection.mutable.ArrayBuffer

object BaseSamplers {
  final class Util(sp: SamplingProfiler) {
    object Snap {
      def apply[N](snapper: Boolean => N, key: Key[N]): Sampler[N, N] = new Sampler(sp, snapper, LATEST[N], PUB(key))
    }
    // Simple snap, publishing difference from previous numeric value to a single property
    def Diff[N: Numeric](snapper: Boolean => N, key: Key[N]): Sampler[N, N] = {
      new Sampler(sp, snapper, MINUS, PUB(key))
    }
  }
}

//noinspection ScalaUnusedSymbol // ServiceLoader
class BaseSamplers extends SamplerProvider {
  import BaseSamplers._

  override val priority: Int = 0

  private val osBean: OperatingSystemMXBean = DefensiveManagementFactory.getOperatingSystemMXBean()

  def provide(sp: SamplingProfiler): Seq[SamplerTrait[_, _]] = {
    val util = new Util(sp)
    import util._

    val ss = ArrayBuffer.empty[SamplerTrait[_, _]]
    // Various OS bean stats
    ss += Diff(_ => osBean.getProcessCpuTime / NANOSPERMILLI, profJvmCPUTime)
    ss += Snap(_ => osBean.getSystemCpuLoad, profSysCPULoad)
    ss += Snap(_ => osBean.getProcessCpuLoad, profJvmCPULoad)
    ss += Snap(_ => osBean.getSystemLoadAverage, profLoadAvg)
    ss += Snap(_ => osBean.getCommittedVirtualMemorySize / MILLION, profCommitedMB)

    Option(ManagementFactory.getCompilationMXBean).filter(_.isCompilationTimeMonitoringSupported).foreach { bean =>
      ss += Diff(_ => bean.getTotalCompilationTime, profJitTime)
    }

    val classLoadBean = ManagementFactoryHelper.getHotspotClassLoadingMBean
    ss += Diff(_ => classLoadBean.getClassLoadingTime, profClTime)

    // Total time spent in collections during epoch, according to gcBeans
    val gcBeans: jutil.List[GarbageCollectorMXBean] = ManagementFactory.getGarbageCollectorMXBeans
    ss += Diff[Long](_ => gcBeans.asScala.map(_.getCollectionTime * ALREADYMILLIS).sum, profGcTimeAll)

    val memBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    ss += new Sampler[MemoryUsage, MemoryUsage](
      sp,
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
      snapper = _ => memBean.getNonHeapMemoryUsage,
      process = LATEST,
      publish = (usage: MemoryUsage) =>
        Elems(
          profCurrNonHeap -> usage.getUsed / MILLION,
          profMaxNonHeap -> usage.getMax / MILLION
        )
    )

    ss += Diff(_ => StackAnalysis.numStacksPublished, numStacksPublished)
    ss += Snap(_ => memBean.getObjectPendingFinalizationCount, gcFinalizerCount)

    ss
  }

}
