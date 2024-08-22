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
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.graph.DiagnosticSettings
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.SchedulerPlugin
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.CustomRegressionMetrics
import optimus.graph.diagnostics.messages.BookmarkCounter
import optimus.platform.internal.TemporalSource
import optimus.platform.storable.Storable
import optimus.platform.temporalSurface.FixedBranchTemporalContext
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedBranchContext
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedLeafSurface
import optimus.platform.temporalSurface.TemporalSurfaceMatchers
import optimus.platform.relational._
import optimus.platform.relational.namespace.Namespace

object At {
  type GetTimeMap = AsyncFunction2[Set[Partition], Boolean, Map[Partition, Instant]]
  private[this] val log = getLogger[At.type]

  def apply(at: ZonedDateTime): Instant = {
    at.toInstant
  }

  def apply(at: OffsetDateTime): Instant = {
    at.toInstant
  }

  def apply(at: Instant): Instant = {
    at
  }

  private val getNowMap = asAsync { (partitions: Set[Partition], waitForMaxCatchup: Boolean) =>
    {
      val lsqtMap = DAL.resolver.atNow(partitions, waitForMaxCatchup)
      require(
        lsqtMap.nonEmpty,
        s"lsqtMap $lsqtMap is expected to be non empty and contain lsqt for all partitions defined")
      lsqtMap
    }
  }

  private val getLsqtMap = asAsync { (partitions: Set[Partition], waitForMaxCatchup: Boolean) =>
    {
      val lsqtMap = DAL.resolver.atLsqt(partitions, waitForMaxCatchup)

      require(
        lsqtMap.nonEmpty,
        s"lsqtMap $lsqtMap is expected to be non empty and contain lsqt for all partitions defined")
      lsqtMap
    }
  }

  @node def initialTime: Instant = {
    if (EvaluationContext.isThreadRunningGraph) {
      TemporalSource.initialTime
    } else {
      Instant.ofEpochMilli(OptimusApp.startupTime)
    }
  }

  @async private def graphChecksForServerTime[T](f: => T, default: T): T = {
    EvaluationContext.verifyOffGraph(ignoreNonCaching = true, beNice = !optimus.graph.Settings.strictAtNow)
    if (!EvaluationContext.isInitialised) {
      log.error("Calling without initialized EvaluationContext.  This will change to throw an Exception soon.")
      default
    } else f
  }

  // At.now needs to be aware of any writing taking place in any partition even through it returns the LSQT of Default
  // partition.
  @async(exposeArgTypes = true) def now: Instant = defaultPartitionTime(getNowMap, "now", waitForMaxCatchup = true)
  @async(exposeArgTypes = true) def lsqt: Instant = defaultPartitionTime(getLsqtMap, "lsqt", waitForMaxCatchup = false)

  @async(exposeArgTypes = true) private def defaultPartitionTime(
      getTimeMap: GetTimeMap,
      id: String,
      waitForMaxCatchup: Boolean): Instant = {
    val timeMap = getTimeMap(Set(DefaultPartition), waitForMaxCatchup)
    graphChecksForServerTime(
      {
        timeMap.getOrElse(
          DefaultPartition, {
            log.error(
              s"lsqt (At.$id API) for default partition not found in map ${timeMap}. Using minimum server time fetched instead")
            timeMap.values.min
          }
        )
      },
      patch.MilliInstant.now
    )
  }

  @async(exposeArgTypes = true) def nowForType[E <: Storable: Manifest]: Instant = forType(getNowMap)
  @async(exposeArgTypes = true) def lsqtForType[E <: Storable: Manifest]: Instant = forType(getLsqtMap)

  @async(exposeArgTypes = true) private def forType[E <: Storable: Manifest](getTimeMap: GetTimeMap): Instant = {
    val pMap = DAL.resolver.partitionMap
    val partition = pMap.partitionForType(manifest[E].runtimeClass.getName)
    val timeMap = getTimeMap(Set(partition), false)
    graphChecksForServerTime(
      {
        timeMap.getOrElse(
          partition,
          throw new IllegalArgumentException(s"Cannot find partition $partition in map $timeMap fetched from broker."))
      },
      patch.MilliInstant.now
    )
  }

  @async(exposeArgTypes = true) def nowContextGen: AsyncFunction1[Instant, FixedBranchTemporalContext] = {
    val contextGen = buildContext(getNowMap)
    asAsync { vt: Instant => contextGen(Some(vt)) }
  }

  @async(exposeArgTypes = true) def nowContextForVt(vt: Instant): FixedBranchTemporalContext =
    buildContext(getNowMap)(Some(vt))

  @async(exposeArgTypes = true) def nowContext: FixedBranchTemporalContext = buildContext(getNowMap)(None)

  @async(exposeArgTypes = true) def lsqtContextGen: AsyncFunction1[Instant, FixedBranchTemporalContext] = {
    val contextGen = buildContext(getLsqtMap)
    asAsync { vt: Instant => contextGen(Some(vt)) }
  }

  @async(exposeArgTypes = true) def lsqtContextForVt(vt: Instant): FixedBranchTemporalContext =
    buildContext(getLsqtMap)(Some(vt))

  @async(exposeArgTypes = true) def lsqtContext: FixedBranchTemporalContext = buildContext(getLsqtMap)(None)

  @async private def buildContext(
      getTimeMap: GetTimeMap): AsyncFunction1[Option[Instant], FixedBranchTemporalContext] = {

    val timeMap = getTimeMap(DAL.resolver.partitionMap.allPartitions, false)
    def throwOnMissing(partition: Partition) =
      throw new IllegalArgumentException(s"Cannot find partition $partition in map $timeMap fetched from broker.")

    asAsync { vt: Option[Instant] =>
      graphChecksForServerTime(
        {
          val pMap = DAL.resolver.partitionMap
          val surfaces = {
            val defaultLsqt = timeMap.getOrElse(DefaultPartition, throwOnMissing(DefaultPartition))
            val partitionSurfaces = pMap.partitionTypeRefMap.flatMap { case (partition, types) =>
              val lsqt = timeMap.getOrElse(partition, throwOnMissing(partition))
              types.toSeq.map { typeRef =>
                FixedLeafSurface(
                  TemporalSurfaceMatchers.forQuery(from(Namespace(typeRef, true))),
                  vt.getOrElse(lsqt),
                  lsqt)
              }
            }.toList
            partitionSurfaces :+ FixedLeafSurface(TemporalSurfaceMatchers.all, vt.getOrElse(defaultLsqt), defaultLsqt)
          }
          FixedBranchContext(TemporalSurfaceMatchers.allScope, surfaces)
        },
        default = FixedBranchContext(
          TemporalSurfaceMatchers.allScope,
          FixedLeafSurface(
            TemporalSurfaceMatchers.all,
            vt.getOrElse(patch.MilliInstant.now),
            patch.MilliInstant.now) :: Nil)
      )
    }
  }

  private[optimus] def atTracingPlugin(out: (String, IllegalStateException) => Unit, impl: String): SchedulerPlugin =
    new SchedulerPlugin {
      val maxLevel: Int =
        DiagnosticSettings.getIntProperty("optimus.platform.diagnostics.dumpAtNowCallStacksMaxLevel", 25)
      override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
        val exception = new IllegalStateException(s"Touched At.$impl")
        GridProfiler.recordCustomCounter(CustomRegressionMetrics.dalAtNowCustomMetric, 1, node = n)
        BookmarkCounter.report("At.now call")
        out(
          s"Touched At.$impl: ${RuntimeEnvironment.waiterNodeStack(n, Some(maxLevel))}, call chain: ${exception.getStackTrace
              .mkString("[", "; ", "]")}",
          exception
        )
        false
      }
    }

  private def dumpAtNowCallStacks: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.platform.diagnostics.dumpAtNowCallStacks", false)
  private def mockEnvironment: Boolean = EvaluationContext.env.config.runtimeConfig.env == "mock"

  if (dumpAtNowCallStacks && !mockEnvironment) {
    now_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "now"))
    nowForType_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "nowForType"))
    nowContext_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "nowContext"))
  }

  private def dumpAtLsqtCallStacks: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.platform.diagnostics.dumpAtLsqtCallStacks", false)

  if (dumpAtLsqtCallStacks && !mockEnvironment) {
    lsqt_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "lsqt"))
    lsqtForType_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "lsqtForType"))
    lsqtContext_info.setPlugin(atTracingPlugin((msg, ex) => log.warn(msg, ex), "lsqtContext"))
  }
}
