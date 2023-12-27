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
package optimus.buildtool.trace

import java.lang.management.ManagementFactory
import java.time.Instant

import ch.epfl.scala.bsp4j.MessageType
import com.sun.management.ThreadMXBean
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.CrumbLogger
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.CrumbHint
import optimus.breadcrumbs.crumbs.CrumbHints
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.Utils
import optimus.core.ChainedNodeID
import optimus.platform.util.Log
import optimus.platform.util.Version
import optimus.scalacompat.collection._
import optimus.utils.CollectionUtils._

import scala.collection.immutable.Seq
import scala.collection.mutable

private[buildtool] object ObtCrumbSource extends Crumb.Source {
  override val name = "OBT"
}

final case class Progress(name: String, time: Instant, progress: Double)

trait StatsHolder extends ObtStatsHolder {
  private val stats = mutable.HashMap.empty[ObtStat, Long]
  private[trace] def getStats = synchronized(stats.toMap)
  private[trace] def getStat(stat: ObtStat) = synchronized(stats.get(stat))

  override def setStat(obtStat: ObtStat, value: Long): Unit = synchronized {
    stats += obtStat -> value
  }
  override def addToStat(obtStat: ObtStat, value: Long): Unit = synchronized {
    stats += (obtStat -> (stats.get(obtStat) match {
      case Some(v0) => v0 + value
      case _        => value
    }))
  }
  override def supportsStats: Boolean = true
}

private[buildtool] class BreadcrumbTrace(
    descriptor: Descriptor,
    scopeId: ScopeId,
    category: CategoryTrace,
    startTime: Instant)
    extends StatisticsTrace(scopeId, category, startTime)
    with StatsHolder {
  import optimus.breadcrumbs.crumbs.Properties._

  private val id = ChainedNodeID.nodeID

  import optimus.breadcrumbs.CrumbLogger

  override def publishMessages(messages: Seq[CompilationMessage]): Unit =
    CrumbLogger.debug(ObtCrumbSource, s"Messages: $messages")

  override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = {
    super.end(success = success, errors = errors, warnings = warnings, time = time)
    // Note: Requires setting -Dbreadcrumb.level=DEBUG
    Breadcrumbs.debug(
      id,
      PropertiesCrumb(
        _,
        ObtCrumbSource,
        obtCategory -> category.toString,
        descriptor.property,
        obtScope -> scopeId.toString,
        obtProgresses -> progresses.map(p => (p.time, p.name, p.progress)),
        obtStats -> getStats.map { case (k, v) => k.key -> v },
        obtStart -> startTime,
        obtEnd -> time
      )
    )
  }
}

private[buildtool] class StatisticsTrace(val scopeId: ScopeId, val category: CategoryTrace, val startTime: Instant)
    extends DefaultTaskTrace
    with StatsHolder {
  @volatile private var _finishTime: Instant = Instant.MAX
  private[trace] def finishTime = _finishTime
  protected val progresses = mutable.ArrayBuffer[Progress]()
  private[trace] def getProgresses = synchronized(progresses.toIndexedSeq)
  private val startBytesAllocated: Long = if (category.isSingleThreaded) MemoryBean.getThreadAllocatedBytes() else -1

  override def reportProgress(message: String, progress: Double = -1.0): Unit = {
    progresses += Progress(message.replace(' ', '_'), patch.MilliInstant.now, progress)
  }
  override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = {
    reportProgress("end")
    _finishTime = time
    setStat(ObtStats.DurationMs, time.toEpochMilli - startTime.toEpochMilli)
    setStat(ObtStats.Success, if (success) 1 else 0)
    setStat(ObtStats.Errors, errors)
    setStat(ObtStats.Warnings, warnings)
    // we can only meaningfully measure allocations if the category runs on a single thread
    if (category.isSingleThreaded)
      setStat(ObtStats.AllocatedBytes, MemoryBean.getThreadAllocatedBytes() - startBytesAllocated)
  }
}

object BreadcrumbTraceListener extends Log {
  def apply(
      crumbs: Boolean,
      workspaceRoot: Directory,
      buildId: String,
      sendSummaryToLongTermIndex: Boolean,
      sendPerScopeStats: Boolean)(prev: List[ObtTraceListener]): Option[BreadcrumbTraceListener] =
    create(
      crumbs,
      workspaceRoot,
      BuildId(buildId),
      sendSummaryToLongTermIndex = sendSummaryToLongTermIndex,
      sendPerScopeStats = sendPerScopeStats)(prev)

  def forSync(crumbs: Boolean, workspaceRoot: Directory, category: SyncTrace)(
      prev: List[ObtTraceListener]): Option[BreadcrumbTraceListener] =
    BreadcrumbTraceListener.create(
      crumbs,
      workspaceRoot,
      Sync(category),
      sendSummaryToLongTermIndex = false,
      sendPerScopeStats = false)(prev)

  def forBuild(crumbs: Boolean, workspaceRoot: Directory, buildId: String)(
      prev: List[ObtTraceListener]): Option[ObtTraceListener] =
    BreadcrumbTraceListener(
      crumbs,
      workspaceRoot,
      buildId,
      sendSummaryToLongTermIndex = false,
      sendPerScopeStats = false)(prev)

  private def create(
      crumbs: Boolean,
      workspaceRoot: Directory,
      descriptor: Descriptor,
      sendSummaryToLongTermIndex: Boolean,
      sendPerScopeStats: Boolean)(prev: List[ObtTraceListener]): Option[BreadcrumbTraceListener] = {
    if (crumbs) {
      val msg = descriptor match {
        case BuildId(id)        => Some(s"Breadcrumbs will be published with rootUuid ${ChainedID.root}, buildId $id")
        case Sync(BuildTargets) => Some(s"Breadcrumbs will be published with rootUuid ${ChainedID.root}")
        case _                  => None
      }
      msg.foreach { m =>
        log.debug(m)
        prev.foreach(_.info(m))
      }

      Some(
        new BreadcrumbTraceListener(
          workspaceRoot,
          descriptor,
          sendSummaryToLongTermIndex = sendSummaryToLongTermIndex,
          sendPerScopeStats = sendPerScopeStats))
    } else None
  }
}

sealed trait Descriptor {
  def property: Properties.Elem[String]
  def prefix: String
}
final case class BuildId(id: String) extends Descriptor {
  override def property: Properties.Elem[String] = Properties.obtBuildId -> id
  def prefix: String = "Build"
}
final case class Sync(category: SyncTrace) extends Descriptor {
  override def property: Properties.Elem[String] = Properties.description -> category.toString
  def prefix: String = "Sync"
}

final class BreadcrumbTraceListener(
    workspaceRoot: Directory,
    descriptor: Descriptor,
    sendSummaryToLongTermIndex: Boolean,
    sendPerScopeStats: Boolean,
    t0: Instant = patch.MilliInstant.now())
    extends StatisticsTraceListener {
  import optimus.breadcrumbs.crumbs.Properties._

  private val moreProperties = mutable.ArrayBuffer[Properties.Elem[_]]()

  override def setProperty[T](p: EnumeratedKey[T], v: T): Unit = synchronized {
    moreProperties += p -> v
  }

  override protected def newTrace(scopeId: ScopeId, category: CategoryTrace, time: Instant): StatisticsTrace =
    new BreadcrumbTrace(descriptor, scopeId, category, time)

  override def logMsg(msg: String, tpe: MessageType): Unit = {
    CrumbLogger.debug(ObtCrumbSource, s"$tpe:$msg")
  }

  override def endBuild(success: Boolean): Boolean = {
    super.endBuild(success)
    publishStats()
    true
  }

  def publishStats(): Unit = {
    val coreFields = Seq(
      descriptor.property,
      Properties.installVersion -> Version.version,
      Properties.user -> sys.props.getOrElse("user.name", "unknown"),
      Properties.sysLoc -> sys.env.getOrElse("SYS_LOC", "unknown")
    )

    val id = ChainedNodeID.nodeID
    val stats = buildStatistics

    def publish(category: String, longTerm: Boolean, elems: Properties.Elem[_]*): Unit = {
      val hints = if (longTerm) CrumbHints.LongTerm else Set.empty[CrumbHint]
      Breadcrumbs.info(
        id,
        PropertiesCrumb(
          _,
          ObtCrumbSource,
          hints,
          Seq(obtCategory -> s"${descriptor.prefix}$category") ++ coreFields ++ elems: _*
        )
      )
    }

    publish(
      "Summary",
      longTerm = sendSummaryToLongTermIndex,
      Seq(
        obtScope -> RootScopeId.toString,
        obtWorkspace -> workspaceRoot.pathString,
        obtStats -> stats.stats,
        obtStatsByCategory -> stats.statsByCategory
      ) ++
        moreProperties: _*
    )

    if (stats.durationByCategory.nonEmpty) {
      val timingProperties = Seq(obtStart -> stats.start, obtDurationByCategory -> stats.durationByCategory) ++
        (if (stats.wallTime.toMillis > 0) Seq(obtEnd -> stats.end, obtWallTime -> stats.wallTime) else Nil)
      publish(
        "Timings",
        longTerm = sendSummaryToLongTermIndex,
        timingProperties: _*
      )
    }

    if (stats.durationByScenario.nonEmpty) {
      val backgroundProperties = Seq(obtStart -> stats.start, obtDurationByScenario -> stats.durationByScenario) ++
        (if (stats.wallTime.toMillis > 0) Seq(obtEnd -> stats.end, obtWallTime -> stats.wallTime) else Nil)
      publish(
        "BackgroundTimings",
        longTerm = sendSummaryToLongTermIndex,
        backgroundProperties: _*
      )
    }

    if (sendPerScopeStats) {
      // these stats are very detailed (there are >1400 scopes) so we never publish them to long-term splunk
      val scopes = stats.statsByScopeAndCategory.keySet union stats.statsByScope.keySet union
        stats.durationByScopeAndPhase.keySet
      scopes.foreach { scope =>
        publish(
          "SummaryByScope",
          longTerm = false,
          obtScope -> scope.toString,
          obtWorkspace -> workspaceRoot.pathString,
          // in practice if we have either we will have both, but defensively we will say getOrElse
          obtStats -> stats.statsByScope.getOrElse(scope, Map.empty),
          obtStatsByCategory -> stats.statsByScopeAndCategory.getOrElse(scope, Map.empty)
        )

        publish(
          "PhasesByScope",
          longTerm = false,
          obtScope -> scope.toString,
          obtDurationByPhase -> stats.durationByScopeAndPhase.getOrElse(scope, Map.empty)
        )
      }
    }

    if (descriptor.isInstanceOf[BuildId]) {
      val durationCentiles = stats.durationCentilesByCategory.map { case (cat, cs) => cat -> cs.map(_._2) }
      publish(
        "TimingCentiles",
        longTerm = false,
        obtDurationCentilesByCategory -> durationCentiles
      )
      publish(
        "Phases",
        longTerm = sendSummaryToLongTermIndex,
        obtDurationByPhase -> stats.durationByPhase
      )
      val phaseCentiles = stats.durationCentilesByPhase.map { case (cat, cs) => cat -> cs.map(_._2) }
      publish(
        "PhaseCentiles",
        longTerm = false,
        obtDurationCentilesByPhase -> phaseCentiles
      )
    }

    publish(
      "Failures",
      longTerm = false,
      obtFailuresByCategory -> stats.failuresByCategory,
      obtErrorsByCategory -> stats.errorsByCategory,
      obtWarningsByCategory -> stats.warningsByCategory
    )
  }
}

sealed class StatisticsTraceListener(t0: Instant = patch.MilliInstant.now())
    extends DefaultObtTraceListener
    with StatsHolder {
  private val traces = mutable.ArrayBuffer[StatisticsTrace]()
  private var t1: Instant = _

  protected def newTrace(scopeId: ScopeId, category: CategoryTrace, time: Instant): StatisticsTrace =
    new StatisticsTrace(scopeId, category, time)

  final override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace = synchronized {
    val trace = newTrace(scopeId, category, time)
    traces += trace
    trace
  }

  override def endBuild(success: Boolean): Boolean = {
    t1 = patch.MilliInstant.now()
    true
  }

  final def buildStatistics: BuildStatistics = {
    def sumStats(statTraces: Seq[StatisticsTrace]): Map[String, Long] = {
      val allStats = statTraces.flatMap(_.getStats).toGroupedMap
      allStats.map { case (s, vs) => s.key -> vs.sum }
    }

    val listenerStats = getStats.map { case (s, v) => s.key -> v }
    val tracesSeq = traces.toIndexedSeq
    val allStats = listenerStats ++ sumStats(tracesSeq)

    val tracesByCategory = tracesSeq.groupBy(_.category.categoryName)

    val tracesByScenario = tracesSeq.groupBy(st => st.category.scenario(st.scopeId))

    val durationByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> Utils.stats(ts.flatMap(_.getStat(ObtStats.DurationMs)))
    }

    val durationByScenario = tracesByScenario.collect { case (Some(cat), ts) =>
      cat -> Utils.stats(ts.flatMap(_.getStat(ObtStats.DurationMs)))
    }

    val durationCentilesByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> Utils.centiles(ts.flatMap(_.getStat(ObtStats.DurationMs)))
    }

    val failuresByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> ts.count(_.getStat(ObtStats.Success).contains(0))
    }

    val errorsByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> ts.flatMap(_.getStat(ObtStats.Errors)).sum
    }

    val warningsByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> ts.flatMap(_.getStat(ObtStats.Warnings)).sum
    }

    val statsByCategory = tracesByCategory.map { case (cat, ts) =>
      cat -> sumStats(ts)
    }

    val statsByScopeAndCategory =
      tracesSeq.groupBy(_.scopeId).mapValuesNow(_.groupBy(_.category.categoryName).mapValuesNow(sumStats))

    val statsByScope = tracesSeq.groupBy(_.scopeId).mapValuesNow(sumStats)

    def progressDurations(trace: StatisticsTrace): Seq[(String, Long)] = synchronized {
      val ps = trace.getProgresses
      if (ps.isEmpty) Seq.empty
      else
        ps.dropRight(1).zip(ps.tail).map { case (Progress(desc, t, _), Progress(_, tNext, _)) =>
          desc -> Math.max(tNext.toEpochMilli - t.toEpochMilli, 0)
        }
    }

    val tracesByPhase = tracesSeq.flatMap(progressDurations).toGroupedMap

    val durationByPhase = tracesByPhase.map { case (phase, durations) =>
      phase -> Utils.stats(durations.toIndexedSeq)
    }

    val durationByScopeAndPhase =
      tracesSeq.groupBy(_.scopeId).mapValuesNow(_.flatMap(progressDurations).toGroupedMap.mapValuesNow(Utils.stats))

    val durationCentilesByPhase = tracesByPhase.map { case (phase, durations) =>
      phase -> Utils.centiles(durations.toIndexedSeq)
    }

    BuildStatistics(
      start = t0,
      end = if (t1 != null) t1 else t0,
      durationByCategory = durationByCategory,
      durationByScenario = durationByScenario,
      durationCentilesByCategory = durationCentilesByCategory,
      failuresByCategory = failuresByCategory,
      errorsByCategory = errorsByCategory,
      warningsByCategory = warningsByCategory,
      durationByPhase = durationByPhase,
      durationByScopeAndPhase = durationByScopeAndPhase,
      durationCentilesByPhase = durationCentilesByPhase,
      stats = allStats,
      statsByCategory = statsByCategory,
      statsByScope = statsByScope,
      statsByScopeAndCategory = statsByScopeAndCategory
    )
  }
}

private[trace] object MemoryBean {
  private val mx = ManagementFactory.getThreadMXBean
    .asInstanceOf[ThreadMXBean]

  def getThreadAllocatedBytes(): Long = mx.getThreadAllocatedBytes(Thread.currentThread().getId)
}
