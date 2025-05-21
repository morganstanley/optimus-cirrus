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
package optimus.graph.diagnostics.gridprofiler

import com.opencsv.CSVWriter
import optimus.core.needsPlugin
import optimus.dist.DistPluginTags.JobTaskIdTag
import optimus.dist.gsfclient.JobClientMetrics
import optimus.graph.DiagnosticSettings.initialProfileAggregation
import optimus.graph.DiagnosticSettings.initialProfileCustomFilter
import optimus.graph.OGSchedulerLostConcurrency.CriticalSyncStack
import optimus.graph.OGSchedulerLostConcurrency.NonConcurrentLoopDesc
import optimus.graph.OGSchedulerLostConcurrency.UniqueCounts
import optimus.graph._
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.SchedulerProfileEntry
import optimus.graph.diagnostics.configmetrics.ConfigMetrics
import optimus.graph.diagnostics.gridprofiler
import optimus.graph.diagnostics.gridprofiler.GridProfiler.DALStatsEntry
import optimus.graph.diagnostics.gridprofiler.GridProfilerData.MetricData
import optimus.graph.diagnostics.gridprofiler.GridProfilerData.nameToTweakID
import optimus.graph.diagnostics.gridprofiler.GridProfilerDefaults._
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils._
import optimus.graph.diagnostics.gridprofiler.Level._
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.EvaluationContext
import optimus.platform.PluginHelpers.toNode
import optimus.platform.ForwardingPluginTagKey
import optimus.platform.PluginTagKeyValue
import optimus.platform.ScenarioStack
import optimus.platform.annotations._
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager
import optimus.utils.MacroUtils.SourceLocation
import org.slf4j.LoggerFactory

import java.io._
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.{util => jutil}
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent => c}
import scala.collection.{mutable => m}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait Monoid[M] {
  def zero: M
  final def append(t1: M, t2: M): M = GridProfiler.defaultCombOp(t1, t2)
  final def sum(ms: Iterable[M]): M = ms.foldLeft(zero)(append)
}

object Monoid {
  implicit object LongMonoid extends Monoid[Long] {
    override def zero: Long = 0L
  }
  implicit object CriticalSyncStackMonoid extends Monoid[CriticalSyncStack] {
    override val zero = new CriticalSyncStack()
  }
  implicit object NonConcurrentLoopDescMonoid extends Monoid[NonConcurrentLoopDesc] {
    override val zero = new NonConcurrentLoopDesc()
  }
  implicit val PNodeTaskInfoMonoid: Monoid[PNodeTaskInfo] = new Monoid[PNodeTaskInfo] {
    override val zero = new PNodeTaskInfo(0)
  }
  implicit val SchedulerProfilerEntryMonoid: Monoid[SchedulerProfileEntry] = new Monoid[SchedulerProfileEntry] {
    override def zero: SchedulerProfileEntry = SchedulerProfileEntry()
  }
  implicit val StringMonoid: Monoid[String] = new Monoid[String] {
    override def zero: String = ""
  }
  implicit def SetMonoid[X]: Monoid[Set[X]] = new Monoid[Set[X]] {
    override def zero: Set[X] = Set.empty[X]
  }
  implicit def MapMonoid[K, V]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    override def zero: Map[K, V] = Map.empty
  }
  implicit object DalStatsMonoid extends Monoid[DALStatsEntry] {
    override def zero: DALStatsEntry = DALStatsEntry.empty
  }
  implicit object ProcessMetricsEntryMonoid extends Monoid[ProcessMetricsEntry] {
    override def zero: ProcessMetricsEntry = ProcessMetricsEntry.empty
  }
}

/**
 * Grid profiling: records profiling information at several levels of detail and intrusiveness on the grid sample usage:
 *
 * import optimus.graph.diagnostics.gridprofiler.GridProfiler.profiled import
 * optimus.graph.diagnostics.gridprofiler.GridProfilerLevel
 *
 * // 1. simplest usage (no code changes): pass --profile-graph and --profile-csvfolder to OptimusAPp
 *
 * // 1. scope-level customization: profiled(GridProfiler.LIGHT) { // calcs happen here, may be distributed, nested
 * profile() blocks may be used }.printSummary(System.out) // or use other accessors on ProfilerResult
 *
 * // 2. named scopes (aggregation will combine metrics for identical scopes) val p = profiled(GridProfiler.LIGHT, scope
 * \= "pv") { // calcs happen here, may be distributed, nested profile() blocks may be used }
 */
object GridProfiler {

  import Monoid._

  private[diagnostics] val log = LoggerFactory.getLogger(GridProfiler.getClass)

  /**
   * Enable profiling based on system properties set for test runners (OptimusTestRunner, SimpleSmokeDSLJUnitRunner) and
   * log summary/dump output files after run completes. Note that this calls reset first!
   * -Doptimus.scheduler.profile.aggregation
   * -Doptimus.scheduler.profile.filters.custom
   * -Doptimus.scheduler.profile.folder
   * -Doptimus.scheduler.profile.mode
   */
  @closuresEnterGraph def profilingOnShutdown(f: => Unit)(implicit source: SourceLocation): Unit = {
    val agg = AggregationType.withNameOrDefault(initialProfileAggregation)
    val custom = if (initialProfileCustomFilter eq null) Array.empty[String] else initialProfileCustomFilter
    setNonNodeInputDefaults(agg, custom, "")
    try f
    finally logSummary(onShutdown = true, source = s"$source:profilingOnShutdown")
  }

  // Called to ensure that class is fully initialised
  def ensureLoaded(): Unit = ProcessMetricsEntry.ensureLoaded()

  // VM options override commandline/API supplied level
  type Level = Level.Level
  def setupProfiling(lvl: Level): Unit = GraphInputConfiguration.setTraceMode(lvl.toString)

  private[optimus] def disable(): Unit =
    setupProfiling(Level.NONE)

  def teardownForLocalRecursion(depth: Int): Unit =
    if (depth != 0)
      // local recursion node ends; pop metrics
      ProcessMetricsEntry.pop(depth)

  def setupForLocalRecursion(depth: Int, task: String, engine: String): Unit =
    if (depth != 0)
      // local recursion mode starts: push metrics
      ProcessMetricsEntry.push(depth, task, engine)

  private def parentOrDefaultCustomMetrics =
    Option(OGSchedulerContext.current) flatMap (_.scenarioStack
      .findPluginTag(GridProfiler.Tag)) map (_.customMetricsFilter) getOrElse defaultCustomMetrics
  private def parentOrDefaultAggregation =
    Option(OGSchedulerContext.current) flatMap (_.scenarioStack
      .findPluginTag(GridProfiler.Tag)) map (_.aggregation) getOrElse defaultAggregationType

  // An Option[Tag] is sent out along with every distributed node
  final case class Tag(
      scope: Seq[
        ProfilerScope
      ], // scope name for aggregating custom (so far) profile data from the child nodes
      customMetricsFilter: Array[String], // included custom metrics
      aggregation: AggregationType.Value, // tells the grid how to aggregate
  )
  object Tag extends ForwardingPluginTagKey[Tag]

  final case class TimeAndCount(timeNanos: Long, count: Int) {
    override def toString: String = f"${timeNanos / 1000000}%,dms ($count%,d calls)"
  }

  final case class MaxValue(value: Long)

  def getDefaultLevel: gridprofiler.Level.Value = ProcessGraphInputs.ProfileLevel.currentValueOrThrow()
  def getDefaultCSVFolder: String = ProcessGraphInputs.ProfilerCsvFolder.currentValueOrThrow()
  def getDefaultAggregation: gridprofiler.AggregationType.Value = defaultAggregationType
  def getDefaultAggregationName: String = getDefaultAggregation match {
    case AggregationType.NONE            => "NONE"
    case AggregationType.ENGINE          => "ENGINE"
    case AggregationType.DEPTH           => "DEPTH"
    case AggregationType.CLIENTANDREMOTE => "CLIENTANDREMOTE"
    case AggregationType.AGGREGATED      => "AGGREGATED"
  }
  def getDefaultCustomMetrics: Array[String] = defaultCustomMetrics
  def getDefaultConfigFolder: String = defaultConfigFolder

  // noinspection ScalaUnusedSymbol
// The profiled block turns on grid profiling for all @nodes within the action block.
  // These blocks may be nested.
  //
  // The following syntax options are supported:
  // Basic use: current or default level of intrusiveness and scope name.
  // profiled { ... }
  // profiled() { ... }
  // Specify level of intrusiveness, such as to enter a scope with hotspots profiling when the enclosing block or commandline argument specifies non-intrusive light mode
  // profiled(level) { ... }
  // Specify scope name, such as when collecting custom metrics grouped by some higher-level key, such as book name
  // profiled(scope) { ... } or profiled(scope = "pv") { ... }
  // Specify both scope name and level of intrusiveness
  // profiled(level, scope) { ... } or profiled(level=LIGHT, scope="abc") { ... }
  //
  @nodeSync
  @nodeSyncLift
  def profiled[T](scope: ProfilerScope)(@nodeLiftByName @nodeLift action: => T): ProfilerResult[T] =
    needsPlugin
  def profiled$withNode[T](scope: ProfilerScope)(action: Node[T]): ProfilerResult[T] =
    profiled$newNode(scope, Some(ProcessMetricsEntry.start()))(action).get
  def profiled$queued[T](scope: ProfilerScope)(action: Node[T]): Node[ProfilerResult[T]] =
    profiled$newNode(scope, None)(action).enqueueAttached
  private[this] def profiled$newNode[T](scope: ProfilerScope, envMetrics: Option[ProcessMetricsBeginSnapshot])(
      action: Node[T]): Node[ProfilerResult[T]] = {
    profiled$newNode[T](scope, parentOrDefaultCustomMetrics, parentOrDefaultAggregation, envMetrics)(action)
  }

  // noinspection ScalaUnusedSymbol
  @nodeSync
  @nodeSyncLift
  def profiled[T](scope: ProfilerScope, customFilter: Array[String], aggregation: AggregationType.Value)(
      @nodeLiftByName @nodeLift action: => T): ProfilerResult[T] =
    needsPlugin
  def profiled$withNode[T](scope: ProfilerScope, customFilter: Array[String], aggregation: AggregationType.Value)(
      action: Node[T]): ProfilerResult[T] =
    profiled$newNode(scope, customFilter, aggregation, Some(ProcessMetricsEntry.start()))(action).get
  def profiled$queued[T](scope: ProfilerScope, customFilter: Array[String], aggregation: AggregationType.Value)(
      action: Node[T]): Node[ProfilerResult[T]] =
    profiled$newNode(scope, customFilter, aggregation, None)(action).enqueueAttached
  private[this] def profiled$newNode[T](
      scope: ProfilerScope,
      customFilter: Array[String],
      aggregation: AggregationType.Value,
      envMetrics: Option[ProcessMetricsBeginSnapshot])(action: Node[T]) = {
    val node = new ProfilerResultNode(action, EvaluationContext.cancelScope.childScope(), envMetrics)
    node.attach(makeSSWithTag(scope, customFilter, aggregation))
    node
  }
  @nodeSync
  @nodeSyncLift
  def profiled[T](
      scope: Option[ProfilerScope] = None,
      customFilter: Option[Array[String]] = None,
      aggregation: Option[AggregationType.Value] = None)(@nodeLiftByName @nodeLift action: => T)(implicit
      loc: SourceLocation): ProfilerResult[T] =
    needsPlugin
  def profiled$withNode[T](
      scope: Option[ProfilerScope] = None,
      customFilter: Option[Array[String]] = None,
      aggregation: Option[AggregationType.Value] = None)(action: Node[T])(implicit
      loc: SourceLocation): ProfilerResult[T] =
    profiled$newNode(
      scope.getOrElse(scopeFromLocation(loc)),
      parentOrDefaultCustomMetrics ++ customFilter.getOrElse(Array.empty[String]),
      (aggregation.toSeq :+ parentOrDefaultAggregation).minBy(_.id),
      Some(ProcessMetricsEntry.start())
    )(action).get
  def profiled$queued[T](
      scope: Option[ProfilerScope] = None,
      customFilter: Option[Array[String]] = None,
      aggregation: Option[AggregationType.Value] = None)(action: Node[T])(implicit
      loc: SourceLocation): Node[ProfilerResult[T]] =
    profiled$newNode(
      scope.getOrElse(scopeFromLocation(loc)),
      parentOrDefaultCustomMetrics ++ customFilter.getOrElse(Array.empty[String]),
      (aggregation.toSeq :+ parentOrDefaultAggregation).minBy(_.id),
      Some(ProcessMetricsEntry.start())
    )(action).enqueueAttached
  // noinspection ScalaUnusedSymbol
  @nodeSync
  @nodeSyncLift
  def profiled[T](scope: String)(@nodeLiftByName @nodeLift action: => T): ProfilerResult[T] =
    profiled$withNode(scope)(toNode(action _))
  def profiled$withNode[T](scope: String)(action: Node[T]): ProfilerResult[T] =
    profiled$newNode(NamedProfilerScope(scope), Some(ProcessMetricsEntry.start()))(action).get
  def profiled$queued[T](scope: String)(action: Node[T]): Node[ProfilerResult[T]] =
    profiled$newNode(NamedProfilerScope(scope), None)(action).enqueueAttached
  def profiled$queued[T](scope: String, action: => T): NodeFuture[ProfilerResult[T]] =
    profiled$newNode(NamedProfilerScope(scope), None)(toNode(action _)).enqueueAttached
  // noinspection ScalaUnusedSymbol
  @nodeSync
  @nodeSyncLift
  def profiled[T](@nodeLiftByName @nodeLift action: => T)(implicit loc: SourceLocation): ProfilerResult[T] =
    profiled$withNode(toNode(action _))(loc)
  def profiled$withNode[T](action: Node[T])(implicit loc: SourceLocation): ProfilerResult[T] =
    profiled$newNode(scopeFromLocation(loc), Some(ProcessMetricsEntry.start()))(action).get
  def profiled$queued[T](action: Node[T])(implicit loc: SourceLocation): Node[ProfilerResult[T]] =
    profiled$newNode(scopeFromLocation(loc), None)(action).enqueueAttached
  // noinspection ScalaUnusedSymbol
  @nodeSync
  @nodeSyncLift
  def profiled[T](unused: Int*)(@nodeLiftByName @nodeLift action: => T)(implicit
      loc: SourceLocation): ProfilerResult[T] = profiled$withNode(toNode(action _))(loc)
  // noinspection ScalaUnusedSymbol
  def profiled$withNode[T](unused: Int*)(action: Node[T])(implicit loc: SourceLocation): ProfilerResult[T] =
    profiled$newNode(scopeFromLocation(loc), Some(ProcessMetricsEntry.start()))(action).get
  // noinspection ScalaUnusedSymbol
  def profiled$queued[T](unused: Int*)(action: Node[T])(implicit loc: SourceLocation): Node[ProfilerResult[T]] =
    profiled$newNode(scopeFromLocation(loc), None)(action).enqueueAttached

  // scope is Seq[String] to represent nesting of profiled blocks:
  // profiled("A") { profiled("B") { node } } will execute node under scope (A,B)

  val globalScope = Seq.empty[ProfilerScope]
  // A wrapper around a pair of map that maintains bidirectional, thread-safe hash maps
  private object scopeRegistry {
    private val scope2blockId = mutable.HashMap.empty[Seq[ProfilerScope], Int]
    private val blockId2Scope = mutable.ArrayBuffer.empty[Seq[ProfilerScope]]

    // the global scope is an empty Seq, and its id is 1 (BLOCK_ID_UNSCOPED)
    require(putIfAbsent(globalScope) == OGTrace.BLOCK_ID_UNSCOPED)

    def putIfAbsent(scope: Seq[ProfilerScope]): Int = synchronized {
      scope2blockId.getOrElseUpdate(
        scope, {
          val index = blockId2Scope.size + OGTrace.BLOCK_ID_UNSCOPED
          blockId2Scope += scope
          index
        })
    }

    def getByID(blk: Int): Option[Seq[ProfilerScope]] = synchronized {
      val i = blk - OGTrace.BLOCK_ID_UNSCOPED
      if (i < blockId2Scope.size) Some(blockId2Scope(i))
      else None
    }

    def toList: Seq[(Seq[ProfilerScope], Int)] = synchronized { scope2blockId.toList }
  }
  def getScopeRegistry: Seq[(Seq[ProfilerScope], Int)] = scopeRegistry.toList
  def scopeToBlockID(scope: Seq[ProfilerScope]): Int = scopeRegistry.putIfAbsent(scope)

  // used when preparing a human-readable scope name, otherwise all scopes are handled as integers
  def blockIDToScope(blk: Int): Option[Seq[ProfilerScope]] = scopeRegistry.getByID(blk)

  // named scope is inherited if empty.
  // named scopes are combined with non-empty parent named scope
  // profiler level is inherited if empty, and if there is nothing to inherit from, the default value (set by command line) is used, see getDefaultLevel
  // hotspot filters do the same thing as the level
  private def makeSSWithTag(
      scope: ProfilerScope,
      customFilter: Array[String],
      aggregation: AggregationType.Value): ScenarioStack = {
    val ss = EvaluationContext.scenarioStack
    val newTag = ss.findPluginTag(GridProfiler.Tag) match {
      case Some(t) =>
        Tag(
          t.scope :+ scope,
          customFilter,
          aggregation,
        )
      case None =>
        Tag(Seq(scope), customFilter, aggregation)
    }
    val blk = scopeToBlockID(newTag.scope)
    ss.withBlockID(blk).withPluginTag(GridProfiler.Tag, newTag)
  }
  private final val scopeFromLocationRegex = "\\.\\$\\w*".r
  private final def scopeFromLocation(loc: SourceLocation) =
    NamedProfilerScope(scopeFromLocationRegex.replaceAllIn(loc.method, "") + '_' + loc.line) // drop ".$impl" etc

  final def tagForNode(node: NodeTask): Option[Tag] =
    for {
      ntsk <- Option(node)
      ss <- Option(ntsk.scenarioStack())
      tag <- ss.findPluginTag(GridProfiler.Tag)
    } yield tag

  final def tagStringForNode(node: NodeTask): Option[String] =
    for {
      tag <- tagForNode(node) if tag.scope.nonEmpty
    } yield tag.scope.map(_.toString).mkString("|")

  final def scopeForNode(node: NodeTask): Int = node.profileBlockID()

  // In distribution (where JobIdTag is set), this will return the distribution id which is required to attribute all metrics to the
  // current dist job (and not to a suspended dist job which may have been suspended halfway through a profiled block with the same id/scope).
  // node can be null when recording a scheduler profile on thread exit
  final def jobIdForNode(n: NodeTask): String =
    Option(n)
      .flatMap(n => Option(n.scenarioStack))
      .flatMap(_.findPluginTag(JobTaskIdTag))
      .map(_.gridProfilerID)
      .getOrElse(clientKey)

  // convenience accessors:
  // true if any profiling is on (used by Distribution to reset/save/restore profiling mode)
  final def profilingOn: Boolean = Settings.profileTemporalContext || getDefaultLevel != Level.NONE
  // DAL recording takes place at "Light" or higher
  final def DALProfilingOn: Boolean = getDefaultLevel >= LIGHT

  // If the node has no profiler tag, imbues the node with a synthetized profiler tag to represent commandline-selected options
  // Called before distributing a node to another JVM, where defaultLevel will not be visible
  final def setDefaultTagIfNeeded(n: NodeTask): Unit = {
    val profilerTagNeeded = tagForNode(n).isEmpty && getDefaultLevel != Level.NONE

    val tags =
      if (profilerTagNeeded)
        Seq(PluginTagKeyValue(GridProfiler.Tag, Tag(globalScope, defaultCustomMetrics, defaultAggregationType)))
      else Seq.empty
    if (tags.nonEmpty)
      n.replace(n.scenarioStack.withPluginTags(tags))
  }

  def copyProfilerTagOnly(ss: ScenarioStack, distTag: Option[JobTaskIdTag]): Option[JobTaskIdTag] = distTag.flatMap {
    pluginTag =>
      // if there is a JobTaskIdTag plugin tag, then keep the same job and task id, but change the grid profiler ID.
      // this is necessary because job and task id are used to find the parent optimus task context, so overriding them
      // causes grid profiler to report the wrong depth
      val curTag = ss.findPluginTag(JobTaskIdTag)
      curTag.map(_.copy(gridProfilerID = pluginTag.gridProfilerID))
  }

  // Task ID registry:
  // This builds up the graph of dist tasks and the engines they ran on
  private[gridprofiler] val taskRegistry =
    c.TrieMap.empty[String, (String, Seq[String])] // task id -> (engine name, list of parent tasks)
  private[optimus] def recordTask(task: String, engine: String, parents: Seq[String]): Unit =
    taskRegistry.update(task, (engine, parents))
  private[optimus] def getTaskRegistry: Map[String, (String, Seq[String])] = taskRegistry.toMap
  private[optimus] def removeTaskRegistry(): Map[String, (String, Seq[String])] = {
    val res = m.Map.empty[String, (String, Seq[String])]
    for (task <- taskRegistry.keySet) {
      taskRegistry.remove(task).foreach { res.update(task, _) }
    }
    res.toMap
  }

  object Metric extends Enumeration {

    class MetricVal[T: Monoid: ClassTag] extends Val(nextId) {
      private val m = implicitly[Monoid[T]]
      private val c = implicitly[ClassTag[T]]
      def zero: T = m.zero
      def append(t1: T, t2: T): T = m.append(t1, t2)
      def sum(ts: Iterable[T]): T = m.sum(ts)
      def checkClass(a: Any): Boolean = {
        val ar = a.asInstanceOf[AnyRef]
        (ar ne null) && c.runtimeClass.isAssignableFrom(a.getClass)
      }
    }

    class MapMetricVal[U: Monoid: ClassTag] extends MetricVal[Map[String, U]]

    val SYNCSTACKS = new MapMetricVal[CriticalSyncStack]
    val NONCONCLOOPS = new MetricVal[NonConcurrentLoopDesc]
    val DALSTATS = new MetricVal[DALStatsEntry]
    val STALLTIME = new MapMetricVal[Long]
    val PROCESS = new MetricVal[ProcessMetricsEntry]
    val HOTSPOTS = new MapMetricVal[PNodeTaskInfo]
    val SCHEDULER = new MapMetricVal[SchedulerProfileEntry]
    val VTTTSTACKS = new MapMetricVal[Set[String]]
    val WALLTIME = new MetricVal[Long]

    // built-in metric types
    type LostConcurrency = Map[String, UniqueCounts] // both sync stacks and non concurrent loops
    type SyncStacks = Map[String, CriticalSyncStack]
    type NonConcurrentLoops = Map[String, NonConcurrentLoopDesc]
    type DALStats = DALStatsEntry
    type StallTime = Map[String, Long]
    type ProcessMetrics = ProcessMetricsEntry
    type Hotspots = Map[String, PNodeTaskInfo] // property name to pnti
    type Scheduler = Map[String, SchedulerProfileEntry]
    type VTTTStacks = Map[String, Set[String]]
  }

  // custom metric names
  private[diagnostics] val nextMetricName = new AtomicInteger(Metric.maxId)
  private val metricNameRegistry = c.TrieMap.empty[String, Int]
  def metricNameToID(name: String): Int = metricNameRegistry.getOrElseUpdate(name, nextMetricName.getAndIncrement())
  private[diagnostics] def getMetricsRegistry = metricNameRegistry.toList
  def metricIDToName(id: Int): Option[String] = metricNameRegistry.find(_._2 == id).map(_._1)

  // magic strings used as keys at different levels of client-side aggregation
  final val aggregatedKey = "<aggregated>"
  final val clientKey = "<client>" // also used as taskId when recording data on the client
  final val remoteKey = "<remote>"
  final def depthKey(d: Int): String = s"<depth $d>"

  object DALStatsEntry {
    val empty: DALStatsEntry = DALStatsEntry(0, 0, 0)
  }

  // DAL stats element
  final case class DALStatsEntry(requests: Int, results: Int, writes: Int) {
    private[diagnostics] def combine(y: DALStatsEntry) = DALStatsEntry(
      requests + y.requests,
      results + y.results,
      writes + y.writes
    )
  }

  // APIs for GridProfiler.data

  // Optimus-private insertion APIs for the metrics of predefined type (implemented in terms of def put)
  //
  // Note: Metrics.PROCESS and Metrics.HOTSPOTS are only inserted in aggregateFromDist because their live versions
  // are maintained separately

  // Record critical sync stacks or non concurrent loops (as defined by counts.tpe)
  private[optimus] final def recordLostConcurrency(desc: String, counts: UniqueCounts, node: NodeTask): Unit = {
    val key = if (counts.tpe == CriticalSyncStack.tpe) Metric.SYNCSTACKS.id else Metric.NONCONCLOOPS.id
    GridProfilerData.put(scopeForNode(node), jobIdForNode(node), key, Map(desc -> counts))
  }

  private[optimus] final def recordDALWrite(results: Int): Unit =
    GridProfilerData.put(currentScope, currentId, Metric.DALSTATS.id, DALStatsEntry(0, 0, results))

  private[optimus] final def recordDALResults(results: Int): Unit =
    GridProfilerData.put(currentScope, currentId, Metric.DALSTATS.id, DALStatsEntry(1, results, 0))

  /** Note: EvaluationContext.current is not always available */
  private[optimus] final def recordStallTime(
      task: NodeTask,
      ni: NodeTaskInfo,
      p: SchedulerPlugin,
      tm: Long,
      nodeName: String): Unit =
    if ((ni ne null) && (task ne null)) {
      GridProfilerData.put(
        scopeForNode(task),
        jobIdForNode(task),
        Metric.STALLTIME.id,
        Map((if (p ne null) "" + p.getClass + " via " else "") + nodeName -> tm))
    }

  private[optimus] final def recordVTTTStack(data: Metric.VTTTStacks): Unit = {
    GridProfilerData.put(currentScope, currentId, Metric.VTTTSTACKS.id, data)
  }

  private[optimus] final def recordSchedulerProfile(thread: String, spe: SchedulerProfileEntry): Unit = {
    GridProfilerData.put(
      OGTrace.BLOCK_ID_UNSCOPED,
      clientKey, // this runs in OGTrace.LocalTables finalizer, no scenario or scope
      Metric.SCHEDULER.id,
      Map(thread -> spe)
    )
  }

  private[optimus] final def recordWallTime(timeInNs: Long): Unit = {
    GridProfilerData.put(currentScope, currentId, Metric.WALLTIME.id, timeInNs)
  }

  // The only public insertion API, for the custom metrics

  /*
   * Adds a custom metric to the grid profiler. Can be called from any node that is executing in the scope of a profiled block (possibly across distribution)
   * Metrics recorded under the same key are aggregated (by addition/concatenation), and V must be one of the types supported by defaultCombOp
   * Example usage:
   *    GridProfiler.recordCustomCounter("pvcount", 1);
   *    GridProfiler.recordCustomCounter("pvtime", 0.314);
   *    GridProfiler.recordCustomCounter("hosts", Set(java.net.InetAddress.getLocalHost.getHostName))
   *
   * By default, this is disabled. To enable, run the top-level OptimusApp with --profile-custom-metrics all (or with an explicit list of keys to allow)
   *
   * the accessors are in ProfilerResult.flat / ProfilerResult.get, e.g.
   *    val p = profiled(scope="abcde") { ... }
   *    val metrics = p.customCounters.get("abcde")
   *    val avgTime = metrics.get("pvtime") / metrics.get("pvcount")
   */
  final def recordCustomCounter[V](
      key: String,
      data: V,
      scopeOverride: Option[Seq[ProfilerScope]] = None,
      node: NodeTask = EvaluationContext.currentNode): Unit = {
    val filter = tagForNode(node) map (_.customMetricsFilter) getOrElse defaultCustomMetrics
    if (filter.contains("all") || filter.contains(key) || isCustomRegressionMetrics(filter, key)) {
      val scope = scopeOverride.map(scopeToBlockID).getOrElse(scopeForNode(node))
      GridProfilerData.put(scope, jobIdForNode(node), metricNameToID(key), data)
    }
  }

  /*This metrics are for extended dal metrics*/
  def isCustomRegressionMetrics(filters: Array[String], key: String): Boolean =
    filters.contains(CustomRegressionMetrics.customRegressionMetrics) && CustomRegressionMetrics.metricsSet.contains(
      key)

  /* See method comment in remapTweakIdsWithBlk (this one just wraps name -> pnti map in a map with a global blkID) */
  def remapTweakIds(orig: Map[String, PNodeTaskInfo]): Map[String, PNodeTaskInfo] =
    remapTweakIdsWithBlk(Map(1 -> orig))(1)

  /**
   * Reassigns new tweak IDs for each property in the system then recomputes tweak dependency mask based on new IDs.
   * e.g. <client> twkA: twkId = 1 twkB: twkId = 2 twkC: twkId = 3 twkD: twkId = 4 calc: depOn = 1 | 2 | 3 | 4 = 15
   *
   * <engine> twkA: twkId = 5 [ remap -> 1 ] twkB: twkId = 6 [ remap -> 2 ] twkC: twkId = 7 [ remap -> 3 ] twkD: twkId =
   * 8 [ remap -> 4 ] calc: depOn = 5 | 6 | 7 | 8 = 240 [ remap -> 1 | 2 | 3 | 4 = 15 ]
   *
   * Note: if engine twkIds overflowed, then we might miss out on a dependency. e.g., with the same client data as
   * above, we miss the dependency on d when we remap: <engine-overflow> twkA: twkId = 5 [ remap -> 1 ] twkB: twkId = 6
   * [ remap -> 2 ] twkC: twkId = 7 [ remap -> 3 ] twkD: twkId = 5 (overflow!) [ remap -> 1 ] calc: depOn = 5 | 6 | 7 =
   * 112 [ remap -> 1 | 2 | 3 = 7 ]
   *
   * This is fixable: we can use our name -> ID mapping when we rewrite calc.depOn for an actually unique identifier
   * depOn 5 | 6 | 7 -> twkA, twkD | twkB | twkC -> 1 | 4 | 2 | 3 = 15
   *
   * This would give us potentially better reuse, but is not an issue for correctness (because of XSFT proxy recovery).
   * Also, this is only really an issue if overflow happened on the engine and not on the client, because this is the
   * only case where we 'lose out' on dependency information. If it's the other way round, we're no worse off.
   *
   * Note: if property name already exists in GridProfilerData.nameToTweakID, then we already saw this property in
   * hotspots reported from an engine
   *
   * Note: the `Map[Int, Map[String, PNodeTaskInfo]]` we pass in here represents blkID -> 'data we care about', where
   * the data we care about is actually the nested Map[String, PNodeTaskInfo], mapping from property name to PNTI. We
   * can ignore blkID because dependencies don't need to be scoped in the same way that other hotspots data does.
   */
  private def remapTweakIdsWithBlk(orig: Map[Int, Map[String, PNodeTaskInfo]]): Map[Int, Map[String, PNodeTaskInfo]] = {
    val remappedIds = mutable.Map[Int, Int](1 -> 1) // internal tweakables (1)

    def foreachPnti(mapper: (String, PNodeTaskInfo) => Unit): Unit =
      orig.foreach { case (_, nameToPnti) => nameToPnti.foreach { case (name, pnti) => mapper(name, pnti) } }

    foreachPnti { (name, pnti) =>
      // Ignore tweakable-but-not-tweaked properties in remapping because dependencies will be relearned when requested
      // proxies actually run, and the value will not be accepted by the proxy with the falsely assumed min scenario.
      // Only tweaked properties will be written to optconfs for smaller dependency sets in the file // [SEE_TWEAKED]
      if (pnti.wasTweaked) {
        val twkID = pnti.tweakID
        if (twkID > 0) {
          val newID = nameToTweakID.synchronized {
            if (twkID == 1) 1 // don't remap internal tweaks
            else nameToTweakID.getOrElseUpdate(name, nameToTweakID.size + 1)
          }
          remappedIds.put(twkID, newID)
        }
      }
    }

    val result = mutable.Map[Int, Map[String, PNodeTaskInfo]]()
    orig.foreach { case (blkID, nameToPnti) =>
      val hotspots = XSFTRemapper.remapTweakDependencies(nameToPnti, remappedIds.toMap)
      result.put(blkID, hotspots)
    }
    result.toMap
  }

  // Called on the client when unpacking a NodeDistResult from the grid
  // This runs unconditionally, but ProfilerData will be empty if profiling was not enabled for this task on the grid
  final def aggregateFromDist(
      over: ProfilerData,
      clientMetrics: JobClientMetrics,
      shouldAggTSProf: Boolean = false): Unit = {
    // temporal surface aggregation
    if (shouldAggTSProf)
      over.tsProfiling.foreach(TemporalSurfaceProfilingDataManager.aggregateTSProfiling)
    // grid profiler aggregation
    val scope = over.tag.map(_.scope).map(GridProfiler.scopeToBlockID).getOrElse(OGTrace.BLOCK_ID_UNSCOPED)
    val aggregation = over.tag.map { _.aggregation }.getOrElse(AggregationType.DEFAULT)
    recordTask(over.taskID, over.gridEngineID, over.parents)

    // calculate dist overhead
    val overTime = TimeUnit.MILLISECONDS.toNanos(
      System.currentTimeMillis - over.distStartTime
    ) /* time passed on the caller */ - over.gridWallTime

    /* time passed on the engine */
    val processMetricsWithDistOverhead =
      over.processMetrics.copy(
        dist = over.processMetrics.dist.copy(
          distOverhead = overTime,
          gsfBytesSent = clientMetrics.getRequestBytes,
          gsfBytesRcvd = clientMetrics.getResponseBytes))

    // aggregate environment metrics
    GridProfilerData.put(
      scope,
      GridProfilerData.aggregationKey(over.taskID, aggregation),
      GridProfiler.Metric.PROCESS.id,
      processMetricsWithDistOverhead)

    // aggregate scheduler metrics
    GridProfilerData.put(
      scope,
      GridProfilerData.aggregationKey(over.taskID, aggregation),
      Metric.SCHEDULER.id,
      over.schedStats)

    // aggregate scoped metrics.
    for (
      (scopeID, m) <- over.data;
      (task, kv) <- m
    ) {
      val newTask = if (over.matches(task)) over.taskID else task
      val newScope = GridProfiler.scopeToBlockID(over.scopeMap(scopeID))
      var i = 0
      while (i < kv.size) {
        val v = kv(i)
        if (v != null)
          GridProfilerData.put(
            newScope,
            GridProfilerData.aggregationKey(newTask, aggregation),
            if (i < GridProfiler.Metric.maxId) i else GridProfiler.metricNameToID(over.metricMap(i)),
            v
          )
        i += 1
      }
    }

    // remap tweakID and dependency info from engine BEFORE we put it in GridProfilerData's global store
    val remapped = remapTweakIdsWithBlk(over.hotspots) // [SEE_REMAP_TWEAK_DEPS]

    // aggregate hotspots (note: recursively-gathered hotspots already arrived in the loop above)
    for ((blkID, hs) <- remapped) {
      GridProfilerData.put(
        GridProfiler.scopeToBlockID(over.scopeMap(blkID)),
        GridProfilerData.aggregationKey(over.taskID, aggregation),
        Metric.HOTSPOTS.id,
        hs
      )
    }
  }

  // Removals

  // Full reset, useful when using profiled block to repeatedly profile a part of a long-running OptimusApp
  final def resetAll(): Unit = {
    GridProfilerData.clear()
    ProcessMetricsEntry.reset()
  }

  // public remove API for the bandwidth-minded users
  // this version takes a metric name and uses the scope from the given or current node
  // to supply a different scope, re-enter it: profiled(scope) { removeCustomCounter("key") }
  // returns the values that were dropped from the profiler's static storage (could be more than one if distribution took place and this metric arrived from different engines)
  final def removeCustomCounter(key: String): MetricData =
    GridProfilerData.removeByKey(metricNameToID(key), currentScope)

  // Lookup/Extraction

  final private def currentId = jobIdForNode(EvaluationContext.currentNode)
  final private def currentScope = scopeForNode(EvaluationContext.currentNode)

  // default aggregator for the profiler metrics.
  // This is invoked when a NodeDistResult returns from distribution, and it carries profiler metrics
  // When using recordCustomCounter, Add aggregators for custom value types here
  private[diagnostics] final def defaultCombOp[T](x: T, y: T): T = {
    type ASet = Set[Any @unchecked] // so Scala knows it's the same type and a + b.head compiles
    (x, y) match {
      // numerics are added
      case (a: Short, b: Short)   => a + b
      case (a: Int, b: Int)       => a + b
      case (a: Long, b: Long)     => a + b
      case (a: Float, b: Float)   => a + b
      case (a: Double, b: Double) => a + b
      // sequences are appended
      case (a: String, b: String)                       => a + b
      case (a: collection.Seq[_], b: collection.Seq[_]) => a ++ b
      // optimization for the most common case, originally made by TemporalContextTrace.scala
      // because calls to recordCustomCounter only ever add one element to a collection
      case (a: ASet, b: ASet) if b.size == 1 => a + b.head
      case (a: ASet, b: ASet) if a.size == 1 => b + a.head
      case (a: ASet, b: ASet)                => a ++ b
      // maps are transformed by reducing the values whose keys are identical over this comboOp, a-la scalaz's |+|
      case (a: Map[_, _], b: Map[_, _]) =>
        val res = m.Map.empty[Any, Any] ++ a
        for ((k, v) <- b) res.put(k, v).foreach(old => res(k) = defaultCombOp(old, v))
        res.toMap
      // special handling (maybe require leaf Value types to define .combine?)
      case (a: DALStatsEntry, b: DALStatsEntry)                 => a.combine(b)
      case (a: UniqueCounts, b: UniqueCounts)                   => a.combine(b)
      case (a: SchedulerProfileEntry, b: SchedulerProfileEntry) => a.combine(b)
      case (a: ProcessMetricsEntry, b: ProcessMetricsEntry)     => a.combine(b)
      case (a: PNodeTaskInfo, b: PNodeTaskInfo)                 => a.combine(b)
      case (a: TimeAndCount, b: TimeAndCount) => TimeAndCount(a.timeNanos + b.timeNanos, a.count + b.count)
      case (a: MaxValue, b: MaxValue)         => MaxValue(math.max(a.value, b.value))
      case _ =>
        throw new IllegalArgumentException(
          s"No aggregation defined for profiler metrics ${x.getClass} and ${y.getClass}")
    }
  }.asInstanceOf[T]

  // for consistency in profiling tools, and in case anything needs to refer to these extensions (eg for looking up optconf files)
  object ProfilerOutputStrings {
    val optconfExtension = "optconf"
    val ogtraceExtension = "ogtrace"
  }

  // if profiling was enabled from command line, log the summary at exit (see comment about appName and pid overrides
  // on generateFileName)
  def logSummary()(implicit source: SourceLocation): Unit =
    logSummary(source = source.toString)

  def logSummary(
      appNameOverride: Option[String] = None,
      pidOverride: Option[Int] = None,
      source: String,
      onShutdown: Boolean = false): Unit = {
    val defaultLvl = getDefaultLevel
    if (System.getProperty("optimus.profiler.noExitSummary") == null && defaultLvl != Level.NONE) {
      val tm1 = System.currentTimeMillis()
      val summaryTable = getSummaryTable
      log.info(s"Final profiler summary: (level = $defaultLvl${if (defaultLvl == RECORDING)
          ", trace file size: %.2f MB".format(OGTrace.traceFileSize() / 1e6)
        else ""})\n${getSummary(summaryTable)}")
      val tm2 = System.currentTimeMillis()
      if (Settings.profileTemporalContext) getTemporalContextCnt.foreach(cntStr => log.info(cntStr))
      // DMC metrics Summary
      if (DmcClientMetrics.hasDmcMetrics(defaultCustomMetrics)) {
        val summary = DmcClientMetrics.summary(
          GridProfilerData
            .aggregateSingle(GridProfilerData.getCombinedCustom, None, AggregationType.AGGREGATED)
            .getOrElse(aggregatedKey, Map.empty))
        log.info(summary.toPrettyString)
        summary.post(EvaluationContext.currentNode)
      }
      writeData(
        generateFileName(appNameOverride, pidOverride),
        summaryTable,
        autoGenerateConfig = true,
        source = source,
        suppliedDir = "",
        onShutdown = onShutdown)
      val tm3 = System.currentTimeMillis()
      log.info(s"elapsed in getSummary: ${tm2 - tm1} ms. elapsed in writeData: ${tm3 - tm2} ms. metrics: ${GridProfiler
          .statistics()}")
      val heapMsg = s"Max heap = ${DiagnosticSettings.getMaxHeapMb}M"
      val xmx = DiagnosticSettings.getJvmArg("-Xmx")
      if (xmx ne null) log.info(s"$heapMsg, configured as -Xmx = $xmx") else log.info(heapMsg)
      log.info(s"Ideal thread count = ${Settings.idealThreadCountToCount}")
    }
  }

  def getTemporalContextCnt: Seq[String] = {
    val allData = for ((_, kvs) <- getVtTtStacks) yield kvs
    allData
      .foldLeft(Map.empty[String, Set[String]])((prev, kvs) => defaultCombOp(prev, kvs))
      .toSeq
      .map { case (k, v) =>
        s"count of $k: ${v.size}"
      }
  }

  // printable summary of how many metrics are there
  def statistics(): String = {
    val stats = m.Map.empty[String, Int].withDefaultValue(0)
    for (
      (_, row) <- GridProfilerData.data;
      (_, kvs) <- row;
      (value, key) <- kvs.zipWithIndex if value != null
    ) {
      if (key < Metric.maxId) Metric(key) match {
        case Metric.SYNCSTACKS   => stats("syncStack") += value.asInstanceOf[Metric.SyncStacks].size
        case Metric.NONCONCLOOPS => stats("nonConcurrentLoop") += value.asInstanceOf[Metric.NonConcurrentLoops].size
        case Metric.DALSTATS     => stats("dal") += 1
        case Metric.STALLTIME    => stats("stall") += value.asInstanceOf[Metric.StallTime].size
        case Metric.PROCESS      => stats("process") += 1
        case Metric.VTTTSTACKS   => stats("temporal") += 1
        case Metric.HOTSPOTS     => stats("hotspots") += value.asInstanceOf[Metric.Hotspots].size
        case Metric.SCHEDULER    => stats("sched") += value.asInstanceOf[Metric.Scheduler].size
        case Metric.WALLTIME     => stats("walltime") += 1
      }
      else stats("custom") += 1
    }
    // add live metrics
    stats("hotspots") += Profiler.getProfileData().size
    stats("sched") += OGTrace.getSchedulerProfiles.size
    // format for printing
    s"${stats.values.sum.toString} ${stats.toSeq.sortBy(_._2).reverse.mkString(" ")}"
  }

  // Global accessors, not associated with any ProfilerResult
  // There are two kinds: getXYZ and getXYZsummary
  // all of these return Map[Task -> Value]
  private def getDALStats = GridProfilerData.getCombined[Metric.DALStats](Metric.DALSTATS.id)
  private def getSchedStats: Map[String, Metric.Scheduler] =
    GridProfilerData.getCombined[Metric.Scheduler](Metric.SCHEDULER.id)
  private[gridprofiler] def getSyncStacks: Map[String, Metric.SyncStacks] =
    GridProfilerData.getCombined[Metric.SyncStacks](Metric.SYNCSTACKS.id)
  private[gridprofiler] def getNonConcurrentLoops =
    GridProfilerData.getCombined[Metric.NonConcurrentLoops](Metric.NONCONCLOOPS.id)
  private def getVtTtStacks = GridProfilerData.getCombined[Metric.VTTTStacks](Metric.VTTTSTACKS.id)
  private def getStallTimes = GridProfilerData.getCombined[Metric.StallTime](Metric.STALLTIME.id)
  private def getProcessMetrics = GridProfilerData.getCombined[Metric.ProcessMetrics](Metric.PROCESS.id)
  private[gridprofiler] def getHotspots = GridProfilerData.getCombined[Metric.Hotspots](Metric.HOTSPOTS.id)

  // The summary APIs aggregate everything down to single value
  def getProcessStatsSummary: Metric.ProcessMetrics =
    getProcessMetrics.values.foldLeft(ProcessMetricsEntry.end())((a, b) => defaultCombOp(a, b))

  def getPerSchedulerProfiles(schedStats: Map[String, Metric.Scheduler]): Map[String, SchedulerProfileEntry] =
    schedStats.values.foldLeft(OGTrace.getSchedulerProfiles.asScala.toMap)(op = (a, b) => defaultCombOp(a, b))

  def getPerSchedulerProfiles: Map[String, SchedulerProfileEntry] = getPerSchedulerProfiles(getSchedStats)

  def getSchedulerStatsSummary(schedStats: Map[String, Metric.Scheduler]): SchedulerProfileEntry = {
    val perScheduler = getPerSchedulerProfiles(schedStats)
    // noinspection ConvertibleToMethodValue
    if (perScheduler.nonEmpty) perScheduler.values.reduce(defaultCombOp(_, _))
    else SchedulerProfileEntry()
  }

  def getSchedulerStatsSummary: SchedulerProfileEntry = getSchedulerStatsSummary(getSchedStats)

  /**
   * Combine local and engine hotspots data. Note: since tweak IDs are JVM-specific, we reassign them for each property
   * in the system, and rewrite tweak dependencies in terms of these new IDs. Only do this for local data here because
   * engine data will be remapped in GridProfiler.aggregateFromDist as engines report results back to client
   */
  def getHotspotsSummary: Metric.Hotspots = {
    val local = Profiler.combinePNTIs(Profiler.getProfileData())
    val remapped = remapTweakIds(local) // [SEE_REMAP_TWEAK_DEPS]
    getHotspots.values.foldLeft(remapped)((a, b) => defaultCombOp(a, b))
  }

  def getDalStatsSummary: Metric.DALStats =
    getDALStats.values.foldLeft(GridProfiler.DALStatsEntry(0, 0, 0))((a, b) => defaultCombOp(a, b))

  /** Used for gating regressions (may want to consider NonConcurrentLoops too */
  def getSyncStacksSummary: Metric.SyncStacks =
    getSyncStacks.values.foldLeft(Map.empty[String, CriticalSyncStack])((a, b) => defaultCombOp(a, b))

  def getNonConcurrentLoopsSummary: Metric.NonConcurrentLoops =
    getNonConcurrentLoops.values.foldLeft(Map.empty[String, NonConcurrentLoopDesc])((a, b) => defaultCombOp(a, b))

  /**
   * Used in OGSchedulerLostConcurrency to make sure we have sync stacks AND non concurrent loops reported from dist
   */
  def getLostConcurrencySummary: jutil.List[jutil.Map.Entry[String, UniqueCounts]] = {
    // denormalize the table: typical dist use has very few agg keys, so e.g. in --profile-aggregation localandremote,
    // we'll get at most two entries for each sync stack (one with key == local, and one with key == remote)
    val entries = m.ArrayBuffer.empty[jutil.Map.Entry[String, UniqueCounts]]
    def addEntriesFrom(metric: Map[String, Map[String, UniqueCounts]]): Unit = {
      for (
        (key, cssMap) <- GridProfilerData.aggregateSingle(metric, None, defaultAggregationType);
        (desc, uCount) <- cssMap
      ) {
        uCount.setKey(key)
        entries += new jutil.AbstractMap.SimpleEntry[String, UniqueCounts](desc, uCount)
      }
    }
    addEntriesFrom(getSyncStacks)
    addEntriesFrom(getNonConcurrentLoops)
    entries.sorted(Ordering.by { e: jutil.Map.Entry[String, UniqueCounts] =>
      (e.getValue.tpe, e.getValue.aggregationKey, -e.getValue.get)
    })
    entries.asJava
  }

  def getStallTimesSummary: Metric.StallTime =
    getStallTimes.values.foldLeft(Map.empty[String, Long])((a, b) => defaultCombOp(a, b))

  def getConfigMetricSummary(hs: Seq[PNodeTaskInfo]): ConfigMetrics = {
    val env = getProcessStatsSummary
    val wallTime = env.totalGraphTime
    val cpuTime = env.jvmCPUTime
    val maxHeap = env.maxHeap
    val cacheTime = getSchedulerStatsSummary.cacheTime
    val cacheHits = hs.map(_.cacheHit).sum
    val cacheMisses = hs.map(_.cacheMiss).sum
    val evictions = hs.map(_.evicted).sum
    val dalRequests = getDALStats.values.map(_.requests).sum
    val enginesWallTime = env.remoteWallTime
    ConfigMetrics(
      wallTime,
      cpuTime,
      cacheTime,
      maxHeap,
      cacheHits,
      cacheMisses,
      evictions,
      dalRequests,
      enginesWallTime)
  }

  // formatted profiler summary for given id
  // uses live profiler data measured so far (not the values snapshotted inside a profiled block)
  // uses unconditional full aggregation (this is for the one-line final profiler summary)
  def getSummary: String = getSummary(getSummaryTable)
  def getSummary(summaryTable: SummaryTable): String =
    summaryString(summaryTable, getPerSchedulerProfiles, getProcessStatsSummary)

  private[optimus] def getSummaryTable: SummaryTable =
    getSummaryTable(
      getDalStatsSummary,
      getPerSchedulerProfiles,
      getSyncStacksSummary,
      getStallTimesSummary,
      getProcessStatsSummary,
      getHotspotsSummary
    )

  private[optimus] def getSummaryTable(pd: ProfilerData): SummaryTable = {
    val dalStatSummary: Metric.DALStats = pd.getDalStatsSummary
    val perSchedulerProfiles: Metric.Scheduler = pd.perSchedulerSummary
    val syncStacksSummary: Metric.SyncStacks = pd.syncStacksSummary
    val stallTimeSummary: Metric.StallTime = pd.stallTimeSummary
    val processStatsSummary: Metric.ProcessMetrics = pd.processMetrics
    val hotSpotsSummary: Metric.Hotspots = pd.hotSpotSummary
    getSummaryTable(
      dalStatSummary,
      perSchedulerProfiles,
      syncStacksSummary,
      stallTimeSummary,
      processStatsSummary,
      hotSpotsSummary)
  }

  private[diagnostics] def getSummaryTable(
      dal: Metric.DALStats,
      sched: Metric.Scheduler,
      syncStacks: Metric.SyncStacks,
      stallTimes: Metric.StallTime,
      env: Metric.ProcessMetrics,
      hs: Metric.Hotspots): SummaryTable = {
    val schedCombined =
      // noinspection ConvertibleToMethodValue
      if (sched.nonEmpty) sched.values.reduce(defaultCombOp(_, _))
      else SchedulerProfileEntry()
    SummaryTable(
      totalProcTime = DiagnosticSettings.jvmUpTimeInMs(),
      preGraphTime = OGSchedulerTimes.getTimeToFirstGraphEnterTime,
      preOptimusStartupTime = OGSchedulerTimes.getPreOptimusStartupTime,
      offGraphOptimusStartupTime = OGSchedulerTimes.getOffGraphOptimusStartupTime,
      onGraphOptimusStartupTime = OGSchedulerTimes.getOnGraphOptimusStartupTime,
      postOptimusOnGraphAppStartupTime = OGSchedulerTimes.getPostOptimusOnGraphAppStartupTime,
      postOptimusBetweenGraphAppStartupTime = OGSchedulerTimes.getPostOptimusBetweenGraphAppStartupTime,
      graphHolesTime = OGSchedulerTimes.getGraphHolesTime,
      postGraphTime = OGSchedulerTimes.getPostGraphTime,
      wallTime = env.totalGraphTime,
      clientWallTime = OGSchedulerTimes.getInGraphWallTime,
      graphTime = schedCombined.userGraphTime,
      waitTime = schedCombined.waitTime,
      stallTime = stallTimes.values.sum,
      selfTime = schedCombined.selfTime,
      cacheTime = schedCombined.cacheTime,
      underUtilizedTime = env.totalUnderUtilizedTime,
      writes = dal.writes,
      requests = dal.requests,
      results = dal.results,
      distOverhead = env.dist.distOverhead,
      engineStartupMs = env.dist.engineStartupMs,
      remoteWallTime = env.remoteWallTime,
      distTasks = env.dist.numTasks,
      syncStacks = syncStacks.values.map(_.get).sum,
      maxHeap = env.maxHeap,
      loadAvg = env.loadAvg,
      optimusCPUTime = schedCombined.cpuTime,
      jvmCPUTime = env.jvmCPUTime,
      jvmCPULoad = env.jvmCPULoad,
      systemCPULoad = env.systemCPULoad,
      gcTimeStopTheWorld = env.gcStats.gcTimeStopTheWorld,
      gcTimeAll = env.gcStats.gcTimeAll,
      jitTime = env.jitTime,
      clTime = env.clTime,
      timeBasedTriggerRatioHit = env.gcStats.gcMonitorStats.timeBasedTriggerRatioHit,
      timeBasedKillTriggerHit = env.gcStats.gcMonitorStats.timeBasedKillTriggerHit,
      heapBasedTriggerHit = env.gcStats.gcMonitorStats.heapBasedTriggerHit,
      heapBasedIncludeSITriggerHit = env.gcStats.gcMonitorStats.heapBasedIncludeSITriggerHit,
      heapBasedBackoffTriggerHit = env.gcStats.gcMonitorStats.heapBasedBackoffTriggerHit,
      totalNumberNodesCleared = env.gcStats.gcMonitorStats.totalNumberNodesCleared,
      threadStatesSummary = env.threadStatesSummary,
      stallDetails = OGSchedulerTimes.getStallingReasons.asScalaUnsafeImmutable,
      nodeStarts = hs.values.map(_.start).sum,
      cacheHits = hs.values.map(_.cacheHit).sum,
      cacheMisses = hs.values.map(_.cacheMiss).sum,
      cacheEvictions = hs.values.map(_.evicted).sum
    )
  }

  def summaryString(summaryTable: SummaryTable, sched: Metric.Scheduler, env: Metric.ProcessMetrics): String = {
    val defaultLvl = getDefaultLevel
    val sw = new StringWriter
    val bw = new BufferedWriter(sw)
    bw.write(
      // environment-only metrics. These are gathered by default (at level NONE), but only written out at level ENVIRON
      if (defaultLvl < LIGHT)
        env.formatSummary
      else
        summaryTable.toString
    )
    if (DiagnosticSettings.profileShowThreadSummary) {
      bw.newLine()
      if (sched.isEmpty)
        bw.write("no data: -Doptimus.profile.showThreadSummary was enabled, but profile level was " + defaultLvl)
      val (names, entries) = sched.toSeq.filter(_._2.graphTime != 0).sortBy(_._1).unzip
      val cols = Seq(
        StatTableCol("Thread name", "%20s", names),
        StatTableCol("Graph time (s)", "%15.3f", entries.map(_.graphTime / 1e9)),
        StatTableCol("Self Time (s)", "%15.3f", entries.map(_.selfTime / 1e9)),
        StatTableCol("CPU Time (s)", "%15.3f", entries.map(_.cpuTime / 1e9)),
        StatTableCol("Cache Time (s)", "%15.3f", entries.map(_.cacheTime / 1e9)),
        StatTableCol("Wait Time (s)", "%15.3f", entries.map(_.waitTime / 1e9)),
        StatTableCol("Spin Time (s)", "%15.3f", entries.map(_.spinTime / 1e9)),
        StatTableCol("Idle Time (s)", "%15.3f", entries.map(_.idleTime / 1e9))
      )
      val table = new StatTable(cols)
      bw.write(table.toString())
    }
    bw.flush()
    sw.toString
  }

  def toMaps(headers: Array[String], data: Iterable[Array[String]]): Iterable[Map[String, String]] =
    data.map { row =>
      headers.zip(row).toMap
    }

  /**
   * Extracted for unit testing since this will be used for gating regressions and format needs to keep in sync with
   * performance_metrics.py
   */
  def syncStacksReport(
      ps: PrintStream,
      sstacks: Map[String, Metric.SyncStacks]): immutable.Iterable[Map[String, String]] =
    for {
      (engine, aggregatedSStacks) <- GridProfilerData.aggregateSingle(sstacks, None, defaultAggregationType)
      (stack, n) <- aggregatedSStacks
    } yield {
      val count = n.get
      ps.println(s"${stack.trim} ($count times on $engine)")
      ps.println()
      Map("engine" -> engine, "stack" -> stack.trim, "n" -> count.toString)
    }

  def getCSV(pr: Option[ProfilerResult[_]]): CSVResults = {
    val flatPR = pr.map(_.flat)

    val all = mutable.HashMap.empty[String, Iterable[Map[String, String]]]

    val dalStats = printString(ps => {
      val stats: Map[String, DALStatsEntry] = flatPR.map(_.allDalStats).getOrElse(getDALStats)
      if (stats.nonEmpty) {
        val writer = new StringWriter
        val csvWriter = new CSVWriter(writer)
        val header = Array("Engine", "DAL writes", "DAL requests", "DAL results")
        val data =
          for ((engine, data) <- GridProfilerData.aggregateSingle(stats, None, defaultAggregationType))
            yield Array(
              s"$engine",
              s"${data.writes}",
              s"${data.requests}",
              s"${data.results}"
            )
        all += "dal" -> toMaps(header, data)
        csvWriter.writeAll((header +: data.toSeq).asJava)
        ps.print(writer.toString)
      }
    })

    // print custom counters if any
    // unlike most other CSVs, this one does not aggregate across scopes and makes the scopes visible
    // this supports user-defined metrics like number of calls to f() (custom counter) for each book (scope)
    val stats: Map[Int, Map[String, Map[String, Any]]] =
      pr.map(_.scopes).map(GridProfilerData.getCustom).getOrElse(GridProfilerData.getAllCustom)
    // exclude from csv dumping
    val writeExcluded = Set(DmcClientMetrics.DmcComputedKeyHash)

    // "k" in key and later down "v" in value must be lower case for parsing done in performance_metrics.py in env repo
    val baseHeader = Array("Engine", "Scope", "Custom key")

    // we are separating the priql data from the non-priql data so that data from certain noisy locations can be filtered out from the total count when we are processing
    val priqlData: ListBuffer[Array[String]] = new ListBuffer[Array[String]]()
    val nonPriqlData: ListBuffer[Array[String]] = new ListBuffer[Array[String]]()
    stats.foreach { case (scope, row) =>
      GridProfilerData.aggregateSingle(row, None, defaultAggregationType).foreach { case (task, customs) =>
        customs.foreach { case (key, value) =>
          if (!(writeExcluded contains key)) {
            val profiledScope = GridProfiler.blockIDToScope(scope)
            if (key == CustomRegressionMetrics.dalPriqlCustomMetric)
              value.asInstanceOf[Map[String, Int]].map { case (loc, count) =>
                priqlData += Array(
                  s"$task",
                  if (profiledScope.isDefined) s"${profiledScope.get.map(_.text).mkString(",")}" else "",
                  s"$key",
                  loc,
                  count.toString)
              }
            else
              nonPriqlData += Array(
                s"$task",
                if (profiledScope.isDefined) s"${profiledScope.get.map(_.text).mkString(",")}" else "",
                s"$key",
                s"$value")
          }
        }
      }
    }
    def generateCustomData(data: Iterable[Array[String]], priql: Boolean = false): String = {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val updatedHeader = if (priql) baseHeader ++ Array("Location", "Custom value") else baseHeader :+ "Custom value"
      if (data.nonEmpty) {
        csvWriter.writeAll((updatedHeader +: data.toSeq).asJava)
        all += "custom" -> toMaps(updatedHeader, data)
      }
      writer.toString
    }
    val customCountersNonPriql = printString(_.print(generateCustomData(nonPriqlData)))
    // "Custom key" for priql will always be the same (dalPriqlCount) but keeping it for consistency with other csv
    val customCountersPriql = printString(_.print(generateCustomData(priqlData, priql = true)))

    val sstacks = flatPR.map(_.allSyncStacks).getOrElse(getSyncStacks)
    val syncStacks = printString { ps =>
      val stats = syncStacksReport(ps, sstacks)
      all += "syncStacks" -> stats
    }

    val vtTtStacks = printString(ps => {
      val vttts: Map[String, Metric.VTTTStacks] = flatPR.map(_.allVTTTStacks).getOrElse(getVtTtStacks)
      // print VT/TT traces
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header = Array("Engine", "JVM Stack", "Node Stacks")
      val data =
        for (
          (engine, aggregatedMetrics) <- GridProfilerData
            .aggregateSingle(vttts, None, defaultAggregationType);
          (k, stacks) <- aggregatedMetrics
        )
          yield (s"$engine" +: (if (k.isEmpty) s"N/A" else s"${k.trim}") +: stacks.toSeq).toArray
      if (data.nonEmpty) {
        all += "vtTtStacks" -> toMaps(header, data)
        csvWriter.writeAll((header +: data.toSeq).asJava)
        ps.print(writer.toString)
      }
    })

    // print stall times, grouped first by scope name, then by the scheduling plugin of the last scheduler thread that entered wait
    val stallMetrics = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header = Array("Engine", "Plugin", "Node", "Stall Time (ms)", "Scope")

      val stats: Map[Int, Map[String, Metric.StallTime]] = pr
        .map(_.scopes)
        .map(GridProfilerData.get[Metric.StallTime](_, Metric.STALLTIME.id))
        .getOrElse(GridProfilerData.getAll[Metric.StallTime](Metric.STALLTIME.id))

      val data =
        for (
          (scope, row) <- stats;
          (engine, aggregated) <- GridProfilerData
            .aggregateSingle(row, None, defaultAggregationType);
          (plugin, time) <- aggregated.toSeq.sortBy(-_._2)
        )
          yield plugin split " " match {
            case Array(cls, plugin, via, node) if cls == "class" && via == "via" =>
              val profiledScope = GridProfiler.blockIDToScope(scope)
              Array(
                s"$engine",
                s"$plugin",
                s"$node",
                s"${time * 1e-6}",
                if (profiledScope.isDefined) profiledScope.get.map(_.text).mkString(".") else "")
            case node =>
              val profiledScope = GridProfiler.blockIDToScope(scope)
              Array(
                s"$engine",
                "no plugin",
                node.mkString(" "),
                s"${time * 1e-6}",
                if (profiledScope.isDefined)
                  profiledScope.get.map(_.text).mkString(".")
                else "")
          }
      if (data.nonEmpty) {
        csvWriter.writeAll((header +: data.toSeq).asJava)
        all += "stall" -> toMaps(header, data)
        ps.print(writer.toString)
      }
    })

    // print distribution environment metrics, grouped by the engine on which they ran
    val environMetrics: String = {
      val data: Map[String, Metric.ProcessMetrics] = flatPR
        .map(_.envMetrics(defaultAggregationType))
        .getOrElse(GridProfilerData
          .aggregateSingle(getProcessMetrics, Some(ProcessMetricsEntry.end()), defaultAggregationType))

      val (csv, stats) = ProcessMetricsEntry.printCSV(data)
      all += "environ" -> stats

      csv
    }

    val threadStats: String = {
      val data: Map[String, Metric.Scheduler] = flatPR
        .map(_.schedStats(defaultAggregationType))
        .getOrElse(
          GridProfilerData.aggregateSingle(
            getSchedStats, // stored
            Some(OGTrace.getSchedulerProfiles.asScala.toMap), // live
            defaultAggregationType))
      val (csv, stats) = SchedulerProfileEntry.printCSV(data)
      all += "thread" -> stats
      csv
    }

    CSVResults(
      dalStats,
      customCountersNonPriql,
      customCountersPriql,
      syncStacks,
      vtTtStacks,
      stallMetrics,
      environMetrics,
      threadStats,
      all.toMap)
  }

  def printString(f: PrintStream => Unit): String = {
    val w = new java.io.ByteArrayOutputStream
    val ps = new PrintStream(w)
    try {
      f(ps)
    } finally {
      ps.close()
    }
    w.toString
  }

  def getCustomCountersFromScope(fullScope: Seq[ProfilerScope]): Map[String, Map[String, Any]] = {
    val ids = GridProfiler.getScopeRegistry.filter(_._1.startsWith(fullScope)).map(_._2)
    val profilerResult = new SingleScopeProfilerResult(ids, null, null)
    profilerResult.customCounters(AggregationType.AGGREGATED)
  }

  def writeAppletNameCsv(appletNames: Set[String]) = {
    val defaultFilePath: String = s"${getDefaultCSVFolder}${File.separator}appletInfo.csv"
    log.info("Writing applet information")
    val contents = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header = Array("applet name")
      val row = appletNames.toSeq.map(Array(_))
      val data = header +: row
      csvWriter.writeAll(data.asJava)
      ps.print(writer.toString)
    })
    val absolutePath = Paths.get(defaultFilePath).toAbsolutePath
    try {
      Files.write(absolutePath, contents.getBytes)
      log.info(s"Wrote applet name at $absolutePath")
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Could not write file at $absolutePath", ex)
    }
  }
}

final case class CSVResults(
    dalStats: String,
    customCountersNonPriql: String,
    customCountersPriql: String,
    syncStacks: String,
    vtTtStacks: String,
    stallTimes: String,
    environMetrics: String,
    threadStats: String,
    all: Map[String, Iterable[Map[String, String]]]
)
