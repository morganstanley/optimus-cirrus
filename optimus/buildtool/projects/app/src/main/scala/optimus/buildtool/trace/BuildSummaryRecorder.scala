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

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.cache.RemoteArtifactCacheTracker
import optimus.buildtool.compilers.zinc.CompilerThrottle
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.resolvers.DependencyDownloadTracker
import optimus.buildtool.utils.Utils
import optimus.graph.GCMonitor
import optimus.graph.GCMonitor.CumulativeGCStats
import optimus.graph.cache.Caches
import optimus.graph.diagnostics.EvictionReason

import java.nio.file.Files
import java.time.Instant
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat._
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BuildSummaryRecorder(outputDir: Option[Directory], compilerThrottle: CompilerThrottle)
    extends DefaultObtTraceListener {
  private val log = getLogger(this)

  @volatile private var startTime: Instant = _

  private val timer = new Timer
  @volatile private var timerTask: Option[TimerTask] = None

  private val stats = new ConcurrentHashMap[ObtStat, Long]()
  private val statsSets = new ConcurrentHashMap[ObtStat, mutable.Set[Any]]()
  private val gcMonitor = GCMonitor.instance
  private val GcMonitorConsumerName = "OptimusBuildTool"

  object BuildSummaryTaskTrace extends DefaultTaskTrace {
    override def supportsStats: Boolean = BuildSummaryRecorder.this.supportsStats
    override def setStat(stat: ObtStat, value: Long): Unit = {
      // deliberately addToStat here since we're adding to the overall build count
      BuildSummaryRecorder.this.addToStat(stat, value)
    }
    override def addToStat(stat: ObtStat, value: Long): Unit =
      BuildSummaryRecorder.this.addToStat(stat, value)
    override def addToStat(stat: ObtStat, value: Set[_]): Unit =
      BuildSummaryRecorder.this.addToStat(stat, value)
  }

  override def startBuild(): Unit = {
    startTime = patch.MilliInstant.now
    stats.clear()
    statsSets.clear()
    gcMonitor.snapAndResetStats(GcMonitorConsumerName)
    val task = new TimerTask() {
      override def run(): Unit = {
        logCacheDetails()
        logStats(buildComplete = false)
      }
    }
    timerTask = Some(task)
    timer.scheduleAtFixedRate(task, 600000, 600000)
  }

  def storeStats(): Unit = {
    import spray.json._, DefaultJsonProtocol._

    outputDir.foreach { dir =>
      val preparedStats = stats.asScala.toMap.map { case (k, v) => k.key -> v }
      val file = dir.resolveFile(s"obt-stats-${System.currentTimeMillis()}.json").asJson
      log.info(s"Writing OBT stats to $file")
      if (!dir.existsUnsafe) Files.createDirectory(dir.path)
      Files.write(file.path, preparedStats.toJson.prettyPrint.getBytes)
    }
  }

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace = BuildSummaryTaskTrace

  override def supportsStats: Boolean = true
  override def setStat(stat: ObtStat, value: Long): Unit =
    stats.put(stat, value)
  override def addToStat(stat: ObtStat, value: Long): Unit =
    stats.compute(stat, (_, current) => current + value)
  override def addToStat(stat: ObtStat, value: Set[_]): Unit =
    statsSets.compute(
      stat,
      (_, current) => if (current == null) value.to(mutable.Set).asInstanceOf[mutable.Set[Any]] else current ++= value)

  override def endBuild(success: Boolean): Boolean = {
    timerTask.foreach(_.cancel())
    storeStats()
    DependencyDownloadTracker.summaryThenClean(DependencyDownloadTracker.downloadedHttpFiles.nonEmpty)(s =>
      log.debug(s))
    RemoteArtifactCacheTracker.summaryThenClean(RemoteArtifactCacheTracker.corruptedFiles.nonEmpty)(s => log.warn(s))
    logCacheDetails()
    logStats(buildComplete = true)
    true
  }

  private def logCacheDetails(): Unit = {
    val caches = Caches.allCaches()
    val cacheDetails = caches.map { c =>
      val counters = c.getCountersSnapshot
      val evictions = EvictionReason.values().map(r => r -> counters.numEvictions(r)).collect {
        case (r, n) if n > 0 =>
          s"$r ($n)"
      }
      val evictionsStr =
        if (evictions.nonEmpty) s" [Evictions: ${evictions.mkString(", ")}]"
        else s" [Evictions: ${counters.insertCount - counters.indicativeCacheSize}]"
      s"${c.getName}: ${counters.indicativeCacheSize}/${c.getMaxSize}$evictionsStr"
    }
    log.debug(s"Cache details: ${cacheDetails.mkString("\n\t", "\n\t", "")}")
  }

  private def logStats(buildComplete: Boolean): Unit = {
    val endTime = patch.MilliInstant.now
    val durationMillis = endTime.toEpochMilli - startTime.toEpochMilli

    def totalScopesSummary: String = {
      val scopes = stats.getOrDefault(ObtStats.Scopes, 0)
      val transitiveScopes = stats.getOrDefault(ObtStats.TransitiveScopes, 0)

      f"\tTotal scopes: ${scopes + transitiveScopes}%,d [Direct: $scopes%,d, Transitive: $transitiveScopes%,d]"
    }

    def workspaceChangesSummary: String = {
      val changedDirectories = stats.getOrDefault(ObtStats.ModifiedDirectories, 0)
      val deletedArtifacts = stats.getOrDefault(ObtStats.DeletedArtifacts, 0)
      val internalBinaryDeps = stringsInSet(ObtStats.InternalBinaryDependencies).size
      val mutableExternalDeps = stringsInSet(ObtStats.MutableExternalDependencies)
      val numMutableExternalDeps = mutableExternalDeps.size

      if (numMutableExternalDeps > 0)
        log.debug(s"Mutable external dependencies: ${mutableExternalDeps.to(Seq).sorted.mkString(", ")}")

      if (changedDirectories > 0 || deletedArtifacts > 0 || internalBinaryDeps > 0 || numMutableExternalDeps > 0)
        f"\tWorkspace changes: ${changedDirectories + deletedArtifacts} [Changed directories: $changedDirectories%,d, Deleted artifacts: $deletedArtifacts%,d, Internal binary dependencies: $internalBinaryDeps, Mutable external dependencies: $numMutableExternalDeps]"
      else ""
    }

    def sizeOfSet(stat: ObtStat): Int =
      Option(statsSets.get(stat)).map(_.size).getOrElse(0)
    def stringsInSet(stat: ObtStat): Set[String] =
      Option(statsSets.get(stat)).map(_.map(_.toString).toSet).getOrElse(Set.empty)

    def jvmHitsSummary: String = {
      def jvmHits(cacheType: ObtStats.Cache): Long =
        stats.getOrDefault(cacheType.ScalaHit, 0) + stats.getOrDefault(cacheType.JavaHit, 0)

      val jvmArtifacts =
        stats.getOrDefault(ObtStats.ScalaArtifacts, 0) + stats.getOrDefault(ObtStats.JavaArtifacts, 0)
      val nodeCacheMisses =
        sizeOfSet(ObtStats.NodeCacheScalaMiss) + sizeOfSet(ObtStats.NodeCacheJavaMiss)
      // node cache hits is inexact, but at least make sure we don't report negative hits
      val nodeCacheHits = math.max(jvmArtifacts - nodeCacheMisses, 0)

      val jvmFilesystemHits = jvmHits(ObtStats.FilesystemStore)
      val jvmDhtHits = jvmHits(ObtStats.DHT)
      val zincHits = stats.getOrDefault(ObtStats.ZincCacheHit, 0)

      val jvmTotalCacheHits = nodeCacheHits + jvmFilesystemHits + zincHits + jvmDhtHits
      f"\t\tCache hits: $jvmTotalCacheHits%,d [In-memory: $nodeCacheHits%,d, Local disk: $jvmFilesystemHits%,d, DHT: $jvmDhtHits%,d, Incremental: $zincHits%,d]"
    }

    def jvmScopeCompilationSummary: String = {
      val scopeCompilations = stats.getOrDefault(ObtStats.Compilations, 0)

      val causes = if (scopeCompilations > 0) {
        val c = stats.asScala
          .collect {
            case (reason: ObtStats.CompilationReason, count) if count > 0 =>
              reason -> count
          }
          .to(Seq)
          .sortBy { case (reason, _) => reason.order }
          .map { case (reason, count) => f"${reason.str.capitalize}: $count%,d" }
          .mkString(", ")
        s", due to [$c]"
      } else ""

      f"\t\tScope compilations: $scopeCompilations%,d$causes"
    }

    def jvmCompilationSummary: String = {
      val readFsSources = stats.getOrDefault(ObtStats.ReadFileSystemSourceFiles, 0)
      val readDistinctFsSources = sizeOfSet(ObtStats.ReadDistinctFileSystemSourceFiles)
      val readGitSources = stats.getOrDefault(ObtStats.ReadGitSourceFiles, 0)
      val readDistinctGitSources = sizeOfSet(ObtStats.ReadDistinctGitSourceFiles)
      val readSources = readFsSources + readGitSources

      val addedSources = stats.getOrDefault(ObtStats.AddedSourceFiles, 0)
      val modifiedSources = stats.getOrDefault(ObtStats.ModifiedSourceFiles, 0)
      val removedSources = stats.getOrDefault(ObtStats.RemovedSourceFiles, 0)
      val changedSources = addedSources + modifiedSources + removedSources
      val invalidatedSources = stats.getOrDefault(ObtStats.InvalidatedSourceFiles, 0)
      val totalSources = stats.getOrDefault(ObtStats.SourceFiles, 0)
      val externalDependenciesChanged = stringsInSet(ObtStats.ExternalDependencyChanges)
      val numExternalDependenciesChanged = externalDependenciesChanged.size

      val compiledLines = stats.getOrDefault(ObtStats.CompiledLinesOfCode, 0)
      val totalLines = stats.getOrDefault(ObtStats.LinesOfCode, 0)

      if (numExternalDependenciesChanged > 0)
        log.debug(s"Changed external dependencies: ${externalDependenciesChanged.to(Seq).sorted.mkString(", ")}")

      val readSourcesStr =
        if (readSources > 0)
          f"\t\tSource files read: $readSources%,d [Local disk: $readFsSources%,d ($readDistinctFsSources%,d distinct), Git: $readGitSources%,d ($readDistinctGitSources%,d distinct)]"
        else ""
      val changedSourcesStr =
        if (changedSources > 0) f"\t\tSource files changed: $changedSources%,d" else ""
      val externalDependenciesChangedStr =
        if (numExternalDependenciesChanged > 0) f"\t\tExternal dependencies changed: $numExternalDependenciesChanged%,d"
        else ""
      val invalidatedSourcesStr =
        if (invalidatedSources > 0)
          f"\t\tSource files recompiled: $invalidatedSources%,d/$totalSources%,d (Source lines: $compiledLines%,d/$totalLines%,d)"
        else ""

      List(
        readSourcesStr,
        changedSourcesStr,
        externalDependenciesChangedStr,
        invalidatedSourcesStr,
      ).filter(!_.isBlank).mkString("\n")
    }

    def jvmThrottleSummary: String = {
      val stats = if (buildComplete) compilerThrottle.snapAndResetStats() else compilerThrottle.snapStats()
      if (stats.nonEmpty) {
        val maxWeight = stats.map(_.inflightWeight).max.toInt
        val concurrency = stats.map(_.nInFlight)
        val maxConcurrency = concurrency.max
        val meanConcurrency = concurrency.sum.toDouble / concurrency.size
        val queued = stats.map(_.nQueued)
        val maxQueued = queued.max
        val meanQueued = queued.sum.toDouble / queued.size
        f"\t\tMean Concurrency: $meanConcurrency%,.1f ($meanQueued%,.1f queued), Max Concurrency: $maxConcurrency%,d ($maxWeight%,d bytes, $maxQueued%,d queued), Limit: ${compilerThrottle.maxNumZincs}%,d (${compilerThrottle.zincByteLimit}%,d bytes)"
      } else ""
    }

    def pythonHitsSummary: String = {
      def pythonHits(cacheType: ObtStats.Cache): Long =
        stats.getOrDefault(cacheType.PythonHit, 0)

      val pythonFilesystemHits = pythonHits(ObtStats.FilesystemStore)
      val pythonDhtHits = pythonHits(ObtStats.DHT)
      // Note: We don't include node cache hits here because there's no easy way to calculate them
      val pythonTotalCacheHits = pythonFilesystemHits + pythonDhtHits

      f"\t\tCache hits: $pythonTotalCacheHits%,d [Local disk: $pythonFilesystemHits%,d, DHT: $pythonDhtHits%,d]"
    }

    def gcSummary: String = {
      val gcStats =
        if (buildComplete) gcMonitor.snapAndResetStats(GcMonitorConsumerName)
        else gcMonitor.snapStats(GcMonitorConsumerName)
      if (gcStats != CumulativeGCStats.empty) {
        val gcDurationMillis = gcStats.duration
        val gcPercent = gcDurationMillis * 100.0 / durationMillis
        f"""\tMin Heap: ${gcStats.minAfter}%,.0fMB, Max Heap: ${gcStats.maxAfter}%,.0fMB
           |\tGarbage Collections: ${gcStats.nMinor + gcStats.nMajor}%,d [Duration: ${Utils.durationString(
            gcDurationMillis)} ($gcPercent%,.1f%%), Major: ${gcStats.nMajor}%,d, Minor: ${gcStats.nMinor}%,d]""".stripMargin
      } else {
        "Garbage Collections: 0"
      }
    }

    val jvmSummary =
      s"""\tJava/Scala:
         |$jvmHitsSummary
         |$jvmScopeCompilationSummary
         |$jvmCompilationSummary""".stripMargin

    val pythonSummary =
      s"""\tPython:
         |$pythonHitsSummary""".stripMargin

    val msg = Seq(
      "Build Summary:",
      s"$totalScopesSummary",
      s"$workspaceChangesSummary",
      s"$jvmSummary",
      jvmThrottleSummary,
      s"$pythonSummary",
      gcSummary
    ).filter(!_.isBlank).mkString("\n")

    if (buildComplete) {
      log.info(msg)
      ObtTrace.info(msg)
    } else {
      log.debug(msg)
    }
  }
}
