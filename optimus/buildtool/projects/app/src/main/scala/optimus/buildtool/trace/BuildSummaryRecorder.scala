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
import optimus.buildtool.config.ScopeId

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat._
import scala.jdk.CollectionConverters._

class BuildSummaryRecorder extends DefaultObtTraceListener {
  private val log = getLogger(this)

  val stats = new ConcurrentHashMap[ObtStat, Long]()
  val statsSets = new ConcurrentHashMap[ObtStat, Set[_]]()

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

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace = BuildSummaryTaskTrace

  override def supportsStats: Boolean = true
  override def setStat(stat: ObtStat, value: Long): Unit =
    stats.put(stat, value)
  override def addToStat(stat: ObtStat, value: Long): Unit =
    stats.compute(stat, (_, current) => current + value)
  override def addToStat(stat: ObtStat, value: Set[_]): Unit =
    statsSets.compute(stat, (_, current) => if (current == null) value else current ++ value)

  override def endBuild(success: Boolean): Boolean = {

    def totalScopesSummary: String = {
      val scopes = stats.getOrDefault(ObtStats.Scopes, 0)
      val transitiveScopes = stats.getOrDefault(ObtStats.TransitiveScopes, 0)

      f"\tTotal scopes: ${scopes + transitiveScopes}%,d [Direct: $scopes%,d, Transitive: $transitiveScopes%,d]"
    }

    def workspaceChangesSummary: String = {
      val changedDirectories = stats.getOrDefault(ObtStats.ModifiedDirectories, 0)
      val deletedArtifacts = stats.getOrDefault(ObtStats.DeletedArtifacts, 0)
      val mutableExternalDeps = statsSets.getOrDefault(ObtStats.MutableExternalDependencies, Set.empty).map(_.toString)
      val numMutableExternalDeps = mutableExternalDeps.size

      if (numMutableExternalDeps > 0)
        log.debug(s"Mutable external dependencies: ${mutableExternalDeps.to(Seq).sorted.mkString(", ")}")

      if (changedDirectories > 0 || deletedArtifacts > 0 || numMutableExternalDeps > 0)
        f"\tWorkspace changes: ${changedDirectories + deletedArtifacts} [Changed directories: $changedDirectories%,d, Deleted artifacts: $deletedArtifacts%,d, Mutable external dependencies: $numMutableExternalDeps]"
      else ""
    }

    def jvmHitsSummary: String = {
      def jvmHits(cacheType: ObtStats.Cache): Long =
        stats.getOrDefault(cacheType.ScalaHit, 0) + stats.getOrDefault(cacheType.JavaHit, 0)

      val jvmArtifacts =
        stats.getOrDefault(ObtStats.ScalaArtifacts, 0) + stats.getOrDefault(ObtStats.JavaArtifacts, 0)
      val nodeCacheMisses =
        statsSets.getOrDefault(ObtStats.NodeCacheScalaMiss, Set.empty).size +
          statsSets.getOrDefault(ObtStats.NodeCacheJavaMiss, Set.empty).size
      val nodeCacheHits = jvmArtifacts - nodeCacheMisses

      val jvmFilesystemHits = jvmHits(ObtStats.FilesystemStore)
      val jvmSkHits = jvmHits(ObtStats.SilverKing)
      val jvmDhtHits = jvmHits(ObtStats.DHT)
      val zincHits = stats.getOrDefault(ObtStats.ZincCacheHit, 0)

      val jvmTotalCacheHits = nodeCacheHits + jvmFilesystemHits + jvmSkHits + zincHits + jvmDhtHits
      f"\t\tCache hits: $jvmTotalCacheHits%,d [In-memory: $nodeCacheHits%,d, Local disk: $jvmFilesystemHits%,d, SilverKing: $jvmSkHits%,d, DHT: $jvmDhtHits%,d, Incremental: $zincHits%,d]"
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
      val readGitSources = stats.getOrDefault(ObtStats.ReadGitSourceFiles, 0)
      val readSources = readFsSources + readGitSources

      val addedSources = stats.getOrDefault(ObtStats.AddedSourceFiles, 0)
      val modifiedSources = stats.getOrDefault(ObtStats.ModifiedSourceFiles, 0)
      val removedSources = stats.getOrDefault(ObtStats.RemovedSourceFiles, 0)
      val changedSources = addedSources + modifiedSources + removedSources
      val invalidatedSources = stats.getOrDefault(ObtStats.InvalidatedSourceFiles, 0)
      val totalSources = stats.getOrDefault(ObtStats.SourceFiles, 0)
      val externalDependenciesChanged =
        statsSets.getOrDefault(ObtStats.ExternalDependencyChanges, Set.empty).map(_.toString)
      val numExternalDependenciesChanged = externalDependenciesChanged.size

      val compiledLines = stats.getOrDefault(ObtStats.CompiledLinesOfCode, 0)
      val totalLines = stats.getOrDefault(ObtStats.LinesOfCode, 0)

      if (numExternalDependenciesChanged > 0)
        log.debug(s"Changed external dependencies: ${externalDependenciesChanged.to(Seq).sorted.mkString(", ")}")

      val readSourcesStr =
        if (readSources > 0)
          f"\t\tSource files read: $readSources%,d [Local disk: $readFsSources%,d, Git: $readGitSources%,d]"
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

    def pythonHitsSummary: String = {
      def pythonHits(cacheType: ObtStats.Cache): Long =
        stats.getOrDefault(cacheType.PythonHit, 0)

      val pythonFilesystemHits = pythonHits(ObtStats.FilesystemStore)
      val pythonSkHits = pythonHits(ObtStats.SilverKing)
      val pythonDhtHits = pythonHits(ObtStats.DHT)
      // Note: We don't include node cache hits here because there's no easy way to calculate them
      val pythonTotalCacheHits = pythonFilesystemHits + pythonSkHits + pythonDhtHits

      f"\t\tCache hits: $pythonTotalCacheHits%,d [Local disk: $pythonFilesystemHits%,d, SilverKing: $pythonSkHits%,d, DHT: $pythonDhtHits%,d]"
    }

    if (success) {
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
        s"$pythonSummary",
      ).filter(!_.isBlank).mkString("\n")
      log.info(msg)
      ObtTrace.info(msg)
    }

    stats.clear()
    statsSets.clear()
    true
  }
}
