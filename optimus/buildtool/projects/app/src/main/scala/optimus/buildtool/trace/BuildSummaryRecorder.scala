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
    if (success) {
      val scopes = stats.getOrDefault(ObtStats.Scopes, 0)
      val transitiveScopes = stats.getOrDefault(ObtStats.TransitiveScopes, 0)

      val changedDirectories = stats.getOrDefault(ObtStats.ModifiedDirectories, 0)
      val deletedArtifacts = stats.getOrDefault(ObtStats.DeletedArtifacts, 0)
      val mutableExternalDeps = stats.getOrDefault(ObtStats.MutableExternalDependencies, 0) > 0

      val jvmArtifacts =
        stats.getOrDefault(ObtStats.ScalaArtifacts, 0) + stats.getOrDefault(ObtStats.JavaArtifacts, 0)
      val nodeCacheMisses =
        statsSets.getOrDefault(ObtStats.NodeCacheScalaMiss, Set.empty).size +
          statsSets.getOrDefault(ObtStats.NodeCacheJavaMiss, Set.empty).size
      val nodeCacheHits = jvmArtifacts - nodeCacheMisses

      def hits(cacheType: ObtStats.Cache): Long =
        stats.getOrDefault(cacheType.ScalaHit, 0) + stats.getOrDefault(cacheType.JavaHit, 0)
      val filesystemHits = hits(ObtStats.FilesystemStore)
      val skHits = hits(ObtStats.SilverKing)
      val zincHits = stats.getOrDefault(ObtStats.ZincCacheHit, 0)
      // Note: We don't include node cache hits here because there's no easy way to calculate them
      val totalCacheHits = nodeCacheHits + filesystemHits + skHits + zincHits

      val scopeCompilations = stats.getOrDefault(ObtStats.Compilations, 0)

      val dueToSource = stats.getOrDefault(ObtStats.CompilationsDueToSourceChanges, 0)
      val dueToUpstreamApi = stats.getOrDefault(ObtStats.CompilationsDueToUpstreamApiChanges, 0)
      val dueToUpstreamAnnotation = stats.getOrDefault(ObtStats.CompilationsDueToUpstreamAnnotationChanges, 0)
      val dueToUpstreamTraits = stats.getOrDefault(ObtStats.CompilationsDueToUpstreamTraitChanges, 0)
      val dueToUpstreamMacros = stats.getOrDefault(ObtStats.CompilationsDueToUpstreamMacroChanges, 0)
      val dueToExternalDeps = stats.getOrDefault(ObtStats.CompilationsDueToExternalDependencyChanges, 0)
      val dueToJavaSigs = stats.getOrDefault(ObtStats.CompilationsDueToNonIncrementalJavaSignatures, 0)
      val dueToUnknown = stats.getOrDefault(ObtStats.CompilationsDueToUnknownReason, 0)

      val causes = if (scopeCompilations > 0) {
        val c = Seq(
          "Source changes" -> dueToSource,
          "Upstream API changes" -> dueToUpstreamApi,
          "Upstream annotation changes" -> dueToUpstreamAnnotation,
          "Upstream trait changes" -> dueToUpstreamTraits,
          "Upstream macro changes" -> dueToUpstreamMacros,
          "External dependency changes" -> dueToExternalDeps,
          "Java signatures" -> dueToJavaSigs,
          "Unknown" -> dueToUnknown
        ).collect { case (reason, num) if num > 0 => f"$reason: $num%,d" }.mkString(", ")
        s", due to [$c]"
      } else ""

      val readFsSources = stats.getOrDefault(ObtStats.ReadFileSystemSourceFiles, 0)
      val readGitSources = stats.getOrDefault(ObtStats.ReadGitSourceFiles, 0)
      val readSources = readFsSources + readGitSources

      val addedSources = stats.getOrDefault(ObtStats.AddedSourceFiles, 0)
      val modifiedSources = stats.getOrDefault(ObtStats.ModifiedSourceFiles, 0)
      val removedSources = stats.getOrDefault(ObtStats.RemovedSourceFiles, 0)
      val changedSources = addedSources + modifiedSources + removedSources
      val invalidatedSources = stats.getOrDefault(ObtStats.InvalidatedSourceFiles, 0)
      val totalSources = stats.getOrDefault(ObtStats.SourceFiles, 0)
      val externalDependenciesChanged = statsSets.getOrDefault(ObtStats.ExternalDependencyChanges, Set.empty).size

      val compiledLines = stats.getOrDefault(ObtStats.CompiledLinesOfCode, 0)
      val totalLines = stats.getOrDefault(ObtStats.LinesOfCode, 0)

      val workspaceChangesStr =
        if (changedDirectories > 0 || deletedArtifacts > 0 || mutableExternalDeps)
          f"Workspace changes: ${changedDirectories + deletedArtifacts} [Changed directories: $changedDirectories%,d, Deleted artifacts: $deletedArtifacts%,d, Mutable external dependencies: $mutableExternalDeps]"
        else ""

      val readSourcesStr =
        if (readSources > 0)
          f"Source files read: $readSources%,d [Local disk: $readFsSources%,d, Git: $readGitSources%,d]"
        else ""
      val changedSourcesStr =
        if (changedSources > 0) f"Source files changed: $changedSources%,d" else ""
      val externalDependenciesChangedStr =
        if (externalDependenciesChanged > 0) f"External dependencies changed: $externalDependenciesChanged%,d" else ""
      val invalidatedSourcesStr =
        if (invalidatedSources > 0)
          f"Source files recompiled: $invalidatedSources%,d/$totalSources%,d (Source lines: $compiledLines%,d/$totalLines%,d)"
        else ""

      val msg = Seq(
        "Build Summary:",
        f"\tTotal scopes: ${scopes + transitiveScopes}%,d [Direct: $scopes%,d, Transitive: $transitiveScopes%,d]",
        s"\t$workspaceChangesStr",
        "\tJava/Scala:",
        f"\t\tCache hits: $totalCacheHits%,d [In-memory: $nodeCacheHits%,d, Local disk: $filesystemHits%,d, SilverKing: $skHits%,d, Incremental: $zincHits%,d]",
        f"\t\tScope compilations: $scopeCompilations%,d$causes",
        s"\t\t$readSourcesStr",
        s"\t\t$changedSourcesStr",
        s"\t\t$externalDependenciesChangedStr",
        s"\t\t$invalidatedSourcesStr"
      ).filter(!_.isBlank).mkString("\n")
      log.info(msg)
      ObtTrace.info(msg)
    }
    stats.clear()
    statsSets.clear()
    true
  }
}
