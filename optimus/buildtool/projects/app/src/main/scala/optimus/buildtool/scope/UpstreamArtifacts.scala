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
package optimus.buildtool.scope

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactId
import optimus.buildtool.artifacts.Artifacts
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.IncrementalArtifact
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.InternalCppArtifact
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.artifacts.ResolutionArtifact
import optimus.platform._

import scala.collection.immutable.{IndexedSeq, Seq}
import scala.collection.mutable

@entity object UpstreamArtifacts {

  // distinctLast, but also remove:
  // - duplicate ClassFileArtifacts which differ only in containsOrUsedByMacros
  // - duplicate artifacts which differ only in incrementality
  @node private[scope] def distinctArtifacts(artifactss: IndexedSeq[IndexedSeq[Artifact]]): IndexedSeq[Artifact] = {
    val artifactIdsUsedByMacros = mutable.Set[ArtifactId]()
    val incrementalArtifactIds = mutable.Set[ArtifactId]()
    for {
      artifacts <- artifactss
      artifact <- artifacts
    } {
      artifact match {
        case cfa: ClassFileArtifact if cfa.containsOrUsedByMacros => artifactIdsUsedByMacros += cfa.id
        case _                                                    =>
      }
      artifact match {
        case ia: IncrementalArtifact if ia.incremental => incrementalArtifactIds += ia.id
        case _                                         =>
      }
    }

    val builder = Vector.newBuilder[Artifact]
    val seenById = mutable.Set[ArtifactId]()
    val seenOthers = mutable.Set[Artifact]() // not all artifacts have unique IDs, so use the full artifact here
    val it = artifactss.reverseIterator.flatMap(_.reverseIterator)
    while (it.hasNext) {
      it.next() match {
        case icfa: InternalClassFileArtifact =>
          if (seenById.add(icfa.id)) {
            val containsOrUsedByMacros = artifactIdsUsedByMacros.contains(icfa.id)
            val incremental = incrementalArtifactIds.contains(icfa.id)
            val updated =
              if (icfa.incremental != incremental || icfa.containsOrUsedByMacros != containsOrUsedByMacros)
                icfa.copyArtifact(incremental = incremental, containsOrUsedByMacros = containsOrUsedByMacros)
              else icfa
            builder += updated
          }
        case cfa: ClassFileArtifact =>
          if (seenById.add(cfa.id)) {
            val containsOrUsedByMacros = artifactIdsUsedByMacros.contains(cfa.id)
            val updated =
              if (cfa.containsOrUsedByMacros != containsOrUsedByMacros)
                cfa.copy(containsOrUsedByMacros = containsOrUsedByMacros)
              else cfa
            builder += updated
          }
        case ia: IncrementalArtifact =>
          if (seenById.add(ia.id)) {
            val incremental = incrementalArtifactIds.contains(ia.id)
            val updated =
              if (ia.incremental != incremental)
                ia.withIncremental(incremental = incremental)
              else ia
            builder += updated
          }
        case fp: FingerprintArtifact =>
          builder += fp
        case a =>
          if (seenOthers.add(a)) builder += a
      }
    }

    val messages = if (OptimusBuildToolAssertions.enabled) {
      // Check that we've not got duplicate IDs in ClassFileArtifacts or IncrementalArtifacts
      val distinct = artifactss.flatten.apar.map {
        case icfa: InternalClassFileArtifact =>
          icfa.copyArtifact(containsOrUsedByMacros = false, incremental = false)
        case ecfa: ExternalClassFileArtifact =>
          ecfa.copy(containsOrUsedByMacros = false)
        case ia: IncrementalArtifact =>
          ia.withIncremental(false)
        case a => a
      }.distinct

      Artifacts.checkDuplicates(distinct)
    } else Nil

    builder.result().reverse ++ messages
  }
  distinctArtifacts_info.setCacheable(false)
}

@entity private[buildtool] class UpstreamArtifacts(
    private[scope] val compileDependencies: ScopeDependencies,
    private[scope] val compileOnlyDependencies: ScopeDependencies,
    val runtimeDependencies: ScopeDependencies
) {
  import UpstreamArtifacts._

  def allCompileDependencies: IndexedSeq[ScopeDependencies] = Vector(compileDependencies, compileOnlyDependencies)

  @node def signaturesForOurCompiler: IndexedSeq[Artifact] =
    distinctArtifacts(Vector(signaturesForDownstreamCompilers, signaturesForOurCompilerOnly))

  @node private def signaturesForOurCompilerOnly: IndexedSeq[Artifact] =
    // ask our upstreams what we need (it will vary depending on whether or not they have macros)
    distinctArtifacts(
      compileOnlyDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.signaturesForDownstreamCompilers.toVector))

  @node def signaturesForDownstreamCompilers: IndexedSeq[Artifact] =
    // ask our upstreams what we need (it will vary depending on whether or not they have macros)
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.signaturesForDownstreamCompilers))

  @node def classesForOurCompiler: IndexedSeq[Artifact] =
    distinctArtifacts(Seq(classesForDownstreamCompilers, classesForOurCompilerOnly).toVector)

  @node private def classesForOurCompilerOnly: IndexedSeq[Artifact] =
    distinctArtifacts(
      compileOnlyDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.classesForDownstreamCompilers))

  @node def classesForDownstreamCompilers: IndexedSeq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.classesForDownstreamCompilers))

  @node def pluginsForOurCompiler: IndexedSeq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.pluginsForDownstreamCompilers))

  @node def cppForOurOsCompiler(osVersion: String): IndexedSeq[Artifact] =
    cppForOurCompiler.flatMap {
      case a: InternalCppArtifact if a.osVersion == osVersion => Some(a)
      case _: InternalCppArtifact                             => None
      case a                                                  => Some(a)
    }.toVector

  @node def cppForOurCompiler: IndexedSeq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.cppForDownstreamCompilers))

  @node def internalAgentsForOurRuntime: IndexedSeq[Artifact] =
    distinctArtifacts(
      runtimeDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.agentsForDownstreamRuntimes))

  @node def artifactsForOurRuntime: IndexedSeq[Artifact] =
    // ask our upstreams for full runtime artifacts
    distinctArtifacts(
      runtimeDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .map(_.artifactsForDownstreamRuntimes))

  @node def agentsForOurRuntime: IndexedSeq[Artifact] = {
    val externalArtifacts = runtimeDependencies.transitiveExternalArtifacts
    val resolvedArtifacts =
      externalArtifacts
        .collect { case c: ResolutionArtifact => c.result.resolvedArtifacts }
        .flatten
        .filter(_.containsAgent)

    internalAgentsForOurRuntime ++ resolvedArtifacts
  }

  @node def agentsForOurRuntimeArtifacts: IndexedSeq[ClassFileArtifact] = agentsForOurRuntime.collect {
    case c: ClassFileArtifact => c
  }

  @node def allUpstreamArtifacts: IndexedSeq[Artifact] =
    // ask our upstreams for full compile, compileOnly and runtime artifacts
    distinctArtifacts(
      (compileDependencies.directScopeDependencies ++
        compileOnlyDependencies.directScopeDependencies ++
        runtimeDependencies.directScopeDependencies).distinct
        .sortBy(_.id.toString)
        .apar
        .map(_.allArtifacts.all)
    )
}
