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
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.InternalCppArtifact
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.mutable

@entity private[buildtool] class UpstreamArtifacts(
    private[scope] val compileDependencies: ScopeDependencies,
    private[scope] val compileOnlyDependencies: ScopeDependencies,
    val runtimeDependencies: ScopeDependencies
) {

  def allCompileDependencies: Seq[ScopeDependencies] = Seq(compileDependencies, compileOnlyDependencies)

  @node def signaturesForOurCompiler: Seq[Artifact] =
    distinctArtifacts(signaturesForDownstreamCompilers ++ signaturesForOurCompilerOnly)

  @node private def signaturesForOurCompilerOnly: Seq[Artifact] =
    // ask our upstreams what we need (it will vary depending on whether or not they have macros)
    distinctArtifacts(
      compileOnlyDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.signaturesForDownstreamCompilers))

  @node def signaturesForDownstreamCompilers: Seq[Artifact] =
    // ask our upstreams what we need (it will vary depending on whether or not they have macros)
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.signaturesForDownstreamCompilers))

  @node def classesForOurCompiler: Seq[Artifact] =
    distinctArtifacts(classesForDownstreamCompilers ++ classesForOurCompilerOnly)

  @node private def classesForOurCompilerOnly: Seq[Artifact] =
    distinctArtifacts(
      compileOnlyDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.classesForDownstreamCompilers))

  @node def classesForDownstreamCompilers: Seq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.classesForDownstreamCompilers))

  @node def pluginsForOurCompiler: Seq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.pluginsForDownstreamCompilers))

  @node def cppForOurOsCompiler(osVersion: String): Seq[Artifact] =
    cppForOurCompiler.flatMap {
      case a: InternalCppArtifact if a.osVersion == osVersion => Some(a)
      case _: InternalCppArtifact                             => None
      case a                                                  => Some(a)
    }

  @node def cppForOurCompiler: Seq[Artifact] =
    distinctArtifacts(
      compileDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.cppForDownstreamCompilers))

  @node def artifactsForOurRuntime: Seq[Artifact] =
    // ask our upstreams for full runtime artifacts
    distinctArtifacts(
      runtimeDependencies.directScopeDependencies
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.artifactsForDownstreamRuntimes))

  @node def allUpstreamArtifacts: Seq[Artifact] =
    // ask our upstreams for full compile, compileOnly and runtime artifacts
    distinctArtifacts(
      (compileDependencies.directScopeDependencies ++
        compileOnlyDependencies.directScopeDependencies ++
        runtimeDependencies.directScopeDependencies).distinct
        .sortBy(_.id.toString)
        .apar
        .flatMap(_.allArtifacts.all))

  // distinctLast, but also remove duplicate ClassFileArtifacts which differ only in containsOrUsedByMacros
  @node private def distinctArtifacts(artifacts: Seq[Artifact]): Seq[Artifact] = {
    val artifactIdsUsedByMacros = artifacts.collect {
      case cfa: ClassFileArtifact if cfa.containsOrUsedByMacros => cfa.id
    }.toSet
    val builder = Seq.newBuilder[Artifact]
    val seenClassFiles = mutable.Set[ArtifactId]()
    val seenOthers = mutable.Set[Artifact]() // not all artifacts have unique IDs, so use the full artifact here
    artifacts.reverse.foreach {
      case cfa: ClassFileArtifact =>
        if (seenClassFiles.add(cfa.id)) {
          val updated =
            if (!cfa.containsOrUsedByMacros && artifactIdsUsedByMacros.contains(cfa.id))
              cfa.copy(containsOrUsedByMacros = true)
            else cfa
          builder += updated
        }
      case a =>
        if (seenOthers.add(a)) builder += a
    }
    builder.result().reverse
  }
}
