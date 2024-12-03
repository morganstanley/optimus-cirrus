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
package optimus.buildtool.builders.reporter

import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.builders.BuildResult.CompletedBuildResult
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.format.OutputMappings
import optimus.platform._

import scala.collection.immutable.Seq

class ScopedClasspathReporter {

  @async def writeClasspathReports(
      buildDir: Directory,
      buildResult: CompletedBuildResult,
      factory: ScopedCompilationFactory): Unit = {
    if (buildResult.successful) {
      val compiledArtifacts = buildResult.artifacts
      val compiledScopeIds = Artifact.scopeIds(compiledArtifacts).toSet
      val bundleScopeIds = factory.scopeConfigSource.pathingBundles(compiledScopeIds)
      val bundleArtifacts = compiledArtifacts.collect {
        case Artifact.InternalArtifact(id, a: PathingArtifact) if bundleScopeIds.contains(id.scopeId) =>
          id.scopeId.metaBundle -> a
      }.toGroupedMap
      storeClasspathMapping(buildDir, compiledArtifacts, bundleArtifacts)
    }
  }

  /**
   * Updates the classpath mapping used by IntelliJ to run apps and tests. Note that we update rather than overwrite so
   * that modules built in previous builds but not in this one are not removed from the mapping.
   */
  private def storeClasspathMapping(
      buildDir: Directory,
      artifacts: Seq[Artifact],
      bundleArtifacts: Map[MetaBundle, Seq[PathingArtifact]]): Unit = {
    val updatedScopes = artifacts.collect { case p: PathingArtifact =>
      val scopeId = p.id.scopeId
      val pathingArtifacts = p +: bundleArtifacts.getOrElse(scopeId.metaBundle, Nil)
      p.id.scopeId.properPath -> pathingArtifacts.map(_.pathingFile.pathString)
    }.toSingleMap

    val plainDest = buildDir.resolveFile(NamingConventions.ClassPathMapping)
    OutputMappings.updateClasspathPlain(plainDest, updatedScopes)
  }
}
