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
package optimus.buildtool.builders.postbuilders.installer.component.fingerprintdiffing

import optimus.buildtool.artifactcomparator.BuildArtifactComparatorParamsLocation
import optimus.buildtool.builders.postbuilders.installer.component.testplans.Changes
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.platform._
import optimus.platform.util.Log

import java.io.FileNotFoundException
import java.nio.file.Path

object FingerprintDiffChanges extends Log {

  @async def create(scopeConfigSource: ScopeConfigurationSource, buildPath: Path, installVersion: String): Changes =
    Changes(changesAsScopes(scopeConfigSource, buildPath, installVersion), scopeConfigSource)

  @async private def changesAsScopes(
      scopeConfigSource: ScopeConfigurationSource,
      buildPath: Path,
      installVersion: String): Option[Set[ScopeId]] =
    BuildArtifactComparatorParamsLocation.loadDeps(buildPath) match {
      case Left(e: FileNotFoundException) =>
        // Prepare Build Artifact App did not generate file with dependencies, ignore
        log.debug(e.toString)
        None
      case Left(throwable) => throw throwable
      case Right(deps) =>
        Some(BuildArtifactComparator(deps, scopeConfigSource, regCopyLocal = false, installVersion).run())
    }
}
