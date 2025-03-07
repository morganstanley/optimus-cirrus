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
package optimus.buildtool.builders.postbuilders.installer.component.fingerprint_diffing

import optimus.buildtool.artifactcomparator.BuildArtifactComparatorParamsLocation
import optimus.buildtool.builders.postbuilders.installer.component.testplans.Changes
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.platform._

import java.io.FileNotFoundException
import java.nio.file.Path

@entity object FingerprintDiffChanges {
  @node def apply(scopeConfigSource: ScopeConfigurationSource, srcPath: Path): Changes = {
    Changes(changesAsScopes(scopeConfigSource, srcPath), scopeConfigSource)
  }

  @node private def changesAsScopes(
      scopeConfigSource: ScopeConfigurationSource,
      srcPath: Path): Option[Set[ScopeId]] = {
    val depsEither = BuildArtifactComparatorParamsLocation.loadDeps(srcPath)
    depsEither match {
      case Left(throwable) =>
        throwable match {
          case e: FileNotFoundException =>
            // Prepare Build Artifact App did not generate file with dependencies, ignore
            log.debug(e.toString)
            None
          case _ =>
            throw throwable
        }
      case Right(deps) =>
        val artifactComparator = BuildArtifactComparatorApp(deps, scopeConfigSource)
        val fingerprintChangedScopes = artifactComparator.run()
        Some(fingerprintChangedScopes)
    }
  }
}
