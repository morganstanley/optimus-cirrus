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
package optimus.buildtool.scope.partial

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.compilers.ConfigurationValidator
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.ScopeDependencies
import optimus.buildtool.scope.sources.ConfigurationMessagesCompilationSources
import optimus.buildtool.trace.ConfigurationValidation
import optimus.platform._

import scala.collection.immutable.Seq

@entity
class ConfigurationMessagesScopedCompilation(
    val scope: CompilationScope,
    val sources: ConfigurationMessagesCompilationSources,
    forbiddenDependencies: Seq[ForbiddenDependencyConfiguration],
    allDependencies: Seq[ScopeDependencies])
    extends PartialScopedCompilation {

  @node private def scopeId = scope.id
  @node override protected def upstreamArtifacts: Seq[Artifact] = Nil
  @node override protected def containsRelevantSources: Boolean = forbiddenDependencies.nonEmpty

  @node
  def messages: Seq[Artifact] =
    compile(ArtifactType.ValidationMessages, None) {
      val artifactId = InternalArtifactId(scope.id, ArtifactType.ValidationMessages, None)
      val jsonFile = scope.pathBuilder
        .outputPathFor(scopeId, fingerprint.hash, ArtifactType.ValidationMessages, None, incremental = false)
        .asJson
      val configurationValidator = ConfigurationValidator(scopeId, forbiddenDependencies)
      // Only do the forbidden dependencies check if the forbidden dependencies defined are valid (no duplicate definitions)
      val messages = configurationValidator.duplicateDefinitionsCheck match {
        case Some(messages) => messages
        case None =>
          allDependencies.apar.flatMap(sd =>
            configurationValidator.validate(
              internalDirectDeps = sd.internalDependencyIds,
              externalDirectDeps = sd.dualExternalDependencyIds,
              internalTransitiveDeps = sd.transitiveInternalDependencyIdsAll,
              externalTransitiveDeps = sd.transitiveExternalDependencyIds.all,
              sd.tpe
            ))
      }
      val a =
        CompilerMessagesArtifact.create(artifactId, jsonFile, messages, ConfigurationValidation, incremental = false)
      a.storeJson()
      Some(a)
    }
}
