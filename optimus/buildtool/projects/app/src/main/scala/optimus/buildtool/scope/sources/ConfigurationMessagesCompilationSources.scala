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
package optimus.buildtool.scope.sources

import optimus.buildtool.artifacts.ArtifactType.ValidationMessagesFingerprint
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.ExternalDependencyResolver
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.ScopeDependencies
import optimus.platform._

import scala.collection.immutable.Seq

@entity
class ConfigurationMessagesCompilationSources(
    scope: CompilationScope,
    scopeDependencies: Seq[ScopeDependencies],
    forbiddenDependencies: Seq[ForbiddenDependencyConfiguration],
    externalDependencyResolver: ExternalDependencyResolver,
) extends CompilationSources {

  override def id: ScopeId = scope.id

  @node def nonEmpty: Boolean = forbiddenDependencies.nonEmpty

  @node def includeTransitive: Boolean = forbiddenDependencies.exists(_.transitive)

  @node private def forbiddenDepsFingerprint =
    forbiddenDependencies.map(_.fingerprint)

  @node private def dependenciesFingerprint = {
    if (includeTransitive)
      scopeDependencies.apar.flatMap(d =>
        externalDependencyResolver.fingerprintDependencies(d.transitiveExternalDependencyIds.all)
          ++ d.transitiveInternalDependencyIdsAll.map(_.toString))
    else
      scopeDependencies.apar.flatMap(d =>
        externalDependencyResolver.fingerprintDependencies(d.dualExternalDependencyIds)
          ++ d.internalDependencyIds.map(_.toString))
  }

  @node override protected def hashedSources: HashedSources = {
    val hash =
      scope.hasher.hashFingerprint(forbiddenDepsFingerprint ++ dependenciesFingerprint, ValidationMessagesFingerprint)
    // `content = Nil` since we don't depend on the content, we depend on dependencies
    // `generatedSourceArtifacts = Nil` since we're not dependent on any source generation
    HashedSourcesImpl(content = Nil, generatedSourceArtifacts = Nil, hash)
  }

}
