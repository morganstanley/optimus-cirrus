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

import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.platform._
import optimus.workflow.filer.model.ScopeFingerprintDiffs

object FingerprintDependencyAnalyzer {
  def toScopeId(scope: String): Option[ScopeId] = scope.split("\\.") match {
    case Array(meta: String, bundle: String, module: String, tpe: String) => Some(ScopeId(meta, bundle, module, tpe))
    case _                                                                => None
  }
}

@entity class FingerprintDependencyAnalyzer(
    scopeConfigSource: ScopeConfigurationSource,
    diffs: Set[ScopeFingerprintDiffs]) {
  @node def allCompilationScopes: Set[ScopeId] = scopeConfigSource.compilationScopeIds
  @node def directlyChangedScopes: Set[ScopeId] =
    diffs.apar.map(_.scope).apar.flatMap(FingerprintDependencyAnalyzer.toScopeId)
  @node def runtimeScopesThatDependOnDirectlyChangedScopes: Set[ScopeId] =
    allCompilationScopes.apar.filter(haveDependenciesChanged)

  @node private def haveDependenciesChanged(scopeId: ScopeId): Boolean = {
    val sharedScopes = dependencies(scopeId).intersect(directlyChangedScopes)
    if (sharedScopes.nonEmpty) {
      log.debug(
        s"$scopeId dependency change detected based on: ${sharedScopes.toSeq.sortBy(_.properPath).mkString(",")}")
      true
    } else {
      false
    }
  }

  @node private def dependencies(id: ScopeId): Set[ScopeId] = {
    // internalRuntimeDependencies are not transitive, we need to go down recursively
    val deps = scopeConfigSource.scopeConfiguration(id).internalRuntimeDependencies.toSet
    deps.apar.flatMap(dependencies) + id
  }

}
