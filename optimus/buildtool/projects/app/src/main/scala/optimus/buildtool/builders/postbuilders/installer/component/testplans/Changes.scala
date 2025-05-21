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
package optimus.buildtool.builders.postbuilders.installer.component.testplans

import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.runconf.RunConf
import optimus.platform._

@entity class Changes(
    val changes: Option[Set[ScopeId]],
    val scopeConfigSource: ScopeConfigurationSource,
) {

  def isDefined: Boolean = changes.isDefined

  /**
   * Entry is considered as 'changed' when:
   *
   *   - it has changes,
   *   - any of its runtime deps has changed, or
   *   - there are changes in its parent directory.
   */
  @node def onlyChanged(data: TestplanEntry, includedRunconfs: Set[RunConf]): Option[TestplanEntry] = {
    val filteredTasks = data.testTasks.apar.filter(task => taskDependenciesChanged(task, includedRunconfs))

    if (filteredTasks.nonEmpty) Some(data.copyEntry(testTasks = filteredTasks))
    else None
  }

  @node def directlyChangedScopes: Set[ScopeId] = changes.getOrElse(Set.empty)

  @node private def taskDependenciesChanged(task: TestplanTask, includedRunconfs: Set[RunConf]): Boolean = {
    val contractTestNames = Set("consumerContractTest", "providerContractTest")
    val scope = includedRunconfs
      .find(r => {
        val isMatchingModule = r.runConfId.moduleScoped == task.moduleScoped

        // Special handling for pact testing where the scope type is different
        // TODO (OPTIMUS-70267):  Rework this as part of restructuring contract testing logic
        val isMatchingPactContractModule =
          r.runConfId.scope.fullModule == task.module && task.testName == "pactContractTest" && contractTestNames
            .contains(r.runConfId.name)

        isMatchingModule || isMatchingPactContractModule
      })
      .map(_.runConfId.scope)

    val scopeExists = scope.exists(scopeDependenciesChanged)

    log.debug(
      s"dependencies for module: ${task.module}, testName: ${task.testName}, scope: ${scope}, scopeExists: ${scopeExists}")

    scopeExists
  }

  @node def scopeDependenciesChanged(scopeId: ScopeId): Boolean = changes.forall { c =>
    val allDeps = dependencies(scopeId)
    val hasScope = allDeps.intersect(c).nonEmpty
    hasScope
  }

  @node private def dependencies(id: ScopeId): Set[ScopeId] = {
    // internalRuntimeDependencies are not transitive, we need to go go down recursively
    val deps = scopeConfigSource.scopeConfiguration(id).internalRuntimeDependencies.toSet
    deps.apar.flatMap(d => dependencies(d)) + id
  }

}
