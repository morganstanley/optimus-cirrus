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

import java.nio.file.Path
import java.nio.file.Paths
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.utils.GitLog
import optimus.platform._
import org.eclipse.jgit.diff.DiffEntry.ChangeType

@entity object Changes {

  private val prefix = "[Dynamic Scoping] "

  @node def apply(
      git: Option[GitLog],
      scopeConfigSource: ScopeConfigurationSource,
      ignoredPaths: Seq[String]
  ): Changes = {
    val changes = git.map(g => changedScopes(g, scopeConfigSource, ignoredPaths))
    Changes(changes, scopeConfigSource)
  }

  @node private def changedScopes(
      git: GitLog,
      scopeConfigSource: ScopeConfigurationSource,
      ignoredPaths: Seq[String]): Set[ScopeId] = {
    val ignoredPatterns = ignoredPaths.map(_.r.pattern)
    val paths = git
      .diff("HEAD^1", "HEAD")
      .map { entry =>
        Paths.get(if (entry.getChangeType == ChangeType.DELETE) entry.getOldPath else entry.getNewPath)
      }
      .filterNot(p => ignoredPatterns.exists(_.matcher(p.toString).matches))
      // optimization for huge PRs, map to parent dir to get the # of entries lower
      .map(path => Option(path.getParent).getOrElse(Paths.get("")))
      .distinct

    paths.apar.flatMap { path =>
      val scopes = pathToScopes(path, scopeConfigSource)
      log.debug(s"${prefix}Mapped $path to $scopes")
      scopes
    }.toSet
  }

  @node private def pathToScopes(path: Path, scopeConfigSource: ScopeConfigurationSource): Set[ScopeId] = {
    val partialId = maybeScopeId(path, scopeConfigSource)

    scopeConfigSource.tryResolveScopes(partialId).getOrElse {
      log.debug(s"could not resolve path=$path, partialId=$partialId to a scope. Trying parent")
      // if we get to root we don't want 'null' here
      pathToScopes(Option(path.getParent).getOrElse(Paths.get("")), scopeConfigSource)
    }
  }

  @node private def maybeScopeId(path: Path, scopeConfigSource: ScopeConfigurationSource): String = {
    // find a sub-scope that matches
    scopeConfigSource.compilationScopeIds
      .find { scopeId =>
        val scopeConfig = scopeConfigSource.scopeConfiguration(scopeId)
        path.startsWith(scopeConfig.paths.scopeRoot.path)
      } match {
      // yay, we've found our scope
      case Some(scopeId) => scopeId.toString
      // not a file in any scope, just some dirs, let's found out which meta/bundle/module those are in
      case None => path.toString.split("/").filterNot(_ == "projects").take(3).mkString(".")
    }
  }

}

@entity final class Changes private (
    changes: Option[Set[ScopeId]],
    scopeConfigSource: ScopeConfigurationSource
) {
  changes.foreach(c => log.debug(s"Changed scopes: $c"))

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
    val scope = includedRunconfs.find(_.runConfId.moduleScoped == task.moduleScoped).map(_.runConfId.scope)
    scope.exists(scopeDependenciesChanged)
  }

  @node def scopeDependenciesChanged(scopeId: ScopeId): Boolean = changes.forall { c =>
    val allDeps = dependencies(scopeId)
    allDeps.intersect(c).nonEmpty
  }

  @node private def dependencies(id: ScopeId): Set[ScopeId] = {
    // internalRuntimeDependencies are not transitive, we need to go go down recursively
    val deps = scopeConfigSource.scopeConfiguration(id).internalRuntimeDependencies.toSet
    deps.apar.flatMap(d => dependencies(d)) + id
  }

}
