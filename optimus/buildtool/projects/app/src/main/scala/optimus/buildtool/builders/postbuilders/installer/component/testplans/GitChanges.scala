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
import optimus.buildtool.utils.GitLog
import optimus.platform._
import org.eclipse.jgit.diff.DiffEntry.ChangeType

import java.nio.file.Path
import java.nio.file.Paths

@entity class GitChanges(
    val changesByScope: Option[Map[ScopeId, Set[Path]]],
    val scopeConfigSource: ScopeConfigurationSource
) extends Changes {
  override def scopes: Option[Set[ScopeId]] = changesByScope.map(_.keySet)
}

@entity object GitChanges {
  @node def apply(
      gitOpt: Option[GitLog],
      scopeConfigSource: ScopeConfigurationSource,
      ignoredPaths: Seq[String]
  ): GitChanges = {
    val changesByScopes = gitOpt.map(git => changedPaths(git, ignoredPaths)).map(scopeConfigSource.changesByScopes)
    GitChanges(changesByScopes, scopeConfigSource)
  }

  @node def changedPaths(git: GitLog, ignoredPaths: Seq[String]): Set[Path] = {
    val ignoredPatterns = ignoredPaths.map(_.r.pattern)
    git
      .diff("HEAD^1", "HEAD")
      .map { entry =>
        Paths.get(if (entry.getChangeType == ChangeType.DELETE) entry.getOldPath else entry.getNewPath)
      }
      .filterNot(p => ignoredPatterns.exists(_.matcher(p.toString).matches))
      .toSet
  }
}
