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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.config.RelaxedScopeIdString
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.Keys.KeySet

import scala.collection.immutable.Seq

object ForbiddenDependencyConfigurationCompiler {
  private val forbiddenDependenciesKey = "forbiddenDependencies"
  private val dependencyIdKey = "dependencyId"
  private val dependencyRegexKey = "dependencyRegex"
  private val configurationsKey = "configurations"
  private val transitiveKey = "transitive"
  private val isExternalKey = "isExternal"
  private val allowedInKey = "allowedIn"
  private val idsKey = "ids"
  private val patternsKey = "patterns"

  def loadForbiddenDependencies(config: Config, origin: ObtFile): Result[Seq[ForbiddenDependencyConfiguration]] =
    Result
      .tryWith(origin, config) {
        Result
          .sequence({
            config
              .configs(forbiddenDependenciesKey)
              .map { cfg =>
                loadForbiddenDependency(cfg, origin)
              }
          })
      }

  private def loadForbiddenDependency(config: Config, origin: ObtFile): Result[ForbiddenDependencyConfiguration] =
    Result
      .tryWith(origin, config) {
        for {
          dependencyId <- Success(config.optionalString(dependencyIdKey))
          dependencyRegex <- Success(config.optionalString(dependencyRegexKey))
          configurations <- Success(config.stringListOrEmpty(configurationsKey))
          transitive <- Success(config.booleanOrDefault(transitiveKey, default = false))
          isExternal <- Success(config.booleanOrDefault(isExternalKey, default = false))
          allowedInIds <- loadAllowedIn(config, idsKey)
          allowedInPatterns <- loadAllowedIn(config, patternsKey)
        } yield {
          ForbiddenDependencyConfiguration(
            dependencyId,
            dependencyRegex,
            configurations,
            transitive,
            isExternal,
            allowedInIds.map(d => RelaxedScopeIdString.asPartial(d)),
            allowedInPatterns
          )
        }
      }
      .withProblems(
        config.checkExclusiveProperties(origin, KeySet(dependencyIdKey, dependencyRegexKey)) ++
          config.checkEmptyProperties(origin, KeySet(dependencyIdKey, dependencyRegexKey)) ++
          isExternalFlagRequiredCheck(origin, config) ++
          config.checkExtraProperties(origin, Keys.forbiddenDependencyKeys) ++ config
            .optionalConfig(allowedInKey)
            .map(c => c.checkExtraProperties(origin, Keys.dependencyAllowedInKeys))
            .getOrElse(Seq.empty))

  private def loadAllowedIn(config: Config, key: String): Result[Seq[String]] =
    Success(config.optionalConfig(allowedInKey).map(c => c.stringListOrEmpty(key)).getOrElse(Seq.empty))

  private def isExternalFlagRequiredCheck(origin: ObtFile, config: Config): Option[Message] =
    if (config.hasPath(dependencyIdKey) && config.hasPath(isExternalKey))
      Some(
        origin.errorAt(
          v = config.getValue(isExternalKey),
          msg =
            s"Redundant '$isExternalKey' key, Forbidden dependencies defined using '$dependencyIdKey' will always be internal"
        ))
    else if (config.hasPath(dependencyRegexKey) && !config.hasPath(isExternalKey))
      Some(
        origin.errorAt(
          v = config.root(),
          msg =
            s"Missing '$isExternalKey' key. Forbidden dependencies defined using '$dependencyRegexKey' must specify if external"
        ))
    else None
}
