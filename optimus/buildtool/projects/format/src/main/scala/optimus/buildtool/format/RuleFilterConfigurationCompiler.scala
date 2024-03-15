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
import optimus.buildtool.config.RuleFilterConfiguration
import optimus.buildtool.config.Group
import optimus.buildtool.config.Filter
import optimus.buildtool.config.ScopeId
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.Keys.KeySet

import scala.collection.immutable.Seq

object RuleFilterConfigurationCompiler {
  private val allKey = "all"
  private val anyKey = "any"
  private val excludeKey = "exclude"
  private val filePathsKey = "file-paths"
  private val filtersKey = "filters"
  private val groupsKey = "groups"
  private val nameKey = "name"
  private val inScopesKey = "in-scopes"

  val Empty: RuleFilterConfiguration = RuleFilterConfiguration(Seq.empty)

  private val origin = RuleFiltersConfig

  def load(loader: ObtFile.Loader, validScopes: Set[ScopeId]): Result[RuleFilterConfiguration] =
    loader(origin).flatMap(load(_, validScopes))

  def load(config: Config, validScopes: Set[ScopeId]): Result[RuleFilterConfiguration] =
    RuleFilterConfigurationCompiler.loadRuleFilterConfiguration(config, origin, validScopes)

  def loadRuleFilterConfiguration(
      config: Config,
      origin: ObtFile,
      validScopes: Set[ScopeId]): Result[RuleFilterConfiguration] = {
    val ruleFilterConfiguration = for {
      groups <- loadGroups(config, origin, validScopes)
      filters <- loadFilters(config, origin, groups.getOrElse(Seq.empty))
    } yield {
      RuleFilterConfiguration(filters = filters.getOrElse(Seq.empty))
    }

    ruleFilterConfiguration
      .withProblems(config.checkExtraProperties(origin, Keys.groupsAndFiltersDefinition))
  }

  private def loadGroups(config: Config, origin: ObtFile, validScopes: Set[ScopeId]): Result[Option[Seq[Group]]] = {
    Result
      .tryWith(origin, config) {
        Result
          .optional(config.hasPath(groupsKey)) {
            Result
              .sequence({
                config
                  .configs(groupsKey)
                  .map { cfg =>
                    val group = loadGroup(cfg, origin, validScopes)
                    group
                  }
              })
              .withProblems(config.checkUniqueValuesForKey(origin, groupsKey, nameKey))
          }
      }
  }

  private def loadGroup(config: Config, origin: ObtFile, validScopes: Set[ScopeId]): Result[Group] = {
    Result
      .tryWith(origin, config) {
        for {
          name <- config.nonBlankString(origin, nameKey)
          filePaths <- loadStringList(config, filePathsKey)
          inScopesStrList <- loadStringList(config, inScopesKey)
          inScopes <- validScopesIds(origin, config, inScopesStrList, validScopes)
        } yield {
          Group(name, filePaths, inScopes)
        }
      }
      .withProblems(
        config.checkExtraProperties(origin, Keys.groupsDefinition) ++ config
          .checkExclusiveProperties(origin, KeySet(filePathsKey, inScopesKey)) ++ config
          .checkEmptyProperties(origin, KeySet(filePathsKey, inScopesKey)))
  }

  private def loadFilters(config: Config, origin: ObtFile, validGroups: Seq[Group]): Result[Option[Seq[Filter]]] = {
    Result
      .tryWith(origin, config) {
        Result
          .optional(config.hasPath(filtersKey)) {
            Result
              .sequence(
                config
                  .configs(filtersKey)
                  .map { cfg => loadFilter(cfg, origin, validGroups) } ++ validGroups.map(convertGroupToFilter)
              )
              .withProblems(config.checkUniqueValuesForKey(origin, filtersKey, nameKey))
          }
      }
  }

  private def loadFilter(config: Config, origin: ObtFile, groups: Seq[Group]): Result[Filter] = {
    // .toMap is safe as groups are guaranteed to have a unique name!
    val groupsByName = groups.map(g => g.name -> g).toMap
    Result
      .tryWith(origin, config) {
        for {
          nonBlankName <- config.nonBlankString(origin, nameKey)
          name <- validFilterName(origin, config, nonBlankName, groupsByName)
          all <- findGroupsByName(origin, config, allKey, groupsByName)
          validAll <- singleScopeInAll(origin, config, all)
          any <- findGroupsByName(origin, config, anyKey, groupsByName)
          exclude <- findGroupsByName(origin, config, excludeKey, groupsByName)
        } yield {
          Filter(name = name, all = validAll, any = any, exclude = exclude)
        }
      }
      .withProblems(config.checkExtraProperties(origin, Keys.filtersDefinition) ++ config
        .checkEmptyProperties(origin, KeySet(allKey, anyKey, excludeKey)))
  }

  private def convertGroupToFilter(group: Group): Result[Filter] =
    Success { Filter(name = group.name, all = Seq.empty, any = Seq(group), exclude = Seq.empty) }

  private def loadStringList(config: Config, key: String): Result[Seq[String]] =
    Success(config.stringListOrEmpty(key))

  private def validScopesIds(
      origin: ObtFile,
      config: Config,
      inScopeStrings: Seq[String],
      validScopes: Set[ScopeId]): Result[Set[ScopeId]] =
    for {
      validInScopeIds <- Result.sequence(inScopeStrings.map(config.expandPartialScope(origin, _, validScopes)))
    } yield validInScopeIds.flatten.toSet

  private def findGroupsByName(
      origin: ObtFile,
      config: Config,
      key: String,
      groupsByName: Map[String, Group]): Result[Seq[Group]] = {
    val keyValues = config.stringListOrEmpty(key)
    Result.sequence(keyValues.map { givenName =>
      groupsByName.get(givenName) match {
        case Some(group) => Success(group)
        case None =>
          origin.failure(
            v = config.getValue(key),
            msg = s"'$givenName' group is not a valid group name"
          )
      }
    })
  }

  private def singleScopeInAll(origin: ObtFile, config: Config, groups: Seq[Group]): Result[Seq[Group]] = {
    Result.sequence(groups.map(group =>
      group.inScopes.size match {
        case 0 => Success(group)
        case 1 => Success(group)
        case _ =>
          origin.failure(
            v = config.getValue(allKey),
            msg = s"'${group.name}' is invalid. 'all' can contain at most one scope"
          )
      }))
  }

  private def validFilterName(
      origin: ObtFile,
      config: Config,
      name: String,
      groupsByName: Map[String, Group]): Result[String] = {
    // A Filter can not have the same name as a group due to automatic creation of filters from groups
    groupsByName.get(name) match {
      case Some(group) =>
        origin.failure(
          v = config.getValue(nameKey),
          msg = s"Filter name must not be the same as a group name. Invalid filter name: '${group.name}'"
        )
      case None => Success(name)
    }
  }
}
