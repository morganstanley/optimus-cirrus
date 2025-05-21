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

import optimus.buildtool.config.Id
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.config.ScopeId

final case class ConditionalDefaults(
    name: String,
    ids: Seq[Id],
    moduleSets: Seq[ModuleSetId],
    defaults: ScopeDefaults,
    exclude: Boolean
) {

  private def checkMavenDef(
      id: ScopeId,
      moduleSet: ModuleSet,
      loadedAfDef: MavenDefinition,
      useMavenOnlyRules: Boolean): Boolean = {
    val tpe = id.tpe
    val includeTpe = loadedAfDef.includeConditionals.getOrElse(name, "")
    val excludeTpe = loadedAfDef.excludeConditionals.getOrElse(name, "")
    if (excludeTpe == MavenDefinition.MavenOnlyExcludeKey && useMavenOnlyRules)
      false // force exclude target for mavenOnly modules
    else if (includeTpe == tpe || includeTpe == "all") true // force af build include target Conditional for tpe
    else if (excludeTpe == tpe || excludeTpe == "all") false // force af build exclude target from tpe
    else checkExclude(id, moduleSet) // not covered in MavenDefinition, back to Exclude checking
  }

  private def checkExclude(id: ScopeId, moduleSet: ModuleSet): Boolean = {
    val matches = ids.exists(_.contains(id)) || moduleSets.contains(moduleSet.id)
    if (exclude) !matches else matches
  }

  def appliesTo(
      id: ScopeId,
      moduleSet: ModuleSet,
      mavenDefinition: MavenDefinition,
      useMavenDepsRules: Boolean,
      useMavenOnlyRules: Boolean
  ): Boolean = {
    if (useMavenDepsRules) // for maven build or mavenOnly scope
      checkMavenDef(id, moduleSet, mavenDefinition, useMavenOnlyRules)
    else
      checkExclude(id, moduleSet)
  }

  def isForbiddenDependencyConditional: Boolean =
    defaults.depsAndSources.exists { case (_, scopeDefinition) => scopeDefinition.forbiddenDependencies.nonEmpty }
}

final case class ScopeDefaults(
    depsAndSources: Map[String, InheritableScopeDefinition], // Scope type (eg. "main", "test") -> Scope definition
    conditionals: Seq[ConditionalDefaults]
) {
  private def getOrEmpty(name: String) = depsAndSources.getOrElse(name, InheritableScopeDefinition.empty)

  def forScope(
      id: ScopeId,
      moduleSet: ModuleSet,
      mavenDefinition: MavenDefinition,
      useMavenDepsRules: Boolean,
      useMavenOnlyRules: Boolean
  ): InheritableScopeDefinition = {
    val all = depsAndSources.getOrElse("all", InheritableScopeDefinition.empty)
    val combinedParent = conditionals.foldLeft(all) { (parent, conditional) =>
      if (
        conditional.appliesTo(
          id,
          moduleSet,
          mavenDefinition,
          useMavenDepsRules,
          useMavenOnlyRules) || conditional.isForbiddenDependencyConditional
      )
        conditional.defaults
          .forScope(id, moduleSet, mavenDefinition, useMavenDepsRules, useMavenOnlyRules)
          .withParent(parent)
      else parent
    }
    val combinedDefault = depsAndSources.get(id.tpe).foldRight(combinedParent) { _.withParent(_) }
    combinedDefault
  }

  def withParent(parent: ScopeDefaults): ScopeDefaults = {
    val allKeys = parent.depsAndSources.keySet ++ depsAndSources.keySet
    val newDepsAndSources = allKeys.map { k =>
      k -> getOrEmpty(k).withParent(parent.getOrEmpty(k))
    }.toMap
    ScopeDefaults(
      newDepsAndSources,
      parent.conditionals ++ conditionals,
    )
  }
}

object ScopeDefaults {
  val empty: ScopeDefaults = ScopeDefaults(Map.empty, Seq.empty)

}
