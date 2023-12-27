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
import optimus.buildtool.config.RegexConfiguration
import optimus.buildtool.config.ScopeId

final case class ConditionalDefaults(
    name: String,
    ids: Seq[Id],
    defaults: ScopeDefaults,
    exclude: Boolean
) {

  private def checkMavenDef(id: ScopeId, loadedAfDef: MavenDefinition): Boolean = {
    val tpe = id.tpe
    val includeTpe = loadedAfDef.includeConditionals.getOrElse(name, "")
    val excludeTpe = loadedAfDef.excludeConditionals.getOrElse(name, "")
    if (includeTpe == tpe || includeTpe == "all") true // force af build include target Conditional for tpe
    else if (excludeTpe == tpe || excludeTpe == "all") false // force af build exclude target from tpe
    else checkExclude(id) // not covered in MavenDefinition, back to Exclude checking
  }

  def checkExclude(id: ScopeId): Boolean = {
    val matches = ids.exists(_.contains(id))
    if (exclude) !matches else matches
  }

  def appliesTo(
      id: ScopeId,
      mavenDefinition: Option[MavenDefinition],
      mavenLibs: Boolean
  ): Boolean = {
    mavenDefinition match {
      case Some(afDef) if mavenLibs => // for maven build or mavenOnly scope
        checkMavenDef(id, afDef)
      case _ =>
        checkExclude(id)
    }
  }
}

final case class ScopeDefaults(
    depsAndSources: Map[String, InheritableScopeDefinition], // Scope type (eg. "main", "test") -> Scope definition
    conditionals: Seq[ConditionalDefaults],
    defaultRules: Option[RegexConfiguration]
) {
  private def getOrEmpty(name: String) = depsAndSources.getOrElse(name, InheritableScopeDefinition.empty)

  def forScope(
      id: ScopeId,
      mavenDefinition: Option[MavenDefinition],
      mavenLibs: Boolean
  ): InheritableScopeDefinition = {

    val all = depsAndSources.getOrElse("all", InheritableScopeDefinition.empty)
    val allWithDefaultRules =
      all.copy(regexConfiguration = RegexConfiguration.merge(all.regexConfiguration, defaultRules))
    val combinedParent = conditionals.foldLeft(allWithDefaultRules) { (parent, conditional) =>
      if (conditional.appliesTo(id, mavenDefinition, mavenLibs))
        conditional.defaults.forScope(id, mavenDefinition, mavenLibs).withParent(parent)
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
      RegexConfiguration.merge(defaultRules, parent.defaultRules)
    )
  }
}

object ScopeDefaults {
  val empty: ScopeDefaults = ScopeDefaults(Map.empty, Seq.empty, None)

}
