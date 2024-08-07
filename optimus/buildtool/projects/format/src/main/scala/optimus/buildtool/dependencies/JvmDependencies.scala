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
package optimus.buildtool.dependencies

import com.typesafe.config.Config
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyGroup
import optimus.buildtool.config.Exclude
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.Substitution
import optimus.buildtool.dependencies.JvmDependenciesLoader.mavenScalaLibName
import optimus.buildtool.format.MavenDefinition

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

class JvmDependencies(
    val dependencies: Seq[DependencyDefinition],
    val mavenDependencies: Seq[DependencyDefinition],
    val multiSourceDependencies: Option[MultiSourceDependencies],
    val nativeDependencies: Map[String, NativeDependencyDefinition],
    val groups: Seq[DependencyGroup],
    val globalExcludes: Seq[Exclude],
    val globalSubstitutions: Seq[Substitution],
    val mavenDefinition: Option[MavenDefinition],
    val scalaMajorVersion: Option[String]
) {

  private def byName(deps: Seq[MultiSourceDependency]) =
    deps.map(d => d.name -> Seq(d.definition)).toMap.withDefaultValue(Seq())

  private def byKey(definitions: Seq[DependencyDefinition]) =
    definitions.map(d => d.key -> Seq(d)).toMap.withDefaultValue(Seq())

  private def byMultiSourceKey(deps: Seq[MultiSourceDependency]) = byKey(deps.map(_.definition))

  private def multiSourceDepsMap(deps: Seq[MultiSourceDependency]) = {
    // name + afs name + maven names
    byName(deps) ++ byMultiSourceKey(deps) ++ byKey(deps.flatMap(_.maven))
  }

  // from jvm-dependencies.obt
  private val loadedMultiSourceDeps = multiSourceDependencies.getOrElse(MultiSourceDependencies(Nil))

  // mixed mode dependencies could be used for any modules
  val multiSourceDepsMap: Map[String, Seq[DependencyDefinition]] = multiSourceDepsMap(
    loadedMultiSourceDeps.multiSourceDeps)
  val mavenOnlyDepsMap: Map[String, Seq[DependencyDefinition]] = multiSourceDepsMap(loadedMultiSourceDeps.mavenOnlyDeps)
  val allMixedModeDependenciesMap: Map[String, Seq[DependencyDefinition]] =
    mavenOnlyDepsMap ++ multiSourceDepsMap

  // afs dependencies
  val jvmAfsDepsByName: Map[String, Seq[DependencyDefinition]] = byName(loadedMultiSourceDeps.afsDefinedDeps)
  val jvmAfsDepsByKey: Map[String, Seq[DependencyDefinition]] = byMultiSourceKey(loadedMultiSourceDeps.afsDefinedDeps)
  val dependenciesAfsByKey: Map[String, Seq[DependencyDefinition]] = byKey(dependencies) // from dependencies.obt

  // maven dependencies
  val mavenDepsByKey: Map[String, Seq[DependencyDefinition]] = byKey(mavenDependencies) // from maven-dependencies.obt
  val jvmMavenDepsByKey: Map[String, Seq[DependencyDefinition]] =
    byKey(loadedMultiSourceDeps.mavenDefinedDeps.flatMap(_.maven))

  val groupsByName: Map[String, Seq[DependencyDefinition]] =
    groups.map(g => g.name -> g.dependencies).toMap.withDefaultValue(Seq())

  // all allowed dependencies for afs modules and mavenOnly modules
  val afsModulesDependenciesMap: Map[String, Seq[DependencyDefinition]] =
    dependenciesAfsByKey ++ jvmAfsDepsByName ++ jvmAfsDepsByKey ++ allMixedModeDependenciesMap
  val mavenOnlyModulesDependenciesMap: Map[String, Seq[DependencyDefinition]] =
    mavenDepsByKey ++ jvmMavenDepsByKey ++ allMixedModeDependenciesMap

  // transitive mapping rules without version, can't be directly used in codetree
  val noVersionDependenciesMap: Map[String, Seq[DependencyDefinition]] =
    multiSourceDepsMap(loadedMultiSourceDeps.noVersionMavenDeps)

  def ++(buildDeps: JvmDependencies): JvmDependencies = {
    def merge[T](a: Seq[T], b: Seq[T]): Seq[T] = (a ++ b).toList.distinct
    def hasAfDefinition(dep: JvmDependencies) = dep.mavenDependencies.nonEmpty

    // Seq.empty ++ jvm-dependencies.obt
    val mergedMultiSourceDeps = merge(
      this.multiSourceDependencies.map(_.loaded).getOrElse(Nil),
      buildDeps.multiSourceDependencies.map(_.loaded).getOrElse(Nil))

    JvmDependencies(
      merge(this.dependencies, buildDeps.dependencies), // Seq.empty++dependencies.obt
      merge(this.mavenDependencies, buildDeps.mavenDependencies), // maven-dependencies.obt++Seq.empty
      Some(MultiSourceDependencies(mergedMultiSourceDeps)),
      this.nativeDependencies ++ buildDeps.nativeDependencies,
      merge(this.groups, buildDeps.groups),
      merge(this.globalExcludes, buildDeps.globalExcludes),
      merge(this.globalSubstitutions, buildDeps.globalSubstitutions),
      if (hasAfDefinition(this)) this.mavenDefinition
      else if (hasAfDefinition(buildDeps)) buildDeps.mavenDefinition
      else None,
      scalaMajorVersion.orElse(this.scalaMajorVersion)
    )
  }

  lazy val gradleVersions: java.util.Map[String, String] =
    (dependencies ++ mavenDependencies)
      .filter(_.variant.forall(!_.configurationOnly))
      .map(dep => dep.gradleKey -> dep.version)
      .toMap
      .asJava

  private def versionsById(id: String, fromMavenLibs: Boolean): Seq[DependencyDefinition] = {
    val searchDeps = {
      val dependencies =
        if (fromMavenLibs) mavenOnlyModulesDependenciesMap // maven only projects or maven release build
        else afsModulesDependenciesMap
      scalaMajorVersion match {
        case Some(scalaStr) =>
          dependencies
            .get(id)
            .orElse(dependencies.get(mavenScalaLibName(id, scalaStr)))
            .getOrElse(Nil)
        case None => dependencies(id)
      }
    }
    groupsByName(id) ++ searchDeps
  }

  lazy val versionsConfig: Config =
    JvmDependenciesLoader.versionsAsConfig(dependencies ++ mavenDependencies)

  def forId(id: String, fromMavenLibs: Boolean = false): Seq[DependencyDefinition] =
    versionsById(id, fromMavenLibs) match {
      case Seq()    => Seq.empty[DependencyDefinition]
      case versions => versions
    }
}

object JvmDependencies {
  def apply(
      dependencies: Seq[DependencyDefinition],
      mavenDependencies: Seq[DependencyDefinition],
      multiSourceDependencies: Option[MultiSourceDependencies],
      nativeDependencies: Map[String, NativeDependencyDefinition],
      groups: Seq[DependencyGroup],
      globalExcludes: Seq[Exclude],
      globalSubstitutions: Seq[Substitution],
      mavenDefinition: Option[MavenDefinition],
      scalaMajorVersion: Option[String]): JvmDependencies =
    new JvmDependencies(
      dependencies,
      mavenDependencies,
      multiSourceDependencies,
      nativeDependencies,
      groups,
      globalExcludes,
      globalSubstitutions,
      mavenDefinition,
      scalaMajorVersion)

  val empty: JvmDependencies = apply(Seq.empty, Seq.empty, None, Map.empty, Seq.empty, Seq.empty, Seq.empty, None, None)
}
