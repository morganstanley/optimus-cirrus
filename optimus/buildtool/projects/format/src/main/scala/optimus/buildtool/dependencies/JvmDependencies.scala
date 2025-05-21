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
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.Substitution
import optimus.buildtool.dependencies.DependencyLoader.mavenScalaLibName
import optimus.buildtool.format.DependenciesConfig
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.MavenDefinition

import scala.collection.compat._
import scala.collection.immutable.Seq

trait JvmDependencies {
  def dependencies: MultiSourceDependencies
  def dependencySets: Set[DependencySet]
  val variantSets: Set[VariantSet]
  def nativeDependencies: Map[String, NativeDependencyDefinition]
  val groups: Seq[DependencyGroup]
  def globalExcludes: Seq[Exclude]
  def globalSubstitutions: Seq[Substitution]
  def mavenDefinition: MavenDefinition
  def scalaMajorVersion: Option[String]

  private[dependencies] def dependenciesByKey(
      deps: MultiSourceDependencies,
      depGroups: Seq[DependencyGroup],
      onlyMavenKeys: Boolean
  ): Seq[(String, DependencyConfig, Int, Seq[DependencyDefinition])] = {
    // name + afs name + maven names
    val depsByKey = deps.all.flatMap { d =>
      // referencing by name gets us all maven (if defined) or afs
      val byName =
        if (onlyMavenKeys) d.name.collect { case n if d.maven.nonEmpty => (n, d.file, d.line, d.maven) }
        else d.name.map(n => (n, d.file, d.line, d.definitions.preferred))
      // referencing by afs key gets us all maven (if defined) or afs
      val byAfsKey = if (onlyMavenKeys) Nil else d.afs.map(a => (a.key, d.file, a.line, d.definitions.preferred))
      // referencing by maven key gets us just that maven dep
      val byMavenKey = d.maven.map(m => (m.key, d.file, m.line, Seq(m)))
      byName.to(Seq) ++ byAfsKey ++ byMavenKey
    }
    val groupsByKey =
      if (onlyMavenKeys) Nil else depGroups.map(g => (g.name, DependenciesConfig, g.line, g.dependencies))
    (depsByKey ++ groupsByKey).distinct // distinct since multiple ivy configs may include the same maven dep (via extends)
  }

  private def dependencyMap(
      deps: MultiSourceDependencies,
      depGroups: Seq[DependencyGroup],
      onlyMavenKeys: Boolean
  ): Map[String, Seq[DependencyDefinition]] =
    dependenciesByKey(deps, depGroups, onlyMavenKeys).map { case (key, _, _, deps) => key -> deps }.toMap

  // mixed mode dependencies could be used for any modules
  private lazy val allDepsMap: Map[String, Seq[DependencyDefinition]] =
    dependencyMap(dependencies, groups, onlyMavenKeys = false)
  private lazy val mavenDepsMap: Map[String, Seq[DependencyDefinition]] =
    dependencyMap(dependencies, Nil, onlyMavenKeys = true)

  private lazy val dependencySetsMap: Map[DependencySetId, Map[String, Seq[DependencyDefinition]]] =
    dependencySets.map { s =>
      s.id -> dependencyMap(s.dependencies, Nil, onlyMavenKeys = false)
    }.toMap
  private lazy val variantSetsMap: Map[VariantSetId, Map[String, Seq[DependencyDefinition]]] =
    variantSets.map { s =>
      s.id -> dependencyMap(s.dependencies, Nil, onlyMavenKeys = false)
    }.toMap

  def forId(
      id: String,
      moduleSet: Option[ModuleSet] = None,
      fromMavenLibs: Boolean = false
  ): Seq[DependencyDefinition] = {
    val dependencies =
      if (fromMavenLibs) mavenDepsMap // maven only projects or maven release build
      else allDepsMap

    val variantSetDeps = moduleSet.map(_.variantSets.flatMap(variantSetsMap.get).flatten.toMap).getOrElse(Map.empty)
    val dependencySetDeps =
      moduleSet
        .map(_.transitiveNonVariantDependencySets.flatMap(dependencySetsMap.get).flatten.toMap)
        .getOrElse(Map.empty)

    val allDeps = dependencies ++ dependencySetDeps ++ variantSetDeps
    scalaMajorVersion match {
      case Some(scalaStr) =>
        allDeps
          .get(id)
          .orElse(allDeps.get(mavenScalaLibName(id, scalaStr)))
          .getOrElse(Nil)
      case None => allDeps.getOrElse(id, Nil)
    }
  }

  lazy val versionsConfig: Config = JvmDependenciesLoader.versionsAsConfig(dependencies.preferred)

}

final case class JvmDependenciesImpl(
    dependencies: MultiSourceDependencies,
    dependencySets: Set[DependencySet],
    variantSets: Set[VariantSet],
    nativeDependencies: Map[String, NativeDependencyDefinition],
    groups: Seq[DependencyGroup],
    globalExcludes: Seq[Exclude],
    globalSubstitutions: Seq[Substitution],
    mavenDefinition: MavenDefinition,
    scalaMajorVersion: Option[String]
) extends JvmDependencies

object JvmDependencies {
  val empty: JvmDependencies = JvmDependenciesImpl(
    MultiSourceDependencies(Nil),
    Set.empty,
    Set.empty,
    Map.empty,
    Seq.empty,
    Seq.empty,
    Seq.empty,
    MavenDefinition.empty,
    None
  )
}
