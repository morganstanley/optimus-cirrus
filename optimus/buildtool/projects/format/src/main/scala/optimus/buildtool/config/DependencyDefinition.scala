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
package optimus.buildtool.config

import optimus.buildtool.files.Asset

import scala.collection.compat._
import scala.collection.immutable.Seq

final case class Variant(name: String, reason: String, configurationOnly: Boolean = false)
final case class Exclude(group: Option[String], name: Option[String])
final case class IvyArtifact(name: String, tpe: String, ext: String)

sealed trait Kind
object Kind {
  val values: Seq[Kind] = Seq(LocalDefinition, ExtraLibDefinition)
}
case object LocalDefinition extends Kind
case object ExtraLibDefinition extends Kind

final case class DependencyId(
    group: String,
    name: String,
    variant: Option[String] = None,
    configuration: Option[String] = None,
    keySuffix: Option[String] = None
) extends Id {
  override def contains(scopeId: ScopeId): Boolean = false
  override def elements: Seq[String] = Seq(group, name) ++ variant ++ configuration ++ keySuffix
}

final case class DependencyDefinition(
    group: String,
    name: String,
    version: String,
    kind: Kind,
    configuration: String = "runtime",
    classifier: Option[String] = None,
    excludes: Seq[Exclude] = Nil,
    variant: Option[Variant] = None,
    transitive: Boolean = true,
    force: Boolean = false,
    line: Int = 0,
    keySuffix: String = "",
    containsMacros: Boolean = false,
    isScalacPlugin: Boolean = false,
    ivyArtifacts: Seq[IvyArtifact] = Nil,
    isMaven: Boolean = false,
    isDisabled: Boolean = false
) extends OrderedElement {
  def gradleKey = s"$group:$name:${variant.fold("default")(_ => version)}"

  def key: String = path.mkString(".")

  override def id: DependencyId =
    DependencyId(
      group,
      name,
      variant.filter(_.configurationOnly).map(_.name),
      variant.filter(!_.configurationOnly).map(_.name),
      Some(keySuffix).filter(_.nonEmpty)
    )

  // to use in Groovy
  def excludeArray: Array[Exclude] = excludes.toArray
  // to use in Groovy
  def artifactsArray: Array[IvyArtifact] = ivyArtifacts.toArray

  private def path: Seq[String] = {
    val basePath = variant match {
      case Some(v) =>
        if (v.configurationOnly) Seq(group, name, v.name)
        else Seq(group, name, "variant", v.name)
      case _ =>
        Seq(group, name)
    }
    if (keySuffix.nonEmpty) basePath :+ keySuffix else basePath
  }

  def isScalaSdk: Boolean = id == DependencyDefinition.ScalaId

  def isSameName(to: DependencyDefinition): Boolean = this.group == to.group && this.name == to.name
}

final case class DependencyGroup(name: String, dependencies: Seq[DependencyDefinition])

final case class NativeDependencyId(name: String) extends Id {
  override def contains(scopeId: ScopeId): Boolean = false
  override def elements: Seq[String] = Seq(name)
}

// paths go into LD_LIBRARY_PATH; extraFiles just go into the image
final case class NativeDependencyDefinition(
    line: Int,
    name: String,
    // OPTIMUS-46629: If you think you want this to be Asset or Path or something, go read that JIRA and reconsider.
    paths: Seq[String],
    // TODO (OPTIMUS-37398): this will likely be desirous of includes/excludes and such
    extraPaths: Seq[Asset]
) extends OrderedElement {
  def id: NativeDependencyId = NativeDependencyId(name)
}

object DependencyDefinition {
  val ScalaId = DependencyId("ossscala", "scala")
}

final case class DependencyDefinitions(directIds: Seq[DependencyDefinition], indirectIds: Seq[DependencyDefinition]) {
  val all: Seq[DependencyDefinition] = directIds ++ indirectIds
}

final case class ExternalDependency(definition: DependencyDefinition, equivalents: Seq[DependencyDefinition])

object ExternalDependencies {
  def empty: ExternalDependencies =
    ExternalDependencies(AfsDependencies.empty, MavenDependencies.empty)
}

final case class ExternalDependencies(afsDependencies: AfsDependencies, mavenDependencies: MavenDependencies) {
  val multiSourceDependencies: Seq[ExternalDependency] =
    afsDependencies.disabledAfsMappedDeps ++ afsDependencies.enabledAfsMappedDeps
  val allExternalDependencies: Seq[ExternalDependency] =
    afsDependencies.allAfsDeps ++ mavenDependencies.allMavenDeps
  val definitions: Seq[DependencyDefinition] = allExternalDependencies.map(_.definition).toIndexedSeq
  // be used for direct mapping
  val afsToMavenMap: Map[DependencyDefinition, Seq[DependencyDefinition]] =
    multiSourceDependencies.map(d => d.definition -> d.equivalents).toMap
  val mappedAfsDepsStr: Seq[String] = afsToMavenMap
    .map { case (afs, maven) =>
      s"${afs.group}:${afs.name}:${afs.version}"
    }
    .to(Seq)
  // be used for transitive mapping
  val afsGroupNameToMavenMap: Map[(String, String), Seq[DependencyDefinition]] = afsToMavenMap.map {
    case (afs, maven) =>
      (afs.group, afs.name) -> maven
  }
}

object AfsDependencies {
  def empty: AfsDependencies = AfsDependencies(Nil, Nil, Nil)
}

/**
 * loaded afs dependencies in *dependencies.obt files
 * @param unmappedAfsDeps
 *   only afs dependency be defined
 * @param disabledAfsMappedDeps
 *   both afs{} maven{} be defined, but no .version in afs{} definition
 * @param enabledAfsMappedDeps
 *   both afs{} maven{} be defined
 */
final case class AfsDependencies(
    unmappedAfsDeps: Seq[ExternalDependency],
    disabledAfsMappedDeps: Seq[ExternalDependency],
    enabledAfsMappedDeps: Seq[ExternalDependency]) {
  val allAfsDeps: Seq[ExternalDependency] = unmappedAfsDeps ++ disabledAfsMappedDeps ++ enabledAfsMappedDeps
  val allAfsDisabledMavenDefinitions: Seq[DependencyDefinition] = disabledAfsMappedDeps.flatMap(_.equivalents)
  val allMixModeAllowedCoursierKey: Seq[DependencyCoursierKey] = (disabledAfsMappedDeps ++ enabledAfsMappedDeps).map {
    d =>
      DependencyCoursierKey(d.definition.group, d.definition.name, d.definition.configuration, d.definition.version)
  }
}

object MavenDependencies {
  def empty: MavenDependencies = MavenDependencies(Nil, Nil, Nil)
}

/**
 * loaded maven dependencies in *dependencies.obt files
 * @param unmappedMavenDeps
 *   loaded from maven-dependencies.obt and should only be used in 'mavenLibs = []'
 * @param mappedMavenDeps
 *   both afs{} maven{} be defined
 * @param mixModeMavenDeps
 *   only maven{} be defined, open for all modules
 */
final case class MavenDependencies(
    unmappedMavenDeps: Seq[ExternalDependency],
    mappedMavenDeps: Seq[ExternalDependency],
    mixModeMavenDeps: Seq[ExternalDependency]) {
  val allMavenDeps: Seq[ExternalDependency] = unmappedMavenDeps ++ mappedMavenDeps ++ mixModeMavenDeps
  val allMavenOnlyDefinitions: Seq[DependencyDefinition] = mixModeMavenDeps.map(_.definition)
  val allMixModeAllowedCoursierKey: Seq[DependencyCoursierKey] = (mappedMavenDeps ++ mixModeMavenDeps).map { d =>
    DependencyCoursierKey(d.definition.group, d.definition.name, d.definition.configuration, d.definition.version)
  }
}

final case class MappedDependencyDefinitions(
    appliedAfsToMavenMap: Map[DependencyDefinition, Seq[DependencyDefinition]],
    unMappedAfsDeps: Seq[DependencyDefinition]) {
  val allDepsAfterMapping: Seq[DependencyDefinition] =
    unMappedAfsDeps ++ appliedAfsToMavenMap.values.flatten.toIndexedSeq
}

final case class DependencyCoursierKey(org: String, name: String, configuration: String, version: String) {
  def displayName = s"$org:$name.$configuration.$version"
  // ignore configuration & version diff
  def isSameName(to: DependencyCoursierKey): Boolean = org == to.org && name == to.name
}
