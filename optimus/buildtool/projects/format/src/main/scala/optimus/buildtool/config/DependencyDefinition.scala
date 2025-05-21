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

import optimus.buildtool.config.DependencyDefinition.DefaultConfiguration
import optimus.buildtool.files.Asset

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.Seq

final case class Variant(name: String, reason: String, configurationOnly: Boolean = false)
final case class Exclude(
    group: Option[String],
    name: Option[String],
    // Only used when resolving from ivy and only supported for global excludes.
    // Not written into published ivy files, and ignored completely for maven deps.
    ivyConfiguration: Option[String] = None)
final case class GroupNameConfig(group: String, name: String, config: Option[String] = None)
final case class Substitution(from: GroupNameConfig, to: GroupNameConfig)
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
    configuration: String = DefaultConfiguration,
    classifier: Option[String] = None,
    excludes: Seq[Exclude] = Nil,
    variant: Option[Variant] = None,
    resolvers: Seq[String] = Nil,
    transitive: Boolean = true,
    force: Boolean = false,
    line: Int = 0,
    keySuffix: String = "",
    containsMacros: Boolean = false,
    isScalacPlugin: Boolean = false,
    ivyArtifacts: Seq[IvyArtifact] = Nil,
    isMaven: Boolean = false,
    isAgent: Boolean = false,
    isExtraLib: Boolean = false // for metadata generation only
) extends OrderedElement[DependencyId] {

  def noVersion: Boolean = version.isEmpty

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

  @transient
  private[this] val fingerprintSuffix =
    s"-dependency-definition:$group:$name:$variant:$version:configuration=$configuration:resolvers=${resolvers
        .mkString(",")}:ivyArtifacts=${ivyArtifacts.mkString(",")}:excludes=${excludes
        .mkString(",")}:transitive=$transitive:force=$force:macros=$containsMacros:plugin=$isScalacPlugin"
  @transient
  private[this] val fingerprintCache = new ConcurrentHashMap[String, String]()
  def fingerprint(tpe: String): String =
    fingerprintCache.computeIfAbsent(tpe, (tpe: String) => s"$tpe$fingerprintSuffix")
}

final case class DependencyGroup(name: String, dependencies: Seq[DependencyDefinition], line: Int)

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
) extends OrderedElement[NativeDependencyId] {
  def id: NativeDependencyId = NativeDependencyId(name)
}

object DependencyDefinition {
  val ScalaId = DependencyId("ossscala", "scala")
  val DefaultConfiguration = "runtime"
}

final case class DependencyDefinitions(
    directIds: Seq[DependencyDefinition],
    indirectIds: Seq[DependencyDefinition],
    substitutions: Seq[Substitution] = Nil,
    forbiddenDependencies: Seq[ForbiddenDependencyConfiguration] = Nil,
    skipDependencyMappingValidation: Boolean = false
) {
  val all: Seq[DependencyDefinition] = directIds ++ indirectIds
}

object DependencyDefinitions {
  val empty: DependencyDefinitions = DependencyDefinitions(Nil, Nil, Nil, Nil)
}

final case class ExternalDependency(afs: DependencyDefinition, maven: Seq[DependencyDefinition])

object ExternalDependencies {
  def empty: ExternalDependencies =
    ExternalDependencies(AfsDependencies.empty, MavenDependencies.empty)
}

final case class ExternalDependencies(afsDependencies: AfsDependencies, mavenDependencies: MavenDependencies) {
  val multiSourceDependencies: Seq[ExternalDependency] = afsDependencies.afsMappedDeps
  val definitions: Seq[DependencyDefinition] =
    (afsDependencies.allAfsDeps ++ mavenDependencies.allMavenDeps).toIndexedSeq
  // be used for afs to maven mapping
  def afsToMavenMap: Map[DependencyDefinition, Seq[DependencyDefinition]] =
    multiSourceDependencies.map(d => d.afs -> d.maven).toMap

  lazy val mavenToAfsMap: Map[String, DependencyDefinition] = {
    for {
      dep <- multiSourceDependencies
      afsDep = dep.afs
      mavenDep <- dep.maven
    } yield mavenDep.key -> afsDep
  }.toMap
}

trait ExternalDependenciesSource {
  def externalDependencies(ms: ModuleSet): ExternalDependencies
  def coreExternalDependencies: ExternalDependencies
  def allExternalDependencies: ExternalDependencies
}

object AfsDependencies {
  def empty: AfsDependencies = AfsDependencies(Nil, Nil)
}

/**
 * loaded afs dependencies in *dependencies.obt files
 * @param unmappedAfsDeps
 *   only afs dependency be defined
 * @param afsMappedDeps
 *   both afs{} maven{} be defined
 */
final case class AfsDependencies(unmappedAfsDeps: Seq[DependencyDefinition], afsMappedDeps: Seq[ExternalDependency]) {
  val allAfsDeps: Seq[DependencyDefinition] = unmappedAfsDeps ++ afsMappedDeps.map(_.afs)
}

object MavenDependencies {
  def empty: MavenDependencies = MavenDependencies(Nil, Nil, Nil)
}

/**
 * loaded maven dependencies in *dependencies.obt files
 * @param unmappedMavenDeps
 *   Maven dependencies with unknown AFS coordinates. Loaded from maven-dependencies.obt and
 *   should only be used in 'mavenLibs = []'.
 * @param mixedModeMavenDeps
 *   Mapped maven dependencies or those with 'noAfs = true'. Loaded from jvm-dependencies.obt or dependency/variant
 *   sets.
 * @param boms
 *   BOMs for dependency versions. Loaded from dependency/variant sets.
 */
final case class MavenDependencies(
    unmappedMavenDeps: Seq[DependencyDefinition],
    mixedModeMavenDeps: Seq[DependencyDefinition],
    boms: Seq[DependencyDefinition]
) {
  val allMavenDeps: Seq[DependencyDefinition] = unmappedMavenDeps ++ mixedModeMavenDeps
}

final case class DependencyCoursierKey(org: String, name: String, configuration: String, version: String) {
  def displayName = s"$org:$name.$configuration.$version"
  // ignore configuration & version diff
  def isSameName(to: DependencyCoursierKey): Boolean = org == to.org && name == to.name
}
