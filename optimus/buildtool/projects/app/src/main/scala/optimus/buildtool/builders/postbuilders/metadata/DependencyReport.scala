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
package optimus.buildtool.builders.postbuilders.metadata

import optimus.buildtool.artifacts.ExternalArtifactId
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.WebDependency
import spray.json.DefaultJsonProtocol._
import spray.json._

final case class DependencyReport(
    namespace: String,
    meta: String,
    project: String,
    release: Option[String],
    artifact: Option[String],
    qualifiers: Set[QualifierReport],
    isTransitive: Boolean,
    parent: Option[Set[ParentReport]] = None
)

object DependencyReport {

  implicit val dependencyReportFormat: RootJsonFormat[DependencyReport] = jsonFormat(
    DependencyReport.apply,
    fieldName1 = "namespace",
    fieldName2 = "meta",
    fieldName3 = "project",
    fieldName4 = "release",
    fieldName5 = "artifact",
    fieldName6 = "qualifiers",
    fieldName7 = "is_transitive",
    fieldName8 = "parent"
  )

  def dependencyNamespace(isMaven: Boolean): String = if (isMaven) MavenNamespace else AfsNamespace

  // we generate train metadata for all resolved .jar artifacts
  def fromExternalIds(
      extIds: Seq[ExternalArtifactId],
      qualifiers: Set[QualifierReport],
      settings: MetadataSettings): Seq[DependencyReport] = {
    extIds.collect { case id: VersionedExternalArtifactId =>
      DependencyReport(
        namespace = if (settings.generatePoms) MavenNamespace else dependencyNamespace(id.isMaven),
        meta = id.group,
        project = id.name,
        release = Some(id.version),
        artifact = Some(id.artifactName),
        qualifiers = qualifiers,
        isTransitive = false
      )
    }
  }

  def fromExtraLib(extraLib: DependencyDefinition, qualifiers: Set[QualifierReport]): DependencyReport =
    DependencyReport(
      namespace = dependencyNamespace(extraLib.isMaven),
      meta = extraLib.group,
      project = extraLib.name,
      release = Some(extraLib.version),
      artifact = None,
      qualifiers = qualifiers,
      isTransitive = extraLib.transitive,
      parent = None
    )

  def fromWebDependency(npmDep: WebDependency, qualifiers: Set[QualifierReport]): DependencyReport =
    DependencyReport(
      namespace = NpmNamespace,
      meta = npmDep.definition.group,
      project = npmDep.definition.name,
      release = Some(npmDep.definition.version),
      artifact = None,
      qualifiers = qualifiers,
      isTransitive = npmDep.definition.transitive,
      parent =
        if (npmDep.parents.nonEmpty) Some(npmDep.parents.map(ParentReport.generateReport(_, qualifiers))) else None
    )

  def fromWebToolingDefinition(dependency: DependencyDefinition, qualifiers: Set[QualifierReport]): DependencyReport =
    DependencyReport(
      namespace =
        if (dependency.group == MsWebDependencyDefaultMeta) NpmNamespace else dependencyNamespace(dependency.isMaven),
      meta = if (dependency.name == PnpmName) OutsideWebDependencyDefaultMeta else dependency.group,
      project = dependency.name,
      release = Some(dependency.version),
      artifact = None,
      qualifiers = qualifiers,
      isTransitive = dependency.transitive
    )

  def apply(id: ScopeId, qualifiers: Set[QualifierReport], settings: MetadataSettings): DependencyReport = {
    val scopeId = if (settings.generatePoms) id.forMavenRelease else id
    DependencyReport(
      namespace = if (settings.generatePoms) MavenNamespace else AfsNamespace,
      meta = if (settings.generatePoms) s"com.ms." + scopeId.meta else scopeId.meta,
      project = scopeId.bundle,
      release = None,
      artifact =
        Some(if (settings.generatePoms) s"${scopeId.module}-${settings.installVersion}.jar" else scopeId.module),
      qualifiers = qualifiers,
      isTransitive = false
    )
  }

  def merge(depsA: Set[DependencyReport], depsB: Set[DependencyReport]): Set[DependencyReport] = {
    val depsPerDependencyType = (depsA ++ depsB).groupBy(_.copy(qualifiers = Set.empty))
    depsPerDependencyType.map { case (dependency, occurrences) =>
      dependency.copy(qualifiers = occurrences.flatMap(_.qualifiers))
    }.toSet
  }
}

sealed trait QualifierReport
case object Compile extends QualifierReport
case object Runtime extends QualifierReport
case object TestOnly extends QualifierReport
case object Tooling extends QualifierReport

object QualifierReport {

  implicit val qualifierReportFormat: RootJsonFormat[QualifierReport] = new RootJsonFormat[QualifierReport] {
    override def read(json: JsValue): QualifierReport = json.convertTo[String] match {
      case "at_build"   => Compile
      case "at_runtime" => Runtime
      case "at_test"    => TestOnly
      case "tooling"    => Tooling
    }

    override def write(obj: QualifierReport): JsValue = obj match {
      case Compile  => JsString("at_build")
      case Runtime  => JsString("at_runtime")
      case TestOnly => JsString("at_test")
      case Tooling  => JsString("tooling")
    }
  }

}

final case class ParentReport(
    namespace: String,
    meta: String,
    project: String,
    release: Option[String],
    qualifiers: Set[QualifierReport],
    isTransitive: Boolean
)

object ParentReport {
  implicit val parentReportFormat: RootJsonFormat[ParentReport] = jsonFormat(
    ParentReport.apply,
    fieldName1 = "namespace",
    fieldName2 = "meta",
    fieldName3 = "project",
    fieldName4 = "release",
    fieldName5 = "qualifiers",
    fieldName6 = "is_transitive"
  )

  def generateReport(dep: DependencyDefinition, qualifiers: Set[QualifierReport]): ParentReport =
    ParentReport(
      namespace = NpmNamespace,
      meta = dep.group,
      project = dep.name,
      release = Some(dep.version),
      qualifiers = qualifiers,
      isTransitive = dep.transitive
    )
}
