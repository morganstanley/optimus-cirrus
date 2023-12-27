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

import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.platform._
import optimus.scalacompat.collection._
import spray.json.DefaultJsonProtocol._
import spray.json._

final case class MetaBundleReport(
    metadataCreator: String,
    metadataVersion: String,
    meta: String,
    project: String,
    release: String,
    buildInfo: BuildInfoReport,
    artifacts: Iterable[ArtifactReport]
)

object MetaBundleReport {

  implicit val metaBundleReportFormat: RootJsonFormat[MetaBundleReport] = jsonFormat(
    MetaBundleReport.apply,
    fieldName1 = "metadata_creator",
    fieldName2 = "metadata_version",
    fieldName3 = "meta",
    fieldName4 = "project",
    fieldName5 = "release",
    fieldName6 = "build_info",
    fieldName7 = "artifacts"
  )

  @node def apply(
      settings: MetadataSettings,
      metaBundle: MetaBundle,
      scopeConfigurations: Map[ScopeId, ScopeConfiguration]): MetaBundleReport = {
    val scopeConfigurationsPerModule = scopeConfigurations.groupBy { case (id, _) => ModuleId(id) }
    new MetaBundleReport(
      metadataCreator = s"optimus/buildtool/${settings.obtVersion}",
      metadataVersion = "1.0",
      meta = metaBundle.meta,
      project = metaBundle.bundle,
      release = settings.installVersion,
      buildInfo = BuildInfoReport(settings.buildId),
      artifacts = scopeConfigurationsPerModule.apar.map { case (module, configs) =>
        ArtifactReport(settings, module, configs)
      }(Seq.breakOut)
    )
  }

  // This is for maven metadata generation
  @node def apply(
      settings: MetadataSettings,
      metaBundle: MetaBundle,
      id: ScopeId,
      scopeConfiguration: ScopeConfiguration): MetaBundleReport = {
    val confMap = Map(id -> scopeConfiguration)
    new MetaBundleReport(
      metadataCreator = s"optimus/buildtool/${settings.obtVersion}",
      metadataVersion = "1.0",
      meta = metaBundle.meta,
      project = metaBundle.bundle,
      release = settings.installVersion,
      buildInfo = BuildInfoReport(settings.buildId),
      artifacts = Seq(ArtifactReport(settings, ModuleId(id), confMap))
    )
  }
}

final case class BuildInfoReport(ccid: String)

object BuildInfoReport {
  implicit val buildInfoReportFormat: RootJsonFormat[BuildInfoReport] = jsonFormat1(BuildInfoReport.apply)
}
