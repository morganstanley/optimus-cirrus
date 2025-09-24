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

import com.github.plokhotnyuk.jsoniter_scala.macros.named

import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.scope.ScopedCompilation
import optimus.platform._
import optimus.scalacompat.collection._

final case class MetaBundleReport(
    @named("metadata_creator") metadataCreator: String,
    @named("metadata_version") metadataVersion: String,
    meta: String,
    project: String,
    release: String,
    @named("build_info") buildInfo: BuildInfoReport,
    artifacts: Iterable[ArtifactReport]
)

object MetaBundleReport {

  @node def apply(
      settings: MetadataSettings,
      metaBundle: MetaBundle,
      bundleCompilations: Map[ScopeId, ScopedCompilation]): MetaBundleReport = {
    val scopeCompilationsPerModule = bundleCompilations.groupBy { case (id, _) => ModuleId(id) }
    new MetaBundleReport(
      metadataCreator = s"optimus/buildtool/${settings.obtVersion}",
      metadataVersion = "1.0",
      meta = metaBundle.meta,
      project = metaBundle.bundle,
      release = settings.installVersion,
      buildInfo = BuildInfoReport(settings.buildId),
      artifacts = scopeCompilationsPerModule.apar.map { case (module, compilations) =>
        ArtifactReport(settings, module, compilations)
      }(Seq.breakOut)
    )
  }

  // This is for maven metadata generation
  @node def apply(
      settings: MetadataSettings,
      metaBundle: MetaBundle,
      id: ScopeId,
      idCompilation: ScopedCompilation): MetaBundleReport = {
    val compilationMap = Map(id -> idCompilation)
    new MetaBundleReport(
      metadataCreator = s"optimus/buildtool/${settings.obtVersion}",
      metadataVersion = "1.0",
      meta = metaBundle.meta,
      project = metaBundle.bundle,
      release = settings.installVersion,
      buildInfo = BuildInfoReport(settings.buildId),
      artifacts = Seq(ArtifactReport(settings, ModuleId(id.copy(meta = metaBundle.meta)), compilationMap))
    )
  }
}

final case class BuildInfoReport(ccid: String)
