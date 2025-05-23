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

import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.scope.ScopedCompilation
import optimus.platform._

final case class ArtifactReport(
    name: String,
    @named("type") tpe: String,
    scope: String,
    usage: String,
    @named("on_toolchain") onToolchain: Map[String, DependenciesReport]
)

object ArtifactReport {

  @node def apply(
      settings: MetadataSettings,
      module: ModuleId,
      moduleCompilations: Map[ScopeId, ScopedCompilation]): ArtifactReport = new ArtifactReport(
    name = module.module,
    tpe = ".trainsubproject",
    scope = "public",
    usage = s"group: '${module.meta}.${module.bundle}'",
    onToolchain = {
      val externalDependenciesReports = DependenciesReport(settings, moduleCompilations)
      Map(settings.javaVersion -> externalDependenciesReports.jvm, "*" -> externalDependenciesReports.extraLibs)
        .filter { case (_, reports) =>
          reports.dependencies.nonEmpty
        }
    }
  )
}
