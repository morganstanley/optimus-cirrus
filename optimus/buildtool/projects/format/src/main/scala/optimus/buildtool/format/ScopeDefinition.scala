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

import com.typesafe.config.ConfigFactory
import optimus.buildtool.config.ArchiveConfiguration
import optimus.buildtool.config.CopyFilesConfiguration
import optimus.buildtool.config.ExtensionConfiguration
import optimus.buildtool.config.JarConfiguration
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.utils.JarUtils
import spray.json._

import scala.collection.immutable.Seq

final case class JarDefinition(
    manifest: Map[String, String]
) {
  def asJson: JsValue = JarDefinition.writer write this
}

object JarDefinition {
  val default = JarDefinition(manifest = Map.empty)
  private val writer: JsonWriter[JarDefinition] = {
    import DefaultJsonProtocol._
    jsonFormat1(JarDefinition.apply)
  }
}

// eg. compile.modules += a.b.c in d.obt:25 => ScopeRelationship(a.b.c.main, d.obt, 25)
final case class ScopeRelationship(target: ScopeId, origin: ObtFile, line: Int)

final case class ScopeDefinition(
    id: ScopeId,
    module: Module,
    configuration: ScopeConfiguration,
    parents: Seq[String], // eg test extends main
    copyFiles: Option[CopyFilesConfiguration],
    extensions: Option[ExtensionConfiguration],
    archive: Option[ArchiveConfiguration],
    line: Int,
    jar: JarDefinition,
    includeInClassBundle: Boolean,
    postInstallApps: Seq[Set[PostInstallApp]],
    relationships: Seq[ScopeRelationship],
    customScopesDefinitions: Seq[CustomScopesDefinition]
)

object ScopeDefinition {
  private val formatter = ObtFileFormatter("  ", ModuleFormatterSettings)
  def prettyPrint(scopes: Seq[ScopeDefinition]): String = {
    val configs = scopes.map(s => ScopeDefinitionCompiler.asJson(s).prettyPrint).map(ConfigFactory.parseString)
    formatter.formatConfig(configs.foldLeft(ConfigFactory.empty())(_ withFallback _))
  }

  def loadScopeId(path: String): ScopeId = path.split('.') match {
    case Array(meta, bundle, module, tpe) =>
      ScopeId(meta, bundle, module, tpe)
    case Array(meta, bundle, module) =>
      ScopeId(meta, bundle, module, "main")
    case _ =>
      throw new AssertionError(s"Bad scope path: $path")
  }

  def jarConfigurationFor(scope: ScopeDefinition, config: VersionConfiguration): JarConfiguration = {
    val stratoVersion = config.stratosphereVersion
    val obtVersion = config.obtVersion
    JarConfiguration(
      manifest = JarUtils
        .defaultManifest(
          scope.id.meta,
          scope.id.bundle,
          config.installVersion,
          stratoVersion,
          obtVersion) ++ scope.jar.manifest
    )
  }

}
