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
package optimus.buildtool.artifacts
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NpmConfiguration.NpmBuildMode
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars

import java.nio.file.Path
import scala.util.control.NonFatal

object CachedMetadata {
  val MetadataFile: RelativePath = RelativePath(NamingConventions.MetadataFileName)
}

trait CachedMetadata

final case class MessagesMetadata(messages: Seq[CompilationMessage], hasErrors: Boolean) extends CachedMetadata

final case class PythonMetadata(
    osVersion: String,
    messages: Seq[CompilationMessage],
    hasErrors: Boolean,
    inputsHash: String,
    python: PythonDefinition)
    extends CachedMetadata

object PythonMetadata {
  def load(pythonArtifact: Path): PythonMetadata = {
    try {
      val file = JarAsset(pythonArtifact)
      Jars.withJar(file) { root =>
        val metadataJson = root.resolveFile(CachedMetadata.MetadataFile).asJson
        import JsonImplicits._
        AssetUtils.readJson[PythonMetadata](metadataJson, unzip = false)
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalStateException(
          s"Failed to load Python metadata from artifact: $pythonArtifact. Error: ${e.getMessage}",
          e
        )
    }
  }
}

final case class CppMetadata(
    osVersion: String,
    releaseFile: Option[RelativePath],
    debugFile: Option[RelativePath],
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata

final case class ElectronMetadata(
    mode: NpmBuildMode,
    executables: Seq[String],
    messages: Seq[CompilationMessage],
    hasErrors: Boolean)
    extends CachedMetadata

final case class GeneratedSourceMetadata(
    generatorType: GeneratorType,
    generatorName: String,
    sourcePath: RelativePath,
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata

final case class ProcessorMetadata(
    processorName: String,
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata
