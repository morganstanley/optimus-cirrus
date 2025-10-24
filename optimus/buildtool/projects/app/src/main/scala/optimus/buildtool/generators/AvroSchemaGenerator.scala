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
package optimus.buildtool.generators

import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.platform._
import optimus.platform.util.Log
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.Schema.Parser

@entity class AvroSchemaGenerator extends SourceGenerator {
  override val generatorType: String = "avro"
  override type Inputs = AvroSchemaGenerator.Inputs

  override def templateType(configuration: Map[String, String]): PathFilter = Directory.fileExtensionPredicate("avsc")

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val fingerprint = Seq(s"[avro]${GeneratorUtils.location[SpecificCompiler].pathFingerprint}")
    val fingerprintHash = templates.hashFingerprint(configuration, fingerprint)
    AvroSchemaGenerator.Inputs(
      files = templates.toFiles,
      fingerprintHash,
      configuration
    )
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { context =>
    inputs.files.files(SandboxedFiles.Template).foreach { f =>
      AvroSchemaGenerator.generateAvroToSchema(f, context.outputDir)
    }
    context.createArtifact(Nil)
  }
}

object AvroSchemaGenerator extends Log {
  final case class Inputs(
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact,
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs

  private def generateAvroToSchema(sourceFile: FileAsset, outputDir: Directory): Unit = {
    val schema = new Parser().parse(sourceFile.path.toFile)
    val compiler = new SpecificCompiler(schema)
    compiler.compileToDestination(null, outputDir.path.toFile)
  }
}
