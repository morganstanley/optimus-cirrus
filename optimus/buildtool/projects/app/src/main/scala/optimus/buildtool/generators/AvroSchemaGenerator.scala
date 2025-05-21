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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform._
import optimus.platform.util.Log
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.platform.NodeFunction0
import optimus.platform.entity
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.Schema.Parser
import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity class AvroSchemaGenerator(workspaceSourceRoot: Directory) extends SourceGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.Avro

  override type Inputs = AvroSchemaGenerator.Inputs

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val filter = sourceFilter && Directory.fileExtensionPredicate("avsc")
    val (templateFiles, templateFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )
    val fingerprint =
      s"[avro]${SourceGenerator.location[SpecificCompiler].pathFingerprint}" +: (configuration.map { case (k, v) =>
        s"[Config]$k=$v"
      }.toIndexedSeq ++ templateFingerprint)
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint, Some(name))
    AvroSchemaGenerator.Inputs(
      generatorName = name,
      sourceFiles = templateFiles,
      fingerprintHash,
      configuration
    )
  }
  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().sourceFiles.map(_._2).merge.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    ObtTrace.traceTask(scopeId, GenerateSource) {
      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))

        val allTemplates = sourceFiles.apar.map { case (root, files) =>
          val validated = SourceGenerator.validateFiles(root, files)
          validated.files
        }.flatten

        val outputDir = tempDir.resolveDir(AvroSchemaGenerator.SourcePath)
        Utils.createDirectories(outputDir)
        allTemplates.foreach { f =>
          AvroSchemaGenerator.generateAvroToSchema(f, outputDir)
        }
        log.info(s"[$scopeId:$generatorName] avro-schema generation successful")
        val artifact = GeneratedSourceArtifact.create(
          scopeId,
          generatorName,
          artifactType,
          outputJar,
          AvroSchemaGenerator.SourcePath,
          Seq.empty
        )
        SourceGenerator.createJar(
          generatorName,
          AvroSchemaGenerator.SourcePath,
          artifact.messages,
          artifact.hasErrors,
          tempJar,
          tempDir)()
        artifact
      }
      Some(artifact)
    }
  }
}

object AvroSchemaGenerator extends Log {
  private[buildtool] val SourcePath = RelativePath("src")
  final case class Inputs(
      generatorName: String,
      sourceFiles: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      fingerprint: FingerprintArtifact,
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs

  def generateAvroToSchema(sourceFile: FileAsset, outputDir: Directory): Unit = {
    val schema = new Parser().parse(sourceFile.path.toFile)
    val compiler = new SpecificCompiler(schema)
    compiler.compileToDestination(null, outputDir.path.toFile)
  }
}
