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

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceMetadata
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.Utils
import optimus.core.needsPluginAlwaysAutoAsyncArgs
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import java.nio.file.Files

@entity class ArtifactWriter(
    scopeId: ScopeId,
    generatorType: GeneratorType,
    generatorName: String,
    outputJar: JarAsset
) {

  // noinspection ScalaUnusedSymbol
  @alwaysAutoAsyncArgs
  def atomicallyWrite(sourcePath: RelativePath = SourceGenerator.SourcePath)(
      f: ArtifactWriteContext => GeneratedSourceArtifact
  ): Option[GeneratedSourceArtifact] = needsPluginAlwaysAutoAsyncArgs

  @async def atomicallyWrite$NF(sourcePath: RelativePath = SourceGenerator.SourcePath)(
      f: AsyncFunction1[ArtifactWriteContext, GeneratedSourceArtifact]
  ): Option[GeneratedSourceArtifact] =
    Utils.atomicallyWrite(outputJar) { tempPath =>
      val tempJar = JarAsset(tempPath)
      // Use a short temp dir name to avoid issues with too-long paths for generated .java files
      val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
      val outputDir = tempDir.resolveDir(sourcePath)
      Utils.createDirectories(outputDir)
      val context = ArtifactWriteContext(this, tempDir, outputDir, sourcePath, tempJar)
      Some(f(context))
    }

  def atomicallyWrite(f: JarAsset => GeneratedSourceArtifact): Option[GeneratedSourceArtifact] =
    Utils.atomicallyWrite(outputJar)(tempPath => Some(f(JarAsset(tempPath))))

  @async def atomicallyWrite$NF(f: AsyncFunction1[JarAsset, GeneratedSourceArtifact]): Option[GeneratedSourceArtifact] =
    Utils.atomicallyWrite$NF(outputJar)(asAsync(tempPath => Some(f(JarAsset(tempPath)))))

  @node def createArtifact(
      tempDir: Directory,
      tempJar: JarAsset,
      messages: Seq[CompilationMessage],
      sourcePath: RelativePath = SourceGenerator.SourcePath
  )(f: ConsistentlyHashedJarOutputStream => Unit = _ => ()): GeneratedSourceArtifact = {
    val artifact = GeneratedSourceArtifact.create(
      scopeId,
      generatorType,
      generatorName,
      outputJar,
      sourcePath,
      messages
    )
    createJar(
      sourcePath,
      messages,
      artifact.hasErrors,
      tempJar,
      Some(tempDir)
    )(f)
    artifact
  }

  @node def streamToArtifact(
      tempJar: JarAsset,
      messages: Seq[CompilationMessage],
      sourcePath: RelativePath = SourceGenerator.SourcePath
  )(f: ConsistentlyHashedJarOutputStream => Unit = _ => ()): GeneratedSourceArtifact = {
    val artifact = GeneratedSourceArtifact.create(
      scopeId,
      generatorType,
      generatorName,
      outputJar,
      sourcePath,
      messages
    )
    createJar(
      sourcePath,
      messages,
      artifact.hasErrors,
      tempJar,
      None
    )(f)
    artifact
  }

  @async private def createJar(
      sourcePath: RelativePath,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean,
      jar: JarAsset,
      tempDir: Option[Directory]
  )(f: ConsistentlyHashedJarOutputStream => Unit): Seq[CompilationMessage] = {
    import optimus.buildtool.artifacts.JsonImplicits.generatedSourceMetadataValueCodec
    Jars.createJar(
      jar,
      GeneratedSourceMetadata(generatorType, generatorName, sourcePath, messages, hasErrors),
      tempDir
    )(f)
    messages
  }
}

@entity class ArtifactWriteContext(
    writer: ArtifactWriter,
    tempDir: Directory,
    val outputDir: Directory,
    sourcePath: RelativePath,
    tempJar: JarAsset
) {
  @node def createArtifact(messages: Seq[CompilationMessage]): GeneratedSourceArtifact =
    writer.createArtifact(tempDir, tempJar, messages, sourcePath)()
}
