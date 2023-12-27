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

import java.nio.file.FileSystems
import java.nio.file.Files

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.artifacts.GeneratedSourceMetadata
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.reflect._

@entity trait SourceGenerator {
  def artifactType: GeneratedSourceArtifactType
  def tpe: GeneratorType = GeneratorType(artifactType.name)

  type Inputs <: SourceGenerator.Inputs

  @node def dependencies(scope: CompilationScope): Seq[Artifact] = Nil

  // Returned as a NodeFunction0 to prevent Inputs (which will often contain full sources) being inadvertently
  // cached as an argument to other @nodes
  final def inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): NodeFunction0[Inputs] =
    asNode(() => _inputs(name, internalFolders, externalFolders, sourceFilter, configuration, scope))

  @node protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs

  @node def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean

  @node def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact]

}

object SourceGenerator {
  trait Inputs {
    def generatorName: String
    def fingerprintHash: String
  }

  def location[A: ClassTag]: JarAsset =
    JarAsset(
      PathUtils.uriToPath(
        classTag[A].runtimeClass.getProtectionDomain.getCodeSource.getLocation.toURI.toString,
        FileSystems.getDefault
      )
    )

  @node private[generators] def templates(
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter,
      scope: CompilationScope,
      workspaceSourceRoot: Directory,
      fingerprintType: String,
      fingerprintPrefix: String = ""
  ): (SortedMap[FileAsset, HashedContent], Seq[String]) = {
    val (ts, fingerprint) =
      rootedTemplates(
        internalFolders,
        externalFolders,
        sourcePredicate,
        scope,
        workspaceSourceRoot,
        fingerprintType,
        fingerprintPrefix
      )
    (ts.map(_._2).merge, fingerprint)
  }

  @node private[generators] def rootedTemplates(
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter,
      scope: CompilationScope,
      workspaceSourceRoot: Directory,
      fingerprintType: String,
      fingerprintPrefix: String = ""
  ): (Seq[(Directory, SortedMap[FileAsset, HashedContent])], Seq[String]) = {

    val internalContent = internalFolders.apar.map(f => (f, f.findSourceFiles(sourcePredicate)))
    val internalFileContent = internalContent.map { case (folder, sources) =>
      // Note that there's no guarantee that these folders exist on disk (we may be reading them from git, for example),
      // so generators will need to handle that possibility either by passing files as content or copying to a temporary
      // directory
      val dir = workspaceSourceRoot.resolveDir(folder.workspaceSrcRootToSourceFolderPath)
      val files = sources.map { case (id, c) =>
        workspaceSourceRoot.resolveFile(id.workspaceSrcRootToSourceFilePath) -> c
      }
      (dir, files)
    }

    val internalTemplateFingerprint =
      scope.fingerprint(internalContent.map(_._2).merge, fingerprintType, fingerprintPrefix)

    val externalFiles = externalFolders.apar.map(f => (f, f.findFiles(sourcePredicate)))
    val externalFileContent = externalFiles.map { case (d, files) =>
      (d, SortedMap(files.map(f => f -> Hashing.hashFileWithContent(f)): _*))
    }

    val externalTemplateFingerprint = externalFileContent.map(_._2).merge.map { case (f, c) =>
      PathUtils.fingerprintElement(fingerprintType, f.pathFingerprint, c.hash, fingerprintPrefix)
    }

    (internalFileContent ++ externalFileContent, internalTemplateFingerprint ++ externalTemplateFingerprint)
  }

  @async private[generators] def createJar(
      generatorName: String,
      sourcePath: RelativePath,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean,
      jar: JarAsset,
      tempDir: Directory = null
  )(f: ConsistentlyHashedJarOutputStream => Unit = _ => ()): Seq[CompilationMessage] = {
    import optimus.buildtool.artifacts.JsonImplicits._
    Jars.createJar(jar, GeneratedSourceMetadata(generatorName, sourcePath, messages, hasErrors), Option(tempDir))(f)
    messages
  }

  // Ensure files exist on disk in their declared paths, and write content to a temporary location if not
  @node def validateFiles(contentRoot: Directory, content: SortedMap[FileAsset, HashedContent]): ValidatedFiles = {
    if (content.keySet.forall(_.existsUnsafe)) ValidatedFiles(contentRoot, content.keySet.toIndexedSeq)
    else {
      val tempDir = Directory.temporary()
      val tempFiles = content.toIndexedSeq.apar.map { case (f, hc) =>
        val tempFile = tempDir.resolveFile(contentRoot.relativize(f))
        Utils.createDirectories(tempFile.parent)
        Files.copy(hc.contentAsInputStream, tempFile.path)
        tempFile
      }
      ValidatedFiles(tempDir, tempFiles.toIndexedSeq)
    }
  }

  @node def validateFile(contentRoot: Directory, file: FileAsset, content: HashedContent): Option[FileAsset] = {
    validateFiles(contentRoot, SortedMap(file -> content)).files.headOption
  }

  final case class ValidatedFiles(root: Directory, files: Seq[FileAsset])
}
