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
package optimus.buildtool.generators.sandboxed

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.sandboxed.SandboxedBase.Content
import optimus.buildtool.generators.sandboxed.SandboxedBase.TypedContent
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.collection.immutable.SortedMap

@entity class SandboxedInputsImpl(
    sandboxFactory: SandboxFactory,
    protected val workspaceSourceRoot: Directory,
    val generatorId: String,
    protected val internalContent: Seq[TypedContent[SourceFolder, SourceFileId]],
    protected val externalContent: Seq[TypedContent[ReactiveDirectory, FileAsset]],
    protected val sourcePredicate: PathFilter,
    scope: CompilationScope,
    fingerprint: Seq[String]
) extends SandboxedInputs
    with SandboxedBase {
  import SandboxedFiles._

  override protected type InternalLocation = SourceFolder
  override protected type ExternalLocation = ReactiveDirectory

  @node def withFiles(
      fileType: FileType,
      sourcePredicate: PathFilter,
      fingerprintTypeSuffix: String = "",
      allowEmpty: Boolean = false
  ): SandboxedInputs = {
    val internalTemplateFolders = folders(Template, internalContent)
    val externalTemplateFolders = folders(Template, externalContent)
    withFolders(
      fileType,
      internalTemplateFolders,
      externalTemplateFolders,
      sourcePredicate,
      fingerprintTypeSuffix,
      allowEmpty)
  }

  @node def withFolders(
      fileType: FileType,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter = sourcePredicate,
      fingerprintTypeSuffix: String = "",
      allowEmpty: Boolean = false
  ): SandboxedInputs = {
    val (newInternalContent, newInternalFingerprint) =
      SandboxedInputsImpl.internalContent(
        fileType,
        internalFolders,
        sourcePredicate,
        scope,
        fingerprintTypeSuffix,
        allowEmpty
      )
    val (newExternalContent, newExternalFingerprint) =
      SandboxedInputsImpl.externalContent(fileType, externalFolders, sourcePredicate, fingerprintTypeSuffix, allowEmpty)
    SandboxedInputsImpl(
      sandboxFactory,
      workspaceSourceRoot,
      generatorId,
      internalContent :+ newInternalContent,
      externalContent :+ newExternalContent,
      this.sourcePredicate,
      scope,
      fingerprint ++ newInternalFingerprint ++ newExternalFingerprint
    )
  }

  private def folders[A, B](fileType: FileType, allContent: Seq[TypedContent[A, B]]) = (for {
    tc <- allContent if tc.fileType == fileType
    c <- tc.content
  } yield c.location).distinct

  @node def isEmpty: Boolean = internalContent.isEmpty && externalContent.isEmpty

  @node def hashFingerprint(configuration: Map[String, String], extraFingerprint: Seq[String]): FingerprintArtifact = {
    val configFingerprint = configuration.toSeq.sorted.map { case (k, v) => s"[Config]$k=$v" }
    val fullFingerprint = fingerprint ++ configFingerprint ++ extraFingerprint
    scope.hasher.hashFingerprint(fullFingerprint, ArtifactType.GenerationFingerprint, Some(generatorId))
  }

  override def toFiles: SandboxedFiles = SandboxedFilesImpl(
    sandboxFactory,
    workspaceSourceRoot,
    generatorId,
    internalContent.map { t =>
      t.copy(content = t.content.map(c => c.copy(location = c.location.workspaceSrcRootToSourceFolderPath)))
    },
    externalContent,
  )
}

object SandboxedInputsImpl {
  import SandboxedFiles._

  @node def apply(
      sandboxFactory: SandboxFactory,
      workspaceSourceRoot: Directory,
      generatorId: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter,
      scope: CompilationScope
  ): SandboxedInputs = {
    val (internalContent, internalFingerprint) =
      this.internalContent(Template, internalFolders, sourcePredicate, scope, "", allowEmpty = false)

    // TODO (OPTIMUS-79166): Remove `allowEmpty = true` once we have a better solution for
    // built-in proto files than just putting them in the templates
    val (externalContent, externalFingerprint) =
      this.externalContent(Template, externalFolders, sourcePredicate, "", allowEmpty = true)
    val fingerprint = internalFingerprint ++ externalFingerprint

    SandboxedInputsImpl(
      sandboxFactory,
      workspaceSourceRoot,
      generatorId,
      Seq(internalContent),
      Seq(externalContent),
      sourcePredicate,
      scope,
      fingerprint
    )
  }

  @node private def internalContent(
      fileType: FileType,
      internalFolders: Seq[SourceFolder],
      sourcePredicate: PathFilter,
      scope: CompilationScope,
      fingerprintTypeSuffix: String,
      allowEmpty: Boolean
  ) = {
    val tpeSuffix = if (fingerprintTypeSuffix.nonEmpty) s":$fingerprintTypeSuffix" else ""
    val internalContent = internalFolders.apar.map(f => Content(f, f.findSourceFiles(sourcePredicate)))
    val internalTemplateFingerprint =
      scope.fingerprint(internalContent.map(_.files).merge, s"${fileType.name}$tpeSuffix")

    val emptyFolders = internalContent.filter(_.files.isEmpty).map(_.location)
    if (!allowEmpty && internalFolders.nonEmpty && emptyFolders.nonEmpty) {
      throw new IllegalArgumentException(
        s"${fileType.name}: No valid content found for predicate $sourcePredicate in: ${emptyFolders.map(_.workspaceSrcRootToSourceFolderPath).mkString(", ")}"
      )
    }

    (TypedContent(fileType, internalContent), internalTemplateFingerprint)
  }

  @node private def externalContent(
      fileType: FileType,
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter,
      fingerprintTypeSuffix: String,
      allowEmpty: Boolean
  ) = {
    val tpeSuffix = if (fingerprintTypeSuffix.nonEmpty) s":$fingerprintTypeSuffix" else ""
    val externalFiles = externalFolders.apar.map(f => (f, f.findFiles(sourcePredicate)))
    val externalFileContent = externalFiles
      .map { case (d, files) =>
        Content(d, SortedMap(files.map(f => f -> Hashing.hashFileWithContent(f)): _*))
      }

    val externalTemplateFingerprint = for {
      c <- externalFileContent
      (f, hc) <- c.files
    } yield {
      PathUtils.fingerprintElement(s"${fileType.name}$tpeSuffix", f.pathFingerprint, hc.hash)
    }

    val emptyFolders = externalFileContent.filter(_.files.isEmpty).map(_.location)
    if (!allowEmpty && externalFolders.nonEmpty && emptyFolders.nonEmpty)
      throw new IllegalArgumentException(
        s"${fileType.name}: No valid content found for predicate $sourcePredicate in: ${emptyFolders.map(_.pathString).mkString(", ")}"
      )

    (TypedContent(fileType, externalFileContent), externalTemplateFingerprint)
  }
}
