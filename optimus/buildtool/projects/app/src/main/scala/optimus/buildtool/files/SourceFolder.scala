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
package optimus.buildtool.files

import optimus.buildtool.compilers.runconfc.Templates
import optimus.buildtool.files.Directory.FileFilter
import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.Not
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.PathRegexFilter
import optimus.buildtool.runconf.RunConfFile
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import optimus.scalacompat.collection._

@entity trait SourceFolder {
  import SourceFolder._

  def workspaceSrcRootToSourceFolderPath: RelativePath
  def workspaceSourceRoot: Directory

  OptimusBuildToolAssertions.require(
    workspaceSourceRoot.contains(workspaceSourceRoot.resolveDir(workspaceSrcRootToSourceFolderPath)),
    s"Invalid directory path: $workspaceSrcRootToSourceFolderPath"
  )

  @node def exists: Boolean

  @node def sourceFiles: SortedMap[SourceFileId, HashedContent] = findSourceFiles(NoFilter)

  @node def scalaAndJavaSourceFiles: SortedMap[SourceFileId, HashedContent] =
    _findSourceFiles(isScalaOrJavaSourceFile, Int.MaxValue, scopeArtifacts = true)

  @node def cppSourceOrHeaderFiles: SortedMap[SourceFileId, HashedContent] =
    _findSourceFiles(isCppSourceOrHeaderFile, Int.MaxValue, scopeArtifacts = true)

  @node def appScriptsTemplateSourceFiles: SortedMap[SourceFileId, HashedContent] =
    _findSourceFiles(isAppScriptTemplateSourceFile, maxDepth = 1, scopeArtifacts = false)

  @node def resources(includeSources: Boolean = true): SortedMap[SourceFileId, HashedContent] =
    findSourceFiles(isResource(includeSources))

  @node def findSourceFiles(
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue
  ): SortedMap[SourceFileId, HashedContent] = _findSourceFiles(filter, maxDepth, scopeArtifacts = true)

  @node def runconfSourceFiles: SortedMap[SourceFileId, HashedContent] = {
    val nonLocalRunConfs = findSourceFiles(isNonLocalRunConfSourceFile, maxDepth = 1)
    if (nonLocalRunConfs.nonEmpty) {
      // Do not discover anything above the workspace source root
      val parentFiles = findFilesAbove(workspaceSrcRootToSourceFolderPath, isNonLocalRunConfSourceFile)
      parentFiles ++ nonLocalRunConfs
    } else nonLocalRunConfs
  }

  @node def obtConfigSourceFiles(filesToGrab: Seq[String]): SortedMap[SourceFileId, HashedContent] = {
    val predicate = Directory.fileNamePredicate(filesToGrab: _*)
    // Don't look for obt conf files unless we've first got a runconf
    if (findSourceFiles(isNonLocalRunConfSourceFile, maxDepth = 1).nonEmpty) {
      // Do not discover anything above the workspace source root
      val parentFiles = findFilesAbove(workspaceSrcRootToSourceFolderPath, predicate)
      parentFiles ++ findSourceFiles(predicate)
    } else SortedMap.empty
  }

  @node protected[files] def _findSourceFiles(
      filter: PathFilter,
      maxDepth: Int,
      scopeArtifacts: Boolean
  ): SortedMap[SourceFileId, HashedContent]

  @node protected def findFilesAbove(
      path: RelativePath,
      fileFilter: PathFilter
  ): SortedMap[SourceFileId, HashedContent]
}

object SourceFolder {
  val isScalaOrJavaSourceFile: FileFilter = Directory.fileExtensionPredicate("java", "scala")
  private val isCppSourceFile: FileFilter = Directory.caseInsensitiveFileExtensionPredicate("cpp", "c")
  val isCppHeaderFile: FileFilter = Directory.caseInsensitiveFileExtensionPredicate("h")
  val isCppSourceOrHeaderFile: PathFilter = isCppSourceFile || isCppHeaderFile
  val isResourceFile: PathFilter = Not(isScalaOrJavaSourceFile || isCppSourceOrHeaderFile)

  val isRunConfSourceFile: PathFilter = Directory.fileExtensionPredicate(RunConfFile.extension)
  val isNonLocalRunConfSourceFile: PathFilter =
    isRunConfSourceFile && Not(PathRegexFilter(s".*\\.${RunConfFile.localExtension}"))
  val isAppScriptTemplateSourceFile: PathFilter = Directory.fileExtensionPredicate(Templates.templateFileExtensions: _*)

  def isResource(includeSources: Boolean): PathFilter = if (includeSources) NoFilter else isResourceFile
}

@entity class FileSystemSourceFolder protected (
    val sourceDir: ReactiveDirectory,
    val workspaceSourceRoot: Directory,
    val workspaceSrcRootToSourceFolderPath: RelativePath,
    factory: DirectoryFactory
) extends SourceFolder {
  import FileSystemSourceFolder._

  @node override def exists: Boolean = factory.exists(sourceDir)

  @node override def runconfSourceFiles: SortedMap[SourceFileId, HashedContent] = {
    assert(sourceDir.maxDepth == 1, s"Invalid max depth for runconfSourceFiles: ${sourceDir.maxDepth}")
    super.runconfSourceFiles
  }

  @node override def obtConfigSourceFiles(filesToGrab: Seq[String]): SortedMap[SourceFileId, HashedContent] = {
    assert(sourceDir.maxDepth == 1, s"Invalid max depth for obtConfigSourceFiles: ${sourceDir.maxDepth}")
    super.obtConfigSourceFiles(filesToGrab)
  }

  @node override protected[files] def _findSourceFiles(
      filter: PathFilter,
      maxDepth: Int,
      scopeArtifacts: Boolean
  ): SortedMap[SourceFileId, HashedContent] =
    hash(sourceDir.findFiles(filter, maxDepth = maxDepth).map { f =>
      if (scopeArtifacts) artifact(f) else nonScopeArtifact(f)
    })

  @node override protected def findFilesAbove(
      path: RelativePath,
      fileFilter: PathFilter
  ): SortedMap[SourceFileId, HashedContent] =
    path.parentOption
      .map { parent =>
        val dir = workspaceSourceRoot.resolveDir(parent)
        val reactiveParent =
          factory.lookupDirectory(dir.path, sourceDir.fileFilter && fileFilter, sourceDir.dirFilter, maxDepth = 1)
        val files = hash(reactiveParent.listFiles.map(nonScopeArtifact))
        if (parent == RelativePath.empty) files
        else files ++ findFilesAbove(parent, fileFilter)
      }
      .getOrElse(SortedMap.empty)

  @node private def hash(files: Seq[SourceFile]): SortedMap[SourceFileId, HashedContent] = {
    log.debug(s"Hashing ${files.size} local source files for ${workspaceSrcRootToSourceFolderPath.pathString}")
    files.apar.map(f => f.id -> f.hashedContent)(SortedMap.breakOut)
  }

  private def artifact(file: FileAsset): SourceFile = {
    val id = SourceFileId(
      workspaceSrcRootToSourceFilePath = workspaceSourceRoot.relativize(file),
      sourceFolderToFilePath = sourceDir.relativize(file)
    )
    SourceFile(id, file)
  }

  private def nonScopeArtifact(file: FileAsset): SourceFile = {
    // The source folder is the workspace root for runconf
    val workspaceSrcRootToSourceFilePath = workspaceSourceRoot.relativize(file)
    val id = SourceFileId(
      workspaceSrcRootToSourceFilePath = workspaceSrcRootToSourceFilePath,
      sourceFolderToFilePath = workspaceSrcRootToSourceFilePath
    )
    SourceFile(id, file)
  }
}
object FileSystemSourceFolder {
  @entity private class SourceFile(val id: SourceFileId, file: FileAsset) {
    @node def hashedContent: HashedContent = Hashing.hashFileWithContent(file)
    override def toString: String = s"${getClass.getSimpleName}($id, $file)"
  }

  def apply(
      sourceDir: ReactiveDirectory,
      workspaceSourceRoot: Directory,
      factory: DirectoryFactory
  ): FileSystemSourceFolder = {
    val workspaceSrcRootToSourceFolderPath: RelativePath = workspaceSourceRoot.relativize(sourceDir)
    FileSystemSourceFolder(sourceDir, workspaceSourceRoot, workspaceSrcRootToSourceFolderPath, factory)
  }
}
