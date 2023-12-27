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
package optimus.buildtool.builders.postbuilders.installer.component

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

import optimus.buildtool.builders.postbuilders.installer.BundleFingerprints
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.config.ExtensionConfiguration
import optimus.buildtool.config.OctalMode
import optimus.buildtool.config.Replacement
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.TokenFilter
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory._
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.trace.CopyFiles
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.BlockingQueue
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.SortedMap

class CopyFilesInstaller(
    val cache: BundleFingerprintsCache,
    scopeConfigSource: ScopeConfigurationSource,
    src: WorkspaceSourceRoot,
    val pathBuilder: InstallPathBuilder,
    factory: DirectoryFactory
) extends ComponentInstaller {

  private val fileInstructionPerTarget = new ConcurrentHashMap[FileAsset, CopyInstruction[FileAsset]]()
  private val directoryInstructionPerTarget = new ConcurrentHashMap[Directory, CopyInstruction[Directory]]()

  override val descriptor = "copied files"

  // resetting status used to detect copy files conflicts
  override def save(): Unit = {
    fileInstructionPerTarget.clear()
    directoryInstructionPerTarget.clear()
  }

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    copyFiles(installable.includedScopes)

  @async private[component] def copyFiles(includedScopes: Set[ScopeId]): Seq[FileAsset] = {
    includedScopes.apar.flatMap { scopeId =>
      ObtTrace.traceTask(scopeId, CopyFiles) {
        copyFilesForScope(scopeId)
      }
    }.toIndexedSeq
  }

  @async def copyFilesForScope(scopeId: ScopeId): Seq[FileAsset] = {
    FileCopySpec.getCopySpecs(scopeConfigSource, scopeId, src, factory).apar.flatMap { spec =>
      val targetBundle = spec.targetBundle.map(scopeConfigSource.metaBundle).getOrElse(scopeId.metaBundle)
      val (_, copiedFiles) =
        copyFilesToDir(scopeId, spec, { _ => pathBuilder.dirForMetaBundle(targetBundle, leaf = "") })
      copiedFiles
    }
  }

  @async def copyFilesToCustomDir(
      scopeId: ScopeId,
      customDirF: FileCopySpec => Directory): Seq[(Directory, Seq[FileAsset])] = {
    FileCopySpec.getCopySpecs(scopeConfigSource, scopeId, src, factory).apar.map { spec =>
      copyFilesToDir(scopeId, spec, customDirF)
    }
  }

  @async private def copyFilesToDir(
      scopeId: ScopeId,
      spec: FileCopySpec,
      targetDirF: FileCopySpec => Directory): (Directory, Seq[FileAsset]) = {
    val targetBundle = spec.targetBundle.map(scopeConfigSource.metaBundle).getOrElse(scopeId.metaBundle)
    val bundleFingerprints = cache.bundleFingerprints(targetBundle)
    val targetDir = targetDirF(spec)
    val toDir = targetDir.resolveDir(spec.into)
    Files.createDirectories(toDir.path)
    spec.compressAs match {
      case Some(tarName) => targetDir -> processAsCompressed(spec, bundleFingerprints, toDir.resolveFile(tarName))
      case None          => targetDir -> processInstructions(spec, bundleFingerprints, toDir)
    }
  }

  @async private def processInstructions(
      spec: FileCopySpec,
      bundleFingerprints: BundleFingerprints,
      toDir: Directory): Seq[FileAsset] = {
    val (directoryInstructions, fileInstructions) = CopyFilesInstaller.getCopyInstructions(
      spec,
      toDir = toDir,
      tokenReplacements = toTokenReplacements(spec),
      factory,
      src)

    // first we create all the directories
    mergeAndExecute(bundleFingerprints, directoryInstructions, directoryInstructionPerTarget)
    // then we create all the files
    mergeAndExecute(bundleFingerprints, fileInstructions, fileInstructionPerTarget)
  }

  @async private def processAsCompressed(
      spec: FileCopySpec,
      bundleFingerprints: BundleFingerprints,
      target: FileAsset): Seq[FileAsset] = {
    import spec._

    val (directoryInstructions, fileInstructions) = CopyFilesInstaller.getCompressedCopyInstructions(
      taskId = taskId,
      fromDir = from,
      target = target,
      fileFilter = fileFilter,
      fileMode = fileMode,
      dirMode = dirMode,
      tokenReplacements = toTokenReplacements(spec),
      spec.skipIfMissing,
      factory,
      src,
      extensionConfig
    )

    mergeAndExecute[Directory](bundleFingerprints, directoryInstructions, directoryInstructionPerTarget)
    mergeAndExecute[FileAsset](bundleFingerprints, fileInstructions, fileInstructionPerTarget)
  }

  @async private def mergeAndExecute[A <: Asset](
      bundleFingerprints: BundleFingerprints,
      newInstructions: Seq[CopyInstruction[A]],
      existingInstructions: ConcurrentHashMap[A, CopyInstruction[A]]): Seq[A] = {
    val updatedTargets = new BlockingQueue[A]
    newInstructions.apar.foreach { newInstruction =>
      existingInstructions.compute(
        newInstruction.target,
        { (_, existingInstruction) =>
          val updatedInstruction = newInstruction.validateAndMerge(Option(existingInstruction))
          updatedInstruction.execute(bundleFingerprints).foreach(updatedTargets.put)
          updatedInstruction
        }
      )
    }
    updatedTargets.pollAll()
  }

  private def toTokenReplacements(spec: FileCopySpec): Map[String, String] =
    spec.tokenFilters.map { tokenFilter =>
      val replacementText = tokenFilter.replacement match {
        case Replacement.InstallVersion => pathBuilder.installVersion
        case Replacement.FreeText(text) => text
      }
      tokenFilter.token -> replacementText
    }.toMap
}

object CopyFilesInstaller extends Log {

  @async private def getSources(
      directory: Directory,
      src: WorkspaceSourceRoot,
      factory: DirectoryFactory,
      fileFilter: PathFilter): Option[SortedMap[FileAsset, HashedContent]] = {
    if (src.contains(directory)) {
      val sourceFolder = factory.lookupSourceFolder(src, directory)
      if (sourceFolder.exists) {
        Option(sourceFolder.findSourceFiles(fileFilter).map { case (id, c) =>
          src.resolveFile(id.workspaceSrcRootToSourceFilePath) -> c
        })
      } else None
    } else {
      if (directory.exists) {
        val files = Directory.findFiles(directory, fileFilter)
        Option(SortedMap(files.map { f =>
          f -> Hashing.hashFileWithContent(f)
        }: _*))
      } else None
    }
  }
  @async def getCopyInstructions(
      spec: FileCopySpec,
      toDir: Directory,
      tokenReplacements: Map[String, String],
      factory: DirectoryFactory,
      src: WorkspaceSourceRoot
  ): (Seq[DirectoryCopyInstruction], Seq[FileCopyInstruction]) = {
    import spec._

    def nonExistentDirectory(): SortedMap[FileAsset, HashedContent] = {
      val msg = s"Unable to copy files from directory $from as it does not exist"
      if (skipIfMissing) { log.warn(msg); SortedMap.empty[FileAsset, HashedContent] }
      else
        throw new IllegalArgumentException(msg)
    }

    val sources = getSources(from, src, factory, fileFilter).getOrElse(nonExistentDirectory())

    val dirInstructions = sources.keySet.iterator
      .map(_.parent)
      .map { d =>
        val to = toDir.resolveDir(from.relativize(d))
        DirectoryCopyInstruction(taskId = taskId, source = d, target = to, mode = dirMode)
      }
      .toIndexedSeq

    val fileInstructions = sources.toIndexedSeq.apar.map { case (f, c) =>
      FileCopyInstruction(
        taskId = taskId,
        source = f,
        sourceContent = c,
        target = toDir.resolveFile(from.relativize(f)),
        mode = fileMode.orElse(extensionConfig.modeForExtension(f)),
        tokenReplacements
      )
    }.toIndexedSeq

    (dirInstructions, fileInstructions)
  }

  @async def getCompressedCopyInstructions(
      taskId: String,
      fromDir: Directory,
      target: FileAsset,
      fileFilter: PathFilter,
      fileMode: Option[OctalMode],
      dirMode: Option[OctalMode],
      tokenReplacements: Map[String, String],
      skipIfMissing: Boolean,
      factory: DirectoryFactory,
      src: WorkspaceSourceRoot,
      extensions: ExtensionConfiguration
  ): (Seq[DirectoryCopyInstruction], Seq[CompressedCopyInstruction]) = {

    val sourceFolder = factory.lookupSourceFolder(src, fromDir)
    if (sourceFolder.exists) {
      val directoryInstruction = DirectoryCopyInstruction(
        taskId = taskId,
        source = fromDir,
        target = target.parent,
        mode = dirMode
      )

      val sourceContents = factory.lookupSourceFolder(src, fromDir).findSourceFiles(fileFilter).map { case (id, c) =>
        src.resolveFile(id.workspaceSrcRootToSourceFilePath) -> c
      }

      val fileInstruction = CompressedCopyInstruction(
        taskId = taskId,
        assetsDir = fromDir,
        sourceContents = sourceContents,
        target = target,
        mode = fileMode.orElse(extensions.modeForExtension(target)),
        tokenReplacements = tokenReplacements
      )

      (Seq(directoryInstruction), Seq(fileInstruction))
    } else {
      val msg = s"Unable to copy files from directory $fromDir as it does not exist"
      if (skipIfMissing) { log.warn(msg); (Nil, Nil) }
      else throw new IllegalArgumentException(msg)
    }
  }

}

final case class FileCopySpec(
    taskId: String,
    from: Directory,
    into: RelativePath,
    fileMode: Option[OctalMode],
    dirMode: Option[OctalMode],
    targetBundle: Option[String],
    fileFilter: PathFilter,
    compressAs: Option[String],
    tokenFilters: Seq[TokenFilter],
    skipIfMissing: Boolean,
    extensionConfig: ExtensionConfiguration
)

object FileCopySpec extends Log {
  val fileSystem: FileSystem = FileSystems.getDefault

  @async def getCopySpecs(
      scopeConfigSource: ScopeConfigurationSource,
      scopeId: ScopeId,
      src: WorkspaceSourceRoot,
      factory: DirectoryFactory
  ): Seq[FileCopySpec] = {
    scopeConfigSource.copyFilesConfiguration(scopeId).toIndexedSeq.apar.flatMap { config =>
      val absScopeRoot = scopeConfigSource.scopeConfiguration(scopeId).paths.absScopeRoot
      config.tasks.toIndexedSeq.apar.map { task =>
        val fromDir =
          if (task.from.isAbsolute) Directory(task.from) else absScopeRoot.resolveDir(RelativePath(task.from))

        if (fromDir.exists && !Files.isDirectory(fromDir.path)) {
          // you can't create a Directory from a non-directory, of course, but that assertion is behind a flag
          // for performance's sake. Here it's also a correctness issue, though!
          throw new IllegalArgumentException(s"copyFiles source must be a directory: '$fromDir'")
        }

        def toFilter(fs: Seq[String]): PathFilter = {
          val matchers = fs.map(f => fileSystem.getPathMatcher(s"glob:$f"))
          PredicateFilter { p =>
            val relativePath = if (p.isAbsolute) Pathed.relativize(fromDir.path, p) else p
            matchers.exists(_.matches(relativePath))
          }
        }

        val filter = List(
          task.includes.map(toFilter),
          task.excludes.map(toFilter).map(Not)
        ).flatten match {
          case Nil        => NoFilter
          case one :: Nil => one
          case all        => all.foldLeft[PathFilter](NoFilter)(_ && _)
        }

        val extensionConfig = scopeConfigSource.extensionConfiguration(scopeId).getOrElse(ExtensionConfiguration.Empty)

        FileCopySpec(
          taskId = task.id,
          from = fromDir,
          into = task.into,
          fileMode = task.fileMode,
          dirMode = task.dirMode,
          targetBundle = task.targetBundle,
          fileFilter = RegularFileFilter && filter,
          compressAs = task.compressAs,
          tokenFilters = task.filters,
          task.skipIfMissing,
          extensionConfig
        )
      }
    }
  }
}
