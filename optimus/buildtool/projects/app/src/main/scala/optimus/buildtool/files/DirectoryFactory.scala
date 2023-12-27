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

import java.nio.file.Path

import optimus.buildtool.files.Directory._
import optimus.buildtool.files.ReactiveDirectory.SimpleReactiveDirectory
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.GitUtils
import optimus.platform._
import org.eclipse.jgit.lib.Constants

final case class ScanDetails(watchedPaths: Set[Path], scannedFiles: Int)
object ScanDetails {
  val empty = ScanDetails(Set.empty, 0)
}

@entity trait SourceFolderFactory {
  @node def lookupSourceFolder(workspaceSourceRoot: Directory, sourceDir: Directory): SourceFolder
}

@entity trait LocalDirectoryFactory {
  @scenarioIndependent @node def reactive(dir: Directory): ReactiveDirectory =
    lookupDirectory(dir.path, dir.fileFilter, dir.dirFilter, dir.maxDepth)

  @scenarioIndependent @node def lookupDirectory(
      path: Path,
      fileFilter: PathFilter = RegularFileFilter,
      dirFilter: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): ReactiveDirectory

  @node def exists(dir: Directory): Boolean = {
    // NoFilter here to ensure we track changes to directories as well as files
    val reactive = lookupDirectory(dir.path, fileFilter = NoFilter, maxDepth = 0)
    reactive.declareVersionDependence()
    reactive.existsUnsafe // we can use existsUnsafe here because we're explicitly tracking the directory
  }

  @node def fileExists(file: FileAsset): Boolean = {
    val reactive = lookupDirectory(file.parent.path, fileFilter = exactMatchPredicate(file), maxDepth = 1)
    reactive.declareVersionDependence()
    file.existsUnsafe // we can use existsUnsafe here because we're explicitly tracking the parent directory
  }

  def close(): Unit
  // noinspection AccessorLikeMethodIsEmptyParen
  def getTweaksAndReset(): Seq[Tweak]

  def scanDetails: ScanDetails
}

@entity trait DirectoryFactory extends LocalDirectoryFactory with SourceFolderFactory

@entity object NonReactiveDirectoryFactory extends DirectoryFactory {
  @scenarioIndependent @node def lookupDirectory(
      path: Path,
      fileFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int): ReactiveDirectory =
    SimpleReactiveDirectory(path, fileFilter = fileFilter, dirFilter = dirFilter, maxDepth)

  @scenarioIndependent @node def lookupSourceFolder(
      workspaceSourceRoot: Directory,
      sourceDir: Directory
  ): SourceFolder = {
    FileSystemSourceFolder(reactive(sourceDir), workspaceSourceRoot, this)
  }

  def close(): Unit = ()
  override def getTweaksAndReset(): Seq[Tweak] = Nil
  override def scanDetails: ScanDetails = ScanDetails(Set.empty, 0)
}

@entity class ReactiveDirectoryFactory(private val watcher: DirectoryWatcher) extends DirectoryFactory {
  @scenarioIndependent @node def lookupDirectory(
      path: Path,
      fileFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int): ReactiveDirectory = {
    // note that registering is idempotent so if we get a cache miss on this node and do it again, it's fine
    watcher.registerPath(path, notificationFilter = fileFilter, dirFilter = dirFilter, maxDepth)
    SimpleReactiveDirectory(path, fileFilter = fileFilter, dirFilter = dirFilter, maxDepth)
  }

  @scenarioIndependent @node def lookupSourceFolder(
      workspaceSourceRoot: Directory,
      sourceDir: Directory
  ): SourceFolder = {
    // note that registering is idempotent so if we get a cache miss on this node and do it again, it's fine
    watcher.registerPath(
      sourceDir.path,
      notificationFilter = sourceDir.fileFilter,
      dirFilter = sourceDir.dirFilter,
      sourceDir.maxDepth)
    ObtTrace.addToStat(ObtStats.FileSystemSourceFolder, 1)
    FileSystemSourceFolder(reactive(sourceDir), workspaceSourceRoot, this)
  }

  def close(): Unit = watcher.close()

  override def getTweaksAndReset(): Seq[Tweak] =
    watcher
      .getModifiedPathsAndReset()
      .map { p =>
        val msg = s"Directory has changed: $p"
        ObtTrace.info(msg)
        log.info(msg)
        ReactiveDirectory.tweakVersion(p)
      }
      .toSeq

  override def scanDetails: ScanDetails = ScanDetails(watcher.watchedPaths, watcher.scannedFiles)
}

@entity class GitSourceFolderFactory(utils: GitUtils) extends SourceFolderFactory {

  @node override def lookupSourceFolder(
      workspaceSourceRoot: Directory,
      sourceDir: Directory
  ): SourceFolder = {
    val workspaceSourceRootToSourceFolder = workspaceSourceRoot.relativize(sourceDir)
    val gitSource = GitSourceFolder(
      workspaceSourceRootToSourceFolder,
      workspaceSourceRoot,
      utils.repo,
      this,
      utils.commit(Constants.HEAD)
    )
    log.trace(s"Using git for source folder: $gitSource")
    ObtTrace.addToStat(ObtStats.GitSourceFolder, 1)
    gitSource
  }
}

@entity class MultiLevelDirectoryFactory(local: LocalDirectoryFactory, factories: Seq[SourceFolderFactory])
    extends DirectoryFactory {

  @scenarioIndependent @node override def lookupDirectory(
      path: Path,
      fileFilter: PathFilter,
      dirFilter: PathFilter,
      maxDepth: Int
  ): ReactiveDirectory = local.lookupDirectory(path, fileFilter, dirFilter, maxDepth)

  @node override def lookupSourceFolder(
      workspaceSourceRoot: Directory,
      sourceDir: Directory
  ): SourceFolder = _lookupSourceFolder(factories, workspaceSourceRoot, sourceDir)

  // noinspection NoTailRecursionAnnotation
  @node private def _lookupSourceFolder(
      fs: Seq[SourceFolderFactory],
      workspaceSourceRoot: Directory,
      sourceDir: Directory
  ): SourceFolder = fs match {
    case h +: t =>
      val sf = h.lookupSourceFolder(workspaceSourceRoot, sourceDir)
      if (sf.exists) sf else _lookupSourceFolder(t, workspaceSourceRoot, sourceDir)
    case Seq() =>
      factories.head.lookupSourceFolder(workspaceSourceRoot, sourceDir) // if none exist then just use head
  }

  override def close(): Unit = local.close()
  override def getTweaksAndReset(): Seq[Tweak] = local.getTweaksAndReset()
  override def scanDetails: ScanDetails = local.scanDetails
}
