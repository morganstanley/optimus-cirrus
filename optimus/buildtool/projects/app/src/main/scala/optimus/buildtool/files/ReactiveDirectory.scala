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

import java.nio.file.FileSystem
import java.nio.file.Path

import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.fileExtensionPredicate
import optimus.platform._

import scala.collection.immutable.Seq

@entity trait PathedEntity {
  self: Pathed =>

  def fileSystem: FileSystem = Pathed.fileSystem(path)
  def name: String = Pathed.name(path)
  def pathString: String = Pathed.pathString(path)
  def uriString: String = Pathed.uriString(path)
  def pathFingerprint: String = Pathed.pathFingerprint(path)
  def allPathFormats: Seq[String] = Pathed.allPathFormats(path)
  @impure def exists: Boolean = Pathed.exists(path)
  // Potentially unsafe to call from a @node since Files.exists is not RT
  def existsUnsafe: Boolean = Pathed.existsUnsafe(path)
}

@entity trait AssetEntity extends PathedEntity {
  self: Asset =>

  def parent: Directory = {
    val p = path.getParent
    require(p != null, s"No parent for $path")
    Directory(p)
  }

  override def toString: String = pathString
}

@entity trait DirectoryEntity extends AssetEntity {
  self: Directory =>

  Directory.validate(this)

  final def parentOption: Option[Directory] = Option(path.getParent).map(Directory(_))

  final def contains(other: Asset): Boolean = other.path.normalize.startsWith(this.path.normalize)
  final def isChildOf(other: Directory): Boolean = this.path.normalize.startsWith(other.path.normalize)

  final def relativize(a: Asset): RelativePath = RelativePath(Pathed.relativize(path, a.path))

  final def resolveFile(p: RelativePath): FileAsset = FileAsset(Pathed.resolve(path, p.path))
  final def resolveFile(s: String): FileAsset = FileAsset(path.resolve(s))
  final def resolveTimestampedFile(p: RelativePath): FileAsset = FileAsset.timestamped(Pathed.resolve(path, p.path))
  final def resolveTimestampedFile(s: String): FileAsset = FileAsset.timestamped(path.resolve(s))

  final def resolveJar(p: RelativePath): JarAsset = JarAsset(Pathed.resolve(path, p.path))
  final def resolveJar(s: String): JarAsset = JarAsset(path.resolve(s))

  final def resolveDir(p: RelativePath): Directory = resolveDir(p.path.toString)
  final def resolveDir(s: String, maxDepth: Int = Int.MaxValue): Directory =
    Directory(path.resolve(s), fileFilter = fileFilter, dirFilter = dirFilter, maxDepth = maxDepth)

  def copy(
      path: Path = path,
      fileFilter: PathFilter = fileFilter,
      dirFilter: PathFilter = dirFilter,
      maxDepth: Int = maxDepth
  ): Directory =
    Directory(path, fileFilter = fileFilter, dirFilter = dirFilter, maxDepth = maxDepth)

  /** a filter which determines which directories to visit in any files/findFiles calls */
  protected[files] def dirFilter: PathFilter = NoFilter

  /** a filter which is applied (in addition to any caller-specified filters) to any files/findFiles calls */
  protected[files] def fileFilter: PathFilter = NoFilter
  protected[files] def maxDepth: Int = Int.MaxValue
}

@entity trait ReactiveDirectory extends Directory with DirectoryEntity {
  import ReactiveDirectory._

  private val _version = DirectoryVersion(pathString)
  @node final def declareVersionDependence(): Unit = {
    // read version number simply to establish (fake) dependency
    require(_version.version >= 0)
  }
  @node private[files] def peekAtVersionForUnitTest: Int = _version.version

  @node final def listFiles: Seq[FileAsset] = {
    declareVersionDependence()
    Directory.listFilesUnsafe(this, fileFilter)
  }

  @node final def findFiles(predicate: PathFilter = NoFilter, maxDepth: Int = Int.MaxValue): Seq[FileAsset] = {
    declareVersionDependence()
    Directory.findFilesUnsafe(this, predicate, maxDepth = maxDepth)
  }

  @node final def findDirectories(predicate: PathFilter = NoFilter): Seq[Directory] = {
    declareVersionDependence()
    Directory.findDirectoriesUnsafe(this, predicate)
  }

  @node final def files: Seq[FileAsset] = findFiles()

  @node final def containsFilesWithExtension(extn: String): Boolean = findFiles(fileExtensionPredicate(extn)).nonEmpty
}

object ReactiveDirectory {
  @entity private class DirectoryVersion(path: String) {
    // a (fake) version number representing the state of the filesystem that we have read. By incrementing this we can
    // invalidate the cache and cause a re-read of the filesystem
    @node(tweak = true) private[files] def version: Int = 1
  }
  @entersGraph def tweakVersion(p: Path): Tweak = {
    val v = DirectoryVersion(Pathed.pathString(p))
    Tweak.byValue(v.version := v.version + 1)
  }

  @entity private[files] class SimpleReactiveDirectory(
      val path: Path,
      override val fileFilter: PathFilter,
      override val dirFilter: PathFilter,
      override val maxDepth: Int)
      extends ReactiveDirectory
}
