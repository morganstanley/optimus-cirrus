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
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
import java.util.regex.Pattern
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NamingConventions.TempDir
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.PathUtils.ErrorIgnoringFileVisitor
import optimus.platform.impure

import java.util.UUID
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Try
import scala.collection.compat._

trait Directory extends DirectoryAsset {
  import Directory.PathFilter

  def parentOption: Option[Directory]

  def contains(other: Asset): Boolean
  def isChildOf(other: Directory): Boolean

  def relativize(a: Asset): RelativePath

  def resolveFile(p: RelativePath): FileAsset
  def resolveFile(s: String): FileAsset
  def resolveTimestampedFile(p: RelativePath): FileAsset
  def resolveTimestampedFile(s: String): FileAsset

  def resolveJar(p: RelativePath): JarAsset
  def resolveJar(s: String): JarAsset

  def resolveDir(p: RelativePath): Directory
  def resolveDir(s: String, maxDepth: Int = Int.MaxValue): Directory

  def copy(
      path: Path = path,
      fileFilter: PathFilter = fileFilter,
      dirFilter: PathFilter = dirFilter,
      maxDepth: Int = maxDepth
  ): Directory

  /** a filter which determines which directories to visit in any files/findFiles calls */
  protected[files] def dirFilter: PathFilter

  /** a filter which is applied (in addition to any caller-specified filters) to any files/findFiles calls */
  protected[files] def fileFilter: PathFilter
  protected[files] def maxDepth: Int
}

// We can't make Directory an @entity (since platform isn't on the classpath) but we do want entities to be able to extend
// it, so we need to separate out implementation into a subtrait
trait DirectoryOps extends AssetOps {
  self: Directory =>

  Directory.validate(this)
  import Directory._

  final def parentOption: Option[Directory] = Option(path.getParent).map(Directory(_))

  final def contains(other: Asset): Boolean = other.path.normalize.startsWith(this.path.normalize)
  final def isChildOf(other: Directory): Boolean = this.path.normalize.startsWith(other.path.normalize)

  final def relativize(a: Asset): RelativePath = RelativePath(Pathed.relativize(path, a.path))
  final def relativize(a: Directory): RelativePath = RelativePath(Pathed.relativize(path, a.path))

  // Note: We resolve based on String here and below, since we don't want to throw if p.path
  // is on a different filesystem from `path`. We don't however use pathString, since that could cause issues for
  // UNIX paths with `\` in their name.
  final def resolveFile(p: RelativePath): FileAsset = resolveFile(p.path.toString)
  final def resolveFile(s: String): FileAsset = FileAsset(path.resolve(s))
  final def resolveTimestampedFile(p: RelativePath): FileAsset = resolveTimestampedFile(p.pathString)
  final def resolveTimestampedFile(s: String): FileAsset = FileAsset.timestamped(path.resolve(s))

  final def resolveJar(p: RelativePath): JarAsset = resolveJar(p.pathString)
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

object Directory {
  private[files] final case class SimpleDirectory(
      path: Path,
      override val fileFilter: PathFilter,
      override val dirFilter: PathFilter,
      override val maxDepth: Int
  ) extends Directory
      with DirectoryOps

  def apply(
      dir: Path,
      fileFilter: PathFilter = NoFilter,
      dirFilter: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): Directory =
    SimpleDirectory(dir.normalize, fileFilter = fileFilter, dirFilter = dirFilter, maxDepth)

  def root(fs: FileSystem): Directory = Directory(fs.getRootDirectories.iterator.next())

  def validate(d: Directory): Unit = {
    OptimusBuildToolAssertions.require(
      (PathUtils.isAbsolute(d.path) && (!Files.exists(d.path) || Files.isDirectory(d.path))) ||
        (d.path.getFileSystem == NamingConventions.BUILD.getFileSystem && d.path.startsWith(NamingConventions.BUILD)),
      s"Invalid directory path: ${d.path}"
    )
  }

  // Async (non-memoized) version of dir.findFiles
  @impure final def findFiles(
      dir: Directory,
      predicate: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[FileAsset] =
    findFilesUnsafe(dir, predicate, maxDepth)
  // findFilesUnsafe should only be called from a @node if you're manually tracking changes to dir, or if you
  // can guarantee that the contents of the directory are truly RT given the inputs to the @node. Any use
  // outside of Directory/ReactiveDirectory should be accompanied by a comment.
  final def findFilesUnsafe(
      dir: Directory,
      predicate: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[FileAsset] =
    findFilePaths(dir, predicate, maxDepth).map { case (p, lm) => FileAsset.timestamped(p, lm) }

  // Async (non-memoized) version of dir.findDirectories
  @impure final def findDirectories(
      dir: Directory,
      predicate: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[Directory] =
    findDirectoriesUnsafe(dir, predicate, maxDepth)
  // `findDirectoriesUnsafe` should only be called from a @node if you're manually tracking changes to dir, or if you
  // can guarantee that the contents of the directory are truly RT given the inputs to the @node. Any use
  // outside of Directory/ReactiveDirectory should be accompanied by a comment.
  final def findDirectoriesUnsafe(
      dir: Directory,
      predicate: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[Directory] =
    Directory.findDirectoryPaths(dir, predicate, maxDepth).map(Directory(_))

  // Async (non-memoized) version of dir.listFiles
  @impure def listFiles(dir: Directory, filter: PathFilter = NoFilter): Seq[FileAsset] =
    listFilesUnsafe(dir, filter)
  // `listFilesUnsafe` should only be called from a @node if you're manually tracking changes to dir, or if you
  // can guarantee that the contents of the directory are truly RT given the inputs to the @node. Any use outside
  // of Directory/ReactiveDirectory should be accompanied by a comment.
  def listFilesUnsafe(dir: Directory, filter: PathFilter = NoFilter): Seq[FileAsset] =
    findFilesUnsafe(dir, filter, maxDepth = 1)

  @impure def listDirectories(dir: Directory, filter: PathFilter = NoFilter): Seq[Directory] =
    findDirectories(dir, filter, maxDepth = 2).filter(_ != dir) // maxDepth = 1 only gives us `Seq(dir)`

  private def findFilePaths(
      dir: Directory,
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[(Path, Instant)] = {
    val depth = Math.min(dir.maxDepth, maxDepth)
    val fileFilter = RegularFileFilter && dir.fileFilter && filter
    val paths: mutable.Buffer[(Path, Instant)] = mutable.Buffer.empty
    val visitor = new ErrorIgnoringFileVisitor() {
      override def preVisitDirectory(d: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (dir.dirFilter(d, attrs)) FileVisitResult.CONTINUE
        else FileVisitResult.SKIP_SUBTREE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (fileFilter(file, attrs)) paths.append((file, attrs.lastModifiedTime.toInstant))
        FileVisitResult.CONTINUE
      }
    }
    Files.walkFileTree(dir.path, java.util.Collections.emptySet(), depth, visitor)
    paths.sorted.to(Seq)
  }

  private def findDirectoryPaths(
      dir: Directory,
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[Path] = {
    val depth = Math.min(dir.maxDepth, maxDepth)
    val paths: mutable.Buffer[Path] = mutable.Buffer.empty
    val visitor = new ErrorIgnoringFileVisitor() {
      override def preVisitDirectory(directory: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (dir.dirFilter(directory, attrs)) {
          if (filter(directory, attrs)) paths.append(directory)
          FileVisitResult.CONTINUE
        } else FileVisitResult.SKIP_SUBTREE
      }
    }
    Files.walkFileTree(dir.path, java.util.Collections.emptySet(), depth, visitor)
    paths.sorted.to(Seq)
  }

  def findAssets(dir: Directory, filter: PathFilter = NoFilter): Seq[Asset] =
    findAssetsUnsafe(dir, filter)

  // `findAssetsUnsafe` should only be called from a @node if you're manually tracking changes to dir, or if you
  // can guarantee that the contents of the directory are truly RT given the inputs to the @node. Any use outside
  // of Directory/ReactiveDirectory should be accompanied by a comment.
  def findAssetsUnsafe(dir: Directory, filter: PathFilter = NoFilter): Seq[Asset] = {
    findPaths(dir, filter).map {
      case (p, true)  => FileAsset.timestamped(p)
      case (p, false) => Directory(p)
    }
  }

  private def findPaths(
      dir: Directory,
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[(Path, Boolean)] = {
    val depth = Math.min(dir.maxDepth, maxDepth)
    val paths: mutable.Buffer[(Path, Boolean)] = mutable.Buffer.empty
    val visitor = new ErrorIgnoringFileVisitor() {
      override def preVisitDirectory(directory: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (dir.dirFilter(directory, attrs)) {
          if (filter(directory, attrs)) paths.append((directory, false))
          FileVisitResult.CONTINUE
        } else FileVisitResult.SKIP_SUBTREE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (filter.apply(file, attrs)) paths.append((file, true))
        FileVisitResult.CONTINUE
      }
    }
    Files.walkFileTree(dir.path, java.util.Collections.emptySet(), depth, visitor)
    paths.sorted.to(Seq)
  }

  // Use stable classes below to ensure we get cache hits when reusing predicates
  trait PathFilter {
    def apply(pathString: String, attrs: BasicFileAttributes): Boolean = apply(Paths.get(pathString), attrs)
    def apply(path: Path, attrs: BasicFileAttributes): Boolean
    final def &&(other: PathFilter): PathFilter = merge(other)(IntersectionFilter)
    final def ||(other: PathFilter): PathFilter = merge(other)(UnionFilter)

    private def merge(other: PathFilter)(op: (PathFilter, PathFilter) => PathFilter): PathFilter = (this, other) match {
      case (NoFilter, b)    => b
      case (a, NoFilter)    => a
      case (a, b) if a == b => a
      case (a, b)           => op(a, b)
    }
  }

  trait FileFilter extends PathFilter {
    def apply(f: FileAsset): Boolean
  }

  final case class IntersectionFilter(a: PathFilter, b: PathFilter) extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean =
      a.apply(pathString, attrs) && b.apply(pathString, attrs)
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      a.apply(path, attrs) && b.apply(path, attrs)
  }

  final case class UnionFilter(a: PathFilter, b: PathFilter) extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean =
      a.apply(pathString, attrs) || b.apply(pathString, attrs)
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      a.apply(path, attrs) || b.apply(path, attrs)
  }

  case object RegularFileFilter extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean = attrs.isRegularFile
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean = attrs.isRegularFile
  }

  case object NotHiddenFilter extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean =
      !PathUtils.fileName(pathString).startsWith(".")
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean = !path.getFileName.toString.startsWith(".")
  }

  final case class ExclusionFilter(pathsToExclude: Set[Path]) extends PathFilter {
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean = !pathsToExclude.contains(path)
  }

  final case class PredicateFilter(predicate: Path => Boolean) extends PathFilter {
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean = predicate(path)
  }

  object ExclusionFilter {
    def apply(assets: Asset*): ExclusionFilter = {
      val paths: Set[Path] = assets.map(_.path).toSet
      ExclusionFilter(paths)
    }
  }

  // Constructor arg is a String, since Regexes and Patterns don't implement equals/hashCode
  final case class PathRegexFilter(regex: String) extends PathFilter {
    private val pattern: Pattern = Pattern.compile(regex)
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean =
      pattern.matcher(pathString).matches
    def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      pattern.matcher(Pathed.pathString(path)).matches
  }

  final case class EndsWithFilter(paths: RelativePath*) extends PathFilter {
    def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      paths.exists(p => path.endsWith(p.path))
  }

  final case class Not(filter: PathFilter) extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean = !filter(pathString, attrs)
    def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      !filter(path, attrs)
  }

  /** Matches all ancestors and all descendants of the provided path. */
  final case class AncestryFilter(target: Path) extends PathFilter {
    def apply(path: Path, attrs: BasicFileAttributes): Boolean =
      path.startsWith(target) || target.startsWith(path.getParent)
  }

  case object NoFilter extends PathFilter {
    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean = true
    override def apply(p: Path, a: BasicFileAttributes): Boolean = true
  }

  final case class ExtensionPredicate(extns: String*) extends FileFilter {
    override def apply(p: String, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && extns.contains(NamingConventions.suffix(p))
    override def apply(p: Path, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && extns.contains(NamingConventions.suffix(p))

    override def apply(f: FileAsset): Boolean = extns.contains(NamingConventions.suffix(f))
  }
  def fileExtensionPredicate(extns: String*): FileFilter = ExtensionPredicate(extns: _*)

  private final case class CaseInsensitiveExtensionPredicate(extns: String*) extends FileFilter {
    private val lowerExtns: Seq[String] = extns.map(_.toLowerCase).to(Seq)
    override def apply(p: String, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && lowerExtns.contains(NamingConventions.suffix(p).toLowerCase)
    override def apply(p: Path, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && lowerExtns.contains(NamingConventions.suffix(p).toLowerCase)

    override def apply(f: FileAsset): Boolean = lowerExtns.contains(NamingConventions.suffix(f).toLowerCase)
  }
  def caseInsensitiveFileExtensionPredicate(extns: String*): FileFilter = CaseInsensitiveExtensionPredicate(extns: _*)

  private final case class ExactMatchPredicate(files: FileAsset*) extends PathFilter {
    override def apply(p: String, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && files.exists(f => f.pathString == PathUtils.platformIndependentString(p))
    override def apply(p: Path, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && files.exists(_.path == p)
  }
  def exactMatchPredicate(files: FileAsset*): PathFilter = ExactMatchPredicate(files: _*)

  private final case class FileNamePredicate(fileNames: String*) extends PathFilter {
    override def apply(p: String, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && fileNames.contains(PathUtils.fileName(p))
    override def apply(p: Path, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && fileNames.contains(p.getFileName.toString)
  }
  def fileNamePredicate(fileNames: String*): PathFilter = FileNamePredicate(fileNames: _*)

  private final case class ConsistentHashPredicate() extends PathFilter {
    override def apply(p: String, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && Hashing.fileCanBeStampedWithConsistentHash(p)
    override def apply(p: Path, a: BasicFileAttributes): Boolean =
      RegularFileFilter(p, a) && Hashing.fileCanBeStampedWithConsistentHash(FileAsset(p))
  }
  val consistentHashPredicate: PathFilter = ConsistentHashPredicate()

  /** Creates a new temporary directory that will be recursively wiped after the process exits. */
  def temporary(pre: String = "obt", root: Option[Directory] = None): Directory = {
    val uuid = UUID.randomUUID().toString.replace("-", "")
    val dir = root.getOrElse(TempDir).resolveDir(s"$pre-$uuid")
    Files.createDirectories(dir.path)
    tempHook.synchronized(tempHook.toDelete += dir)
    dir
  }
  private object tempHook extends Thread("temporary-directory deleter") {
    Runtime.getRuntime.addShutdownHook(this)
    val toDelete: mutable.Builder[Directory, List[Directory]] = List.newBuilder[Directory]
    override def run(): Unit = if (!sys.props.get("optimus.buildtool.deleteTemporaryDirectories").contains("false")) {
      toDelete.result().foreach(d => Try(AssetUtils.recursivelyDelete(d))) // best effort
    }
  }
}
