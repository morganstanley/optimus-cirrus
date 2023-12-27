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

import java.net.URL
import java.nio.file.AccessDeniedException
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.FileAsset.SimpleFileAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.utils.PathUtils
import optimus.platform.impure
import spray.json.JsonFormat

import scala.collection.immutable.Seq

trait Pathed {

  /**
   * Note that this should not be considered part of the cacheable identity of the Asset, since it will often differ
   * between workspaces.
   */
  def path: Path
  def fileSystem: FileSystem
  def name: String
  def pathString: String
  def uriString: String
  def pathFingerprint: String
  def allPathFormats: Seq[String]
  @impure def exists: Boolean
  // Potentially unsafe to call from a @node since Files.exists is not RT
  def existsUnsafe: Boolean
}

// We can't make Pathed an @entity (since platform isn't on the classpath) but we do want entities to be able to extend
// it, so we need to separate out implementation into a subtrait
trait PathedOps {
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

object Pathed {
  private[buildtool] def name(pathString: String): String = name(Paths.get(pathString))

  private[buildtool] def fileSystem(p: Path): FileSystem = p.getFileSystem

  private[buildtool] def name(p: Path): String = p.getFileName.toString

  private[buildtool] def pathString(p: Path): String = PathUtils.platformIndependentString(p.normalize)

  private[buildtool] def uriString(p: Path): String = PathUtils.uriString(p)

  private[buildtool] def pathFingerprint(p: Path): String = PathUtils.pathFingerprint(p)

  private[buildtool] def allPathFormats(p: Path): Seq[String] = {
    val ps = pathString(p)
    val singleSlashPrefix =
      if (ps.length > 1 && ps.charAt(0) == '/' && ps.charAt(1) == '/') Some(ps.substring(1))
      else None

    Seq(ps) ++ singleSlashPrefix
  }

  @impure private[buildtool] def exists(p: Path): Boolean = Files.exists(p)

  // Potentially unsafe to call from a @node since Files.exists is not RT
  private[buildtool] def existsUnsafe(p: Path): Boolean = Files.exists(p)

  def resolve(parent: Path, child: Path): Path =
    try {
      parent.resolve(child)
    } catch {
      case e: IllegalArgumentException =>
        // Provide a more useful exception message than "'other' is different type of Path"
        throw new IllegalArgumentException(
          s"Failed to resolve ${child.getClass.getSimpleName}($child) based on ${parent.getClass.getSimpleName}($parent)",
          e
        )
    }

  def relativize(parent: Path, child: Path): Path =
    try {
      parent.relativize(child)
    } catch {
      case e: IllegalArgumentException =>
        // Provide a more useful exception message than "'other' is different type of Path"
        throw new IllegalArgumentException(
          s"Failed to relativize ${child.getClass.getSimpleName}($child) based on ${parent.getClass.getSimpleName}($parent)",
          e
        )
    }

}

sealed trait Asset extends Pathed {
  def parent: Directory
}
trait AssetOps extends PathedOps {
  self: Asset =>

  def parent: Directory = {
    val p = path.getParent
    require(p != null, s"No parent for $path")
    Directory(p)
  }

  override def toString: String = pathString
}
object Asset {
  def parse(fs: FileSystem, file: String): Pathed = {
    val sepIndex = file.indexOf('!')
    if (sepIndex < 0) {
      val p = fs.getPath(file)
      if (isJdkPlatformPath(PathUtils.platformIndependentString(p))) JdkPlatformAsset(p)
      else if (PathUtils.isAbsolute(p) || isBuildPath(fs, p)) apply(file, p)
      else RelativePath(p)
    } else {
      val (jar, fileInJar) = file.splitAt(sepIndex)
      FileInJarAsset(JarAsset(fs.getPath(jar)), fileInJar.substring(1))
    }
  }

  def apply(fs: FileSystem, file: String): Asset = {
    apply(file, fs.getPath(file))
  }

  def apply(p: Path): Asset = apply(Pathed.pathString(p), p)

  private def isBuildPath(fs: FileSystem, p: Path): Boolean =
    fs == NamingConventions.BUILD.getFileSystem && p.startsWith(NamingConventions.BUILD)

  def isJdkPlatformPath(s: String): Boolean =
    (s.startsWith("//") && s.endsWith(".sig")) || s.startsWith("//modules/") || s.startsWith("/modules/")

  private def apply(file: String, p: Path): Asset = {
    if (file.endsWith(".jar")) JarAsset(p)
    else if (Files.isDirectory(p)) Directory(p)
    else FileAsset(p)
  }
}

sealed trait FileAsset extends Asset with AssetOps {
  if (OptimusBuildToolAssertions.enabled) validate()

  protected def validate(): Unit =
    require(
      (PathUtils.isAbsolute(path) && (!Files.exists(path) || Files.isRegularFile(path))) ||
        (path.getFileSystem == NamingConventions.BUILD.getFileSystem && path.startsWith(NamingConventions.BUILD)),
      s"Invalid file path: $path"
    )

  def lastModified: Instant

  def asFile: FileAsset = SimpleFileAsset(path, lastModified)
  def asJar: JarAsset = JarFileSystemAsset(path, lastModified)
  def asJson: JsonAsset = JsonAsset(path, lastModified)

  // FileAssets should never terminate in a "/", but on Windows then the default Path behaviour is to append
  // one if the path is only two elements long (eg. "//a/b")
  override def pathString: String = super.pathString.stripSuffix("/")

  override def toString: String =
    if (lastModified == Instant.EPOCH) s"$simpleName($pathString)"
    else s"$simpleName($pathString, $lastModified)"

  protected def simpleName: String = getClass.getSimpleName
}

object FileAsset {
  implicit object FAOrdering extends Ordering[FileAsset] {
    // Note: We're ordering based on String rather than Path here, since path comparison is case-sensitive on linux
    // but case-insensitive on windows. This leads to remote cache misses when reusing artifacts between OSes.
    override def compare(x: FileAsset, y: FileAsset): Int = x.pathString.compareTo(y.pathString)
  }

  private[files] final case class SimpleFileAsset(path: Path, lastModified: Instant) extends FileAsset {
    override protected def simpleName: String = classOf[FileAsset].getSimpleName
  }

  def apply(file: Path): FileAsset = SimpleFileAsset(file, Instant.EPOCH)

  def timestamped(file: Path): FileAsset = SimpleFileAsset(file, lastModified(file))

  private[files] def timestamped(file: Path, lastModified: Instant): FileAsset = SimpleFileAsset(file, lastModified)

  private[files] def lastModified(path: Path): Instant = {
    try {
      Files.getLastModifiedTime(path).toInstant
    } catch {
      case _: NoSuchFileException | _: AccessDeniedException => Instant.EPOCH
    }
  }

  val NoFile = new FileAsset {
    override protected def validate(): Unit = ()
    override def lastModified: Instant = throw new UnsupportedOperationException("last modified of no file")
    override def path: Path = throw new UnsupportedOperationException("path of no file")
  }
}

sealed trait JarAsset extends FileAsset {
  def resolve(relativePath: String): FileInJarAsset = FileInJarAsset(this, RelativePath(relativePath))
}

sealed trait BaseHttpAsset extends FileAsset {
  type LocalAsset <: FileAsset
  def asLocal(localPath: Path): LocalAsset
  def url: URL = throw new UnsupportedOperationException("Should not be invoked here.")
  def validate(url: URL): Boolean = url.getProtocol == "http" || url.getProtocol == "https"
  def isJar: Boolean = validate(url) && name.substring(name.lastIndexOf(".") + 1) == "jar"
  override def path: Path =
    throw new UnsupportedOperationException(s"You must depcopy a HttpAsset in order to get a path for it: $url")
  override def pathString: String = url.toString
  override def uriString: String = url.toString
  override def pathFingerprint: String = url.toString
  override def name: String = url.toString.substring(url.toString.lastIndexOf("/") + 1)
  override def lastModified: Instant = Instant.EPOCH
}

object BaseHttpAsset {
  def isJar[T <: URL, String](url: T): Boolean = url.toString.substring(url.toString.lastIndexOf(".") + 1) == "jar"
  def httpAsset[T <: URL, String](url: T): BaseHttpAsset =
    if (BaseHttpAsset.isJar(url)) JarHttpAsset(url) else HttpAsset(url)
}

final case class HttpAsset private[files] (override val url: URL) extends BaseHttpAsset {
  override type LocalAsset = FileAsset
  override def asLocal(localPath: Path): FileAsset = FileAsset(localPath)
  override protected def validate(): Unit = require(validate(url))
}

object HttpAsset {
  def apply(s: String): HttpAsset = new HttpAsset(new URL(s))
}

final case class JarHttpAsset private[files] (override val url: URL) extends BaseHttpAsset with JarAsset {
  override type LocalAsset = JarAsset
  override def asLocal(localPath: Path): JarAsset = JarAsset(localPath)
  override protected def validate(): Unit = require(isJar)
}

object JarHttpAsset {
  def apply(s: String): JarHttpAsset = new JarHttpAsset(new URL(s))
}

final case class JarFileSystemAsset private[files] (path: Path, lastModified: Instant) extends JarAsset

object JarAsset {
  def apply(file: Path): JarAsset = JarFileSystemAsset(file, Instant.EPOCH)
  def apply(url: URL): JarAsset = JarHttpAsset(url)
  def timestamped(file: Path): JarAsset = JarFileSystemAsset(file, FileAsset.lastModified(file))
}

final case class FileInJarAsset private (jar: JarAsset, file: RelativePath, lastModified: Instant) extends FileAsset {
  override def path: Path = Paths.get(s"${jar.pathString}!${file.path}")
}
object FileInJarAsset {
  def apply(jar: JarAsset, file: RelativePath): FileInJarAsset = FileInJarAsset(jar, file, Instant.EPOCH)
  def apply(jar: JarAsset, file: String): FileInJarAsset = FileInJarAsset(jar, RelativePath(file), Instant.EPOCH)

  def parse(fs: FileSystem, file: String): Option[FileInJarAsset] = {
    val sepIndex = file.indexOf('!')
    if (sepIndex < 0) {
      None
    } else {
      val (jar, fileInJar) = file.splitAt(sepIndex)
      Some(FileInJarAsset(JarAsset(fs.getPath(jar)), fileInJar.substring(1)))
    }
  }
}

final case class JsonAsset private[files] (path: Path, lastModified: Instant) extends FileAsset {
  def storeJson[A: JsonFormat](a: A, replace: Boolean, zip: Boolean = true): Unit =
    AssetUtils.storeJsonAtomically(this, a, replaceIfExists = replace, zip = zip)
}
object JsonAsset {
  def apply(file: Path): JsonAsset = JsonAsset(file, Instant.EPOCH)
  def timestamped(file: Path): JsonAsset = JsonAsset(file, FileAsset.lastModified(file))
}

// Refers to a class or signature in the JDK platform. We assume such assets are immutable since a change of JDK release
// causes a full recompilation of everything anyway. Strictly speaking, the path isn't a real path here (it's of the
// form //modules/java.base/java/lang/Object.class, or //87/java.base/java/lang/Object.sig when using ct.sym)
final case class JdkPlatformAsset(path: Path) extends FileAsset {
  override def lastModified: Instant = Instant.EPOCH
}

trait DirectoryAsset extends Asset
