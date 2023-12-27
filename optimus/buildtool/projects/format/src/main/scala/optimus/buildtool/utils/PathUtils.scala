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
package optimus.buildtool.utils

import java.io.IOException
import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.FileVisitResult
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Asset
import org.slf4j.LoggerFactory.getLogger

import java.nio.file.FileSystems
import java.util.regex.Matcher
import java.util.regex.Pattern

private[optimus] final case class WorkspaceIndependentPath(pathString: String, absolute: Boolean)

/**
 * Utility methods for dealing with paths
 */
object PathUtils {
  val FileProtocol = "file://"
  private val LocalWindowPath = """/([A-Za-z]:.+)""".r

  // Windows can't handle bare "//foo" or "//foo/" paths (since it's expecting "//host/share"), so if we
  // get one of these then convert it to "/foo". Note that this will mean it's strictly a relative rather than
  // absolute path, hence the custom `isAbsolute` method below.
  def get(fs: FileSystem, path: String): Path =
    if (path.startsWith("//") && (path.count(_ == '/') == 2 || (path.endsWith("/") && path.count(_ == '/') == 3)))
      fs.getPath(path.substring(1))
    else
      fs.getPath(path)

  def get(path: String): Path = get(FileSystems.getDefault, path)

  def isAbsolute(p: Path): Boolean =
    p.isAbsolute || p.getRoot != null

  def isDisted(asset: Asset): Boolean = isDisted(asset.pathString)
  def isDisted(path: Path): Boolean = isDisted(PathUtils.platformIndependentString(path))
  def isDisted(platformIndependentString: String): Boolean =
    platformIndependentString.startsWith(NamingConventions.AfsDistStr)

  /**
   * Converts a path to a string representation which will parse back to an equivalent path on Linux or Windows (with
   * the exception that local Windows paths like C:\foo won't work on Linux, but these are preserved in a form which
   * still works on Windows)
   */
  def platformIndependentString(path: Path): String = platformIndependentString(path.toString)

  def platformIndependentString(path: String): String = {
    // convert \ to / (which works in Java APIs on both Linux and Windows)
    val str = path.replace('\\', '/')
    // if path starts with /, convert to // (since this works as a UNC path on windows and is the same as / on Linux)
    if (str.length > 1 && str.charAt(0) == '/' && str.charAt(1) != '/') "/" + str
    else str
  }

  def workspaceIndependentPath(path: Path): WorkspaceIndependentPath = {
    val normalizedPathStr = PathUtils.platformIndependentString(path)
    normalizedPathStr match {
      case s if isDisted(s) =>
        WorkspaceIndependentPath(s, absolute = true)
      case NamingConventions.DepCopyDistRoot(mprPath) =>
        // we've got a path to an external dependency
        WorkspaceIndependentPath(s"${NamingConventions.AfsDistStr}$mprPath", absolute = true)
      case NamingConventions.MsjavaCopyRoot(prPath) =>
        // we've got a path to a specially-treated msjava dependency
        WorkspaceIndependentPath(s"${NamingConventions.AfsDistStr}msjava/PROJ/$prPath", absolute = true)
      case NamingConventions.OssjavaCopyRoot(prPath) =>
        // we've got a path to a specially-treated ossjava dependency
        WorkspaceIndependentPath(s"${NamingConventions.AfsDistStr}ossjava/PROJ/$prPath", absolute = true)
      case s =>
        WorkspaceIndependentPath(s, path.isAbsolute)
    }
  }

  def uriToPath(uriString: String, fileSystem: FileSystem): Path = new URI(uriString).getPath match {
    case LocalWindowPath(path) => fileSystem.getPath(path)
    case path                  => fileSystem.getPath(path)
  }

  // Note that we don't use JarAsset here, since the path may be relative
  def uriString(path: Path): String = uriString(platformIndependentString(path), path.isAbsolute)

  def uriString(path: String, absolute: Boolean): String = {
    // the convention (at least the behavior of Gradle) seems to be that absolute paths should be URLs (file:////some/afs/path)
    // and relative paths should be plain paths. Note that java.net.URL seems to do the wrong thing so we're not using it.
    if (absolute) {
      // special handling for local absolute paths on Windows (which begin with a drive letter not a slash) -
      // the JV seems to expect three slashes not two
      if (path.charAt(0).isLetter) s"$FileProtocol/$path"
      else FileProtocol + path
    } else path
  }

  def fileName(pathString: String): String = {
    val idx = pathString.lastIndexOf('/')
    if (idx >= 0) pathString.substring(idx + 1)
    else pathString
  }

  def pathFingerprint(path: Path, localPrefix: Option[Path] = None): String = {
    val pathStr = workspaceIndependentPath(path).pathString
    // Note - the String produced here will be workspace independent but may not be a valid path, since in
    // some cases (eg. NagJ) paths containing "/common/lib/" don't have a "/lib/" equivalent. If you need
    // a valid path, then use `workspaceIndependentPath` instead.
    // normalize common/lib paths, .exec paths
    val normalizedPathStr = pathStr.replace("/common/lib/", "/lib/").replace("/.exec/@sys/", "/exec/")

    if (isDisted(normalizedPathStr)) {
      normalizedPathStr
    } else {
      val trimmed = localPrefix.map(PathUtils.platformIndependentString) match {
        case Some(wp) if normalizedPathStr.startsWith(wp) => normalizedPathStr.substring(wp.length)
        case None                                         => normalizedPathStr
        case Some(wp) => throw new IllegalArgumentException(s"Unexpected path $path (expected prefix: $wp)")
      }
      trimmed
    }
  }

  def mappedPathString(a: Asset): String = mappedPathString(a, OsUtils.isWindows)

  // Note: We use `path.toString` below to get the platform-native representation
  private[utils] def mappedPathString(a: Asset, isWindows: Boolean): String =
    if (isWindows && isDisted(a))
      a.path.toString
        .replaceFirst(
          Pattern.quote(NamingConventions.AfsRootStrWindows),
          Matcher.quoteReplacement(NamingConventions.AfsRootMappingWindows)
        )
    else
      a.path.toString

  def fingerprintElement(tpe: String, id: String, contentHash: String, prefix: String = ""): String = {
    val tpeStr = if (tpe.isEmpty) "" else s"[$tpe]"
    val prefixStr = if (prefix.isEmpty) "" else s"$prefix:"
    s"$tpeStr$prefixStr$id@$contentHash"
  }
  def fingerprintElement(id: String, contentHash: String): String =
    fingerprintElement("", id, contentHash)

  def fingerprintAsset(tpe: String, a: Asset, strict: Boolean = true): String =
    fingerprintElement(tpe, a.pathFingerprint, Hashing.hashFileOrDirectoryContent(a, strict = strict))

  abstract class ErrorIgnoringFileVisitor extends SimpleFileVisitor[Path] {

    /**
     * There is basically nothing we can do about files disappearing while we are scanning, and given that they are no
     * longer there we probably don't need to do anything about them
     */
    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
      FileVisitResult.CONTINUE
    }
  }

  object ErrorIgnoringFileVisitor {
    private val log = getLogger(getClass)
  }

}
