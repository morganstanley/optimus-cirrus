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

import optimus.platform._
import org.slf4j.LoggerFactory.getLogger

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.math.Ordering.Implicits._
import scala.sys.process.Process
import scala.sys.process.ProcessLogger
import scala.util.Try
import scala.util.matching.Regex

@entity
object OsUtils {
  private val log = getLogger(this.getClass)

  val AutoResolvingNativeLibsForOsFolder: String = "exec"
  val HiddenNativeLibOsVariantsFolder: String = ".exec"
  val AutoResolveAliasFolder: String = "@sys"

  val Linux6Version = "linux-el6.x86_64"
  val Linux7Version = "linux-el7.x86_64"
  val Linux8Version = "linux-el8_9.x86_64"
  val Linux8_10Version = "linux-el8_10.x86_64"
  val WindowsVersion = "windows-10.0"

  val Linux6SysName: String =
    OptimusBuildToolProperties.getOrElse("sysName.linux6", "x86_64.linux.2.6.glibc.2.12")
  val Linux7SysName: String =
    OptimusBuildToolProperties.getOrElse("sysName.linux7", "x86_64.linux.2.6.glibc.2.17")
  val Linux8SysName: String =
    OptimusBuildToolProperties.getOrElse("sysName.linux8", "x86_64.linux.2.6.glibc.2.28")
  val WindowsSysName: String =
    OptimusBuildToolProperties.getOrElse("sysName.windows", "x86_64.nt.win10")

  private val osVersionMapping = Map(
    Linux6Version -> Linux6SysName,
    Linux7Version -> Linux7SysName,
    Linux8Version -> Linux8SysName,
    Linux8_10Version -> Linux8SysName,
    WindowsVersion -> WindowsSysName
  )

  def isWindows: Boolean = osType == "windows"

  def isWindows(osVersion: String): Boolean = osVersion.split('-').head == "windows"

  def osType: String = sys.props("os.name").split(' ').head.toLowerCase
  def osType(osVersion: String): String = osVersion.split('-').head

  def osVersion: String =
    if (isWindows) s"$osType-${sys.props("os.version")}" // eg. "windows-10.0"
    else linuxOsVersion(osType, sys.props("os.version")) // eg. "linux-el7.x86_64", "linux-el6.x86_64"

  private[utils] def linuxOsVersion(tpe: String, version: String): String =
    s"$tpe-${version.split('.').takeRight(2).mkString(".")}"

  lazy val sysName: Seq[String] = {
    var n: Seq[String] = Nil
    Try {
      Process(Seq("fs", "sysname")) ! ProcessLogger { s =>
        n = readSysName(s)
      }
    }
    n
  }

  def exec: String = osVersionMapping.getOrElse(
    OsUtils.osVersion,
    throw new IllegalArgumentException(s"Unrecognized OS version: ${OsUtils.osVersion}"))

  def execFor(osVersion: String): String =
    osVersionMapping.getOrElse(osVersion, throw new IllegalArgumentException(s"Unrecognized OS version: $osVersion"))

  private val sysNameRegex = s"'([^']+)'".r
  private[utils] def readSysName(output: String): Seq[String] =
    sysNameRegex.findAllMatchIn(output).map(_.group(1)).to(Seq)

  // GLIBC helpers

  // We care only about 64 bits OS
  private val GlibcPattern: Regex = """.*x86_64\..*glibc\.(\d+)\.(\d+).*""".r

  def glibcVersion(folderName: String): GlibcVersion =
    folderName match {
      case GlibcPattern(major, minor) => GlibcVersion(major.toInt, minor.toInt)
      case _ => throw new IllegalArgumentException(s"Does not know how to tell GLIBC version from $folderName")
    }

  def glibcVersion(p: Path): (GlibcVersion, Path) =
    glibcVersion(p.getFileName.toString) -> p

  def hasGlibcVersion(p: Path): Boolean =
    GlibcPattern.pattern.matcher(p.toString).matches()

  // Path rewrite helpers

  def folderComponentIndex(p: Path, c: String): Option[Int] =
    (0 until p.getNameCount)
      .map(idx => idx -> p.getName(idx))
      .find { case (_, name) => name.toString == c }
      .map { case (idx, _) => idx }

  def hiddenNativeLibsFromFile(f: Path): Path =
    folderComponentIndex(f, OsUtils.HiddenNativeLibOsVariantsFolder)
      .map { idx =>
        val mappedTo = f.getRoot.resolve(f.subpath(0, idx + 1))
        mappedTo
      }
      .getOrElse(throw new IllegalStateException(s"Could not find hidden native libs folder for $f"))

  // We have caching to avoid repeated file system access on networked storage in particular
  private val pathToGlibcVersions = new ConcurrentHashMap[Path, Seq[GlibcPath]]()
  private[utils] def findSupportedGlibcVersion(hiddenNativeLibsLocation: Path): Seq[GlibcPath] = {
    require(Files.isDirectory(hiddenNativeLibsLocation), s"$hiddenNativeLibsLocation is not a directory")
    require(
      hiddenNativeLibsLocation.getFileName.toString == HiddenNativeLibOsVariantsFolder,
      s"$hiddenNativeLibsLocation is not a hidden native libs folder")
    pathToGlibcVersions.computeIfAbsent(
      hiddenNativeLibsLocation,
      { _ =>
        Files
          .list(hiddenNativeLibsLocation)
          .iterator()
          .asScala
          .toList
          .distinct
          .collect { case p if hasGlibcVersion(p) => glibcVersion(p) }
          .sortBy { case (version, _) => version }
          .reverse
          .map { case (version, path) => GlibcPath(path, version) }
      }
    )
  }

  def findNativeLibsForTargetOs(
      hiddenNativeLibsLocation: Path,
      desiredGlibcVersion: GlibcVersion
  ): Option[GlibcPath] = {
    // We got a choice of libraries here, we need to filter by version and select the one matching os version or lower
    // It happens that some very old library do not even have native libs for the target OS
    val glicVersions = findSupportedGlibcVersion(hiddenNativeLibsLocation)
    glicVersions
      .find { case GlibcPath(_, version) => version <= desiredGlibcVersion }
      .orElse {
        log.warn(
          s"No glibc version <= $desiredGlibcVersion listed in $hiddenNativeLibsLocation: the image for the target OS may be broken or it may be inconsequential (particularly very old libraries) (folder last modified on ${Files
              .getLastModifiedTime(hiddenNativeLibsLocation)})")
        None
      }
  }
}
