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

import com.google.common.hash.Hashing
import optimus.buildtool.config
import optimus.buildtool.config.AfsNamingConventions
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.StackUtils
import org.slf4j.LoggerFactory.getLogger

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import scala.collection.compat._
import scala.util.Properties
import scala.util.control.NonFatal

/* Create with factory methods in companion object please */
sealed abstract class InstallPathBuilder(
    override val installVersion: String,
    override protected val distDir: Directory
) extends CommonDirs {

  type Path = Directory

  val mavenReleaseMetaBundle: MetaBundle =
    MetaBundle(config.NamingConventions.MavenDepsCentralMeta, config.NamingConventions.MavenDepsCentralBundle)

  /** If `installDir` is on AFS, the AFS path for `installVersion`, otherwise the NFS path based on it. */
  def primaryInstallDir(metaBundle: MetaBundle, leaf: String = "", branch: String = NamingConventions.Common): Directory

  // for maven upload
  final def mavenDir(scopeId: ScopeId): Directory =
    dirForScope(scopeId)
      .resolveDir(scopeId.meta)
      .resolveDir(scopeId.bundle)
      .resolveDir(scopeId.module)
      .resolveDir(installVersion)

  def getInArtifactoryDepsPath(classJar: JarAsset, disted: Boolean = false): Option[BundledMavenJarAsset] =
    classJar.pathString match {
      case NamingConventions.DepCopyMavenRoot(_, relativePath) =>
        // put into sharable MetaBundle dir for AFS release
        if (disted)
          Some(BundledMavenJarAsset(dirForDist(mavenReleaseMetaBundle).resolveDir("lib").resolveJar(relativePath)))
        else Some(BundledMavenJarAsset(libDir(mavenReleaseMetaBundle).resolveJar(relativePath)))
      case _ => None
    }

  def pathForTpa(scopeId: ScopeId): FileAsset = etcDir(scopeId).resolveFile(s"${scopeId.module}.tpa")
  def wheelsDir(): Directory = etcDir(MetaBundle("optimus", "dependencies")).resolveDir("wheels")
}

class RelativeInstallPathBuilder(override protected val installVersion: String) extends CommonDirs {
  type Path = RelativePath

  override protected val distDir: Directory = NamingConventions.AfsDist

  override def dirForMetaBundle(
      metaBundle: MetaBundle,
      leaf: String,
      branch: String
  ): RelativePath = InstallPathBuilder.pathForMetaBundle(metaBundle, installVersion, leaf, branch)

  def locationIndependentJar(scopeId: ScopeId, jar: JarAsset): Seq[String] = {
    val mavenReleaseMetaBundle: MetaBundle =
      MetaBundle(config.NamingConventions.MavenDepsCentralMeta, config.NamingConventions.MavenDepsCentralBundle)

    val scopeBinPath = binDir(scopeId)
    val mavenLibDir = libDir(mavenReleaseMetaBundle)
    val pathFromBinToMavenLib = scopeBinPath.relativize(mavenLibDir)

    val jarPath = jar.pathString match {
      case NamingConventions.DepCopyMavenRoot(_, relativePath) =>
        // put into sharable MetaBundle dir for AFS release
        val localPath = s"$pathFromBinToMavenLib/$relativePath"
        if (installVersion == NamingConventions.LocalVersion) {
          Seq(localPath)
        } else {
          val distedPath = dirForDist(mavenReleaseMetaBundle).resolveDir("lib").resolveJar(relativePath).pathString
          Seq(localPath, distedPath)
        }
      case _ => Seq.empty
    }
    jarPath
  }

  def locationIndependentJar(scopeId: ScopeId, targetScopeId: ScopeId, jarName: String): Seq[String] = {
    val scopeBinPath = binDir(scopeId)
    val localPath =
      scopeBinPath.relativize(dirForMetaBundle(targetScopeId.metaBundle, leaf = "lib"))
    val jarLocalPath = s"${localPath.pathString}/$jarName"

    if (installVersion == NamingConventions.LocalVersion || targetScopeId.metaBundle == scopeId.metaBundle) {
      // no need to add a disted path when doing a local install or if it's in the same metabundle as the script
      Seq(jarLocalPath)
    } else {
      // two paths needed here - one for an install locally (or to NFS) and one for AFS
      val distedPath = dirForDist(targetScopeId.metaBundle, leaf = "lib").pathString
      val jarDistedPath = s"$distedPath/$jarName"
      // this order is needed [SEE_AGENT_PATHS]
      Seq(jarLocalPath, jarDistedPath)
    }
  }

  def locationIndependentNativePath(
      scopeId: ScopeId,
      nativeScopeDeps: Seq[ScopeId],
      dirVariable: String,
      exec: Option[String],
      fileName: Option[ScopeId => String] = None
  ): Seq[String] = {
    val scopeBinPath = binDir(scopeId)
    nativeScopeDeps.flatMap { s =>
      val execPath = exec.map(e => s".exec/$e").getOrElse("exec")
      val localPath =
        scopeBinPath.relativize(dirForMetaBundle(s.metaBundle, branch = execPath, leaf = "lib"))
      // Note that when resolved, we expect `dirVariable` to include a trailing `/`
      val localPathStr = s"$dirVariable${localPath.pathString}"
      val dirs = if (installVersion == NamingConventions.LocalVersion || s.metaBundle == scopeId.metaBundle) {
        // no need to add a disted path when doing a local install or if it's in the same metabundle as the script
        Seq(localPathStr)
      } else {
        // two paths needed here - one for an install locally (or to NFS) and one for AFS
        val distedPath = dirForDist(s.metaBundle, branch = "exec", leaf = "lib")
        Seq(localPathStr, distedPath.pathString)
      }

      fileName match {
        case Some(f) => dirs.map(d => s"$d/${f(s)}")
        case None    => dirs
      }
    }.distinct
  }
}

sealed trait CommonDirs {

  type Path <: Pathed

  protected val distDir: Directory
  protected val installVersion: String

  def dirForScope(scopeId: ScopeId, leaf: String = "", branch: String = NamingConventions.Common): Path =
    dirForMetaBundle(scopeId.metaBundle, leaf, branch)
  def dirForMetaBundle(metaBundle: MetaBundle, leaf: String = "", branch: String = NamingConventions.Common): Path

  final def binDir(scopeId: ScopeId): Path = dirForScope(scopeId, "bin")
  final def binDir(metaBundle: MetaBundle): Path = dirForMetaBundle(metaBundle, "bin")
  final def etcDir(scopeId: ScopeId): Path = dirForScope(scopeId, "etc")
  final def etcDir(metaBundle: MetaBundle): Path = dirForMetaBundle(metaBundle, "etc")
  final def libDir(scopeId: ScopeId): Path = dirForScope(scopeId, "lib")
  final def libDir(metaBundle: MetaBundle): Path = dirForMetaBundle(metaBundle, "lib")

  final def dirForDist(
      metaBundle: MetaBundle,
      leaf: String = "",
      branch: String = NamingConventions.Common
  ): Directory = {
    require(
      installVersion != NamingConventions.LocalVersion,
      s"dirForDist doesn't make sense for non-release builds: $metaBundle"
    )
    val result = distDir
      .resolveDir(metaBundle.meta)
      .resolveDir("PROJ")
      .resolveDir(metaBundle.bundle)
      .resolveDir(installVersion)
      .resolveDir(branch)
    if (leaf.nonEmpty) result.resolveDir(leaf) else result
  }

}

private final class DevInstallPathBuilder(installDir: Directory, installVersion: String)
    extends InstallPathBuilder(installVersion, NamingConventions.AfsDist) {
  def dirForMetaBundle(
      metaBundle: MetaBundle,
      leaf: String = "",
      branch: String = NamingConventions.Common
  ): Directory = {
    val path = InstallPathBuilder.pathForMetaBundle(metaBundle, installVersion, leaf, branch)
    installDir.resolveDir(path)
  }

  /** If `installDir` is on AFS, the AFS path for `installVersion`, otherwise the NFS path based on it. */
  def primaryInstallDir(
      metaBundle: MetaBundle,
      leaf: String = "",
      branch: String = NamingConventions.Common
  ): Directory = {
    if (installDir isChildOf NamingConventions.AfsDist) dirForDist(metaBundle, leaf, branch)
    else dirForMetaBundle(metaBundle, leaf, branch)
  }
}

private final class MavenInstallPathBuilder(installDir: Directory, installVersion: String)
    extends InstallPathBuilder(installVersion, NamingConventions.AfsDist) {

  def dirForMetaBundle(
      rawMetaBundle: MetaBundle,
      leaf: String = "",
      branch: String = ""
  ): Directory = {
    val metaBundle = rawMetaBundle.forMavenRelease()
    if (!metaBundle.isEmpty)
      installDir
        .resolveDir(
          NamingConventions.MavenCIScope
        ) // This is required for CI job upload, suitable for release metadata & Strato.
        .resolveDir(installVersion)
        .resolveDir(NamingConventions.MavenUploadDirectory)
    else throw new IllegalArgumentException(s"Can't install maven files for empty metaBundle.")
  }

  def primaryInstallDir(
      metaBundle: MetaBundle,
      leaf: String = "",
      branch: String = ""
  ): Directory = dirForMetaBundle(metaBundle)
}

private final class DockerInstallPathBuilder(installDir: Directory, installVersion: String)
    extends InstallPathBuilder(installVersion, NamingConventions.AfsDist) {

  override def primaryInstallDir(metaBundle: MetaBundle, leaf: String, branch: String): Directory =
    dirForMetaBundle(metaBundle, leaf, branch)

  override def dirForMetaBundle(metaBundle: MetaBundle, leaf: String = "", branch: String = ""): Directory = {
    if (!metaBundle.isEmpty) {
      val result = installDir
        .resolveDir(NamingConventions.DockerPackage)
        .resolveDir(metaBundle.meta)
        .resolveDir(metaBundle.bundle)
        .resolveDir(installVersion)
      if (leaf.nonEmpty) result.resolveDir(leaf) else result
    } else {
      throw new IllegalArgumentException("Can't package docker files for empty metaBundle.")
    }
  }
}

private final class DistInstallPathBuilder(installVersion: String, distDir: Directory = NamingConventions.AfsDist)
    extends InstallPathBuilder(installVersion, distDir) {

  override def dirForMetaBundle(
      metaBundle: MetaBundle,
      leaf: String,
      branch: String = NamingConventions.Common
  ): Directory =
    dirForDist(metaBundle, leaf, branch)
  override def primaryInstallDir(
      metaBundle: MetaBundle,
      leaf: String,
      branch: String = NamingConventions.Common
  ): Directory =
    dirForDist(metaBundle, leaf, branch)
}

object InstallPathBuilder {
  private val log = getLogger(this.getClass)
  private val sha256 = Hashing.sha256

  def getCachedMavenPath(
      classJar: JarAsset,
      cacheRootPath: Path,
      cachePrefix: String,
      baseArtifactDir: Option[RelativePath],
      releaseRevisions: Map[String, Char]): Option[Path] =
    classJar.pathString match {
      case NamingConventions.DepCopyMavenRoot(_, relativePath) =>
        val hash = sha256.hashString(relativePath, Charset.forName("UTF-8")).toString.take(31)
        val shard = hash.head
        val version = releaseRevisions.getOrElse(relativePath, '0')
        val cacheBucketPath = cacheRootPath.resolve(s"$cachePrefix$shard")
        val hashWithVersion = s"$hash$version"
        val pathToHashedDir = cacheBucketPath.resolve(hashWithVersion)
        val baseCacheDir = baseArtifactDir
          .map { baseDir =>
            pathToHashedDir.resolve(baseDir.path)
          }
          .getOrElse(pathToHashedDir)

        Some(baseCacheDir.resolve(relativePath))
      case _ => None
    }

  /** Builder with layout for NFS style builds. This includes normal releases but not Docker. */
  def dev(installDir: Directory, installVersion: String): InstallPathBuilder =
    new DevInstallPathBuilder(installDir, installVersion)

  /** Builder with layout for docker (i.e. /packages/docker) style builds. */
  def dockerRelease(installDir: Directory, installVersion: String): InstallPathBuilder =
    new DockerInstallPathBuilder(installDir, installVersion)

  /** Builder with layout for maven (i.e. /packages/maven) style builds. */
  def mavenRelease(installDir: Directory, installVersion: String): InstallPathBuilder =
    new MavenInstallPathBuilder(installDir, installVersion)

  /** This one infers the version if none is provided. */
  def runtime(installDir: Directory, maybeInstallVersion: Option[String]): InstallPathBuilder = {
    // We want to resolve symlinks defined in the context of Continuous Integration and quality assurance
    // (i.e. latest build, current QA release, etc.
    def resolveSymLinkButNotMountPoint(resolved: Directory, original: Directory): (Boolean, Directory) = {
      val resolvedPath = resolved.path
      val originalPath = original.path
      val differentPaths = resolvedPath != originalPath
      val symLinkResolved =
        if (Properties.isLinux) {
          // We want a difference, so long as the roots are the same, otherwise we deem it a mount point, which
          // cannot be passed on to other nodes in a cluster (e.g. computation grid)
          differentPaths && resolvedPath.getNameCount > 0 && originalPath.getNameCount > 0 && resolvedPath
            .getName(0) == originalPath.getName(0)
        } else {
          differentPaths
        }
      (symLinkResolved, if (symLinkResolved) resolved else original)
    }

    val (symLinkResolved, realInstallDir) =
      resolveSymLinkButNotMountPoint(
        try {
          Directory(installDir.path.toRealPath())
        } catch {
          case NonFatal(e) =>
            log.warn(s"Unable find real path for $installDir: ${StackUtils.multiLineStacktrace(e)}")
            installDir
        },
        original = installDir
      )
    if (symLinkResolved)
      log.info(s"Symlink path found: $installDir -> $realInstallDir")
    val root = inferInstallRoot(realInstallDir)
    val version = maybeInstallVersion.getOrElse(inferVersion(root, realInstallDir))
    log.info(s"Inferred root '$root' and version '$version' from $realInstallDir for the installation.")
    if (root.isChildOf(NamingConventions.AfsDist))
      dist(version)
    else
      dev(root, version)
  }

  /**
   * Builder that always lays out files as if on AFS. To be used with Docker mostly (you can't write to AFS here).
   */
  def dist(installVersion: String): InstallPathBuilder =
    new DistInstallPathBuilder(installVersion)

  def staging(installVersion: String, stagingDir: Directory): InstallPathBuilder =
    new DistInstallPathBuilder(installVersion, stagingDir)

  private[files] def inferVersion(installRoot: Directory, installDir: Directory): String = {
    // Not using NamingConventions.MsDist here since it prevents testing on different filesystems
    val msDist = Directory(installDir.fileSystem.getPath(AfsNamingConventions.AfsDistStr))
    if (installDir isChildOf msDist) {
      try msDist.relativize(installDir).path.getName(3).toString
      catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Directory ${installDir.path} does not look like an AFS-style path (quoth NIO: ${e.getMessage})"
          )
      }
    } else if (installDir.path.endsWith(NamingConventions.InstallPattern)) {
      installDir.parent.parent.name
    } else {
      import scala.jdk.CollectionConverters._
      // Get all the directories that hopefully match the pattern installDir/someMeta/someBundle/<version>/install
      val mbInstallDirs =
        Files
          .find(
            installRoot.path,
            4,
            (p, attrs) => p != installRoot.path && attrs.isDirectory && p.getFileName.toString == "install"
          )
          .iterator
          .asScala

      val versions = mbInstallDirs.map(_.getParent.getFileName.toString).to(Seq).distinct

      versions match {
        case Seq(version) => version
        case _ =>
          val default = NamingConventions.LocalVersion
          log.warn(s"Couldn't infer a version for $installDir (root: $installRoot); assuming $default")
          default
      }
    }
  }

  private val linuxPartsForAfsRoot: Int = 6 // //afs/base is three parts, namely root (/), afs, path
  private val windowsPartsForAfsRoot: Int = 4 // //afs/base is one part on Windows OS
  private val thisOSPartsForAfsRoot: Int = if (OsUtils.isWindows) windowsPartsForAfsRoot else linuxPartsForAfsRoot
  private[files] def inferInstallRoot(installLocation: Directory): Directory = {
    def getParts(upToIndex: Int): Seq[Path] =
      (0 until upToIndex.min(installLocation.path.getNameCount)).map(installLocation.path.getName)
    def buildPath(parts: Seq[Path]): Path = {
      val root = installLocation.path.getRoot
      log.debug(s"Building path from root $root with (${parts.map(_.toString).mkString(", ")})")
      parts.foldLeft(root) { case (acc, name) => acc.resolve(name) }
    }

    val parts = getParts(installLocation.path.getNameCount)
    val pathToInstallCommon = parts.foldLeft(installLocation.path.getRoot) {
      case (acc, _) if acc.endsWith(NamingConventions.InstallPattern) => acc // Sweet spot, skipping subfolders
      case (acc, name)                                                => acc.resolve(name)
    }
    log.debug(
      s"We decomposed $installLocation into [${parts.mkString(", ")}] from ${installLocation.path.getRoot} and reassembled as $pathToInstallCommon"
    )
    if (pathToInstallCommon.startsWith(AfsNamingConventions.AfsDistStr) && parts.size >= thisOSPartsForAfsRoot) {
      Directory(buildPath(getParts(thisOSPartsForAfsRoot)))
    } else if (
      pathToInstallCommon.endsWith(NamingConventions.InstallPattern) &&
      parts.size > NamingConventions.InstallPathComponents
    ) {
      Directory(buildPath(getParts(pathToInstallCommon.getNameCount - NamingConventions.InstallPathComponents)))
    } else {
      log.warn(
        s"Not a known case (i.e. /afs/path/to/meta/PROJ/project/release, NFS build artifacts or workspace's install/): using $installLocation"
      )
      installLocation
    }
  }

  private[files] def pathForMetaBundle(
      metaBundle: MetaBundle,
      installVersion: String,
      leaf: String,
      branch: String
  ): RelativePath = {
    val path =
      if (!metaBundle.isEmpty)
        RelativePath(metaBundle.meta)
          .resolvePath(metaBundle.bundle)
          .resolvePath(installVersion)
          .resolvePath("install")
          .resolvePath(branch)
      else RelativePath.empty
    if (leaf.nonEmpty) path.resolvePath(leaf) else path
  }

}
