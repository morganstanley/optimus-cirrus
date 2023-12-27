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

import optimus.buildtool.config

import java.nio.file.{Files, Path}
import optimus.buildtool.config.{MetaBundle, NamingConventions, ScopeId}
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.PathUtils
import org.slf4j.LoggerFactory.getLogger

import scala.collection.compat._
import scala.collection.immutable.Seq

/* Create with factory methods in companion object please */
sealed abstract class InstallPathBuilder(
    override val installVersion: String,
    override protected val distDir: Directory
) extends CommonDirs {

  type Path = Directory

  /** If `installDir` is on AFS, the AFS path for `installVersion`, otherwise the NFS path based on it. */
  def primaryInstallDir(metaBundle: MetaBundle, leaf: String = "", branch: String = NamingConventions.Common): Directory

  // for maven upload
  final def mavenDir(scopeId: ScopeId): Directory =
    dirForScope(scopeId)
      .resolveDir(scopeId.meta)
      .resolveDir(scopeId.bundle)
      .resolveDir(scopeId.module)
      .resolveDir(installVersion)

  def getMavenPath(classJar: JarAsset, disted: Boolean = false): Option[JarAsset] = classJar.pathString match {
    case NamingConventions.DepCopyMavenRoot(relativePath) =>
      val afReleaseMetaBundle =
        MetaBundle(config.NamingConventions.MavenDepsCentralMeta, config.NamingConventions.MavenDepsCentralBundle)
      // put into sharable MetaBundle dir for AFS release
      if (disted) Some(dirForDist(afReleaseMetaBundle).resolveDir("lib").resolveJar(relativePath))
      else Some(libDir(afReleaseMetaBundle).resolveJar(relativePath))
    case _ => None
  }

  def mavenDependencyPaths(
      classpath: Seq[JarAsset],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)]
  ): Seq[(JarAsset, JarAsset)] = {
    // only copy maven files
    classpath
      .filterNot(jar =>
        installJarMapping.contains(jar) && jar.pathString.contains(NamingConventions.MavenDepsCentralBundle))
      .flatMap { dependencyJar =>
        getMavenPath(dependencyJar).map { j =>
          (dependencyJar, j)
        }
      }
      .distinct
  }

  def locationIndependentClasspath(
      scopeId: ScopeId,
      classpath: Seq[JarAsset],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)],
      includeRelativePaths: Boolean = true
  ): Seq[String] = {
    def isLocalOrInSameDir(localPath: RelativePath) =
      installVersion == NamingConventions.LocalVersion || localPath.parentOption.contains(RelativePath.empty)

    val pathingDir = libDir(scopeId)
    classpath.flatMap { classJar =>
      // Remap jars we have installed to their installed paths. Note that we reference them by relative paths so
      // that the whole installation is location independent
      installJarMapping.get(classJar) match {
        case Some((jarScopeId, installJar)) =>
          // we've got a path to a jar from another scope in the workspace
          lazy val localPath = pathingDir.relativize(installJar)
          def distedPath: JarAsset =
            dirForDist(jarScopeId.metaBundle).resolveDir("lib").resolveJar(installJar.name)
          val paths =
            if (includeRelativePaths) {
              if (isLocalOrInSameDir(localPath))
                // no need to add a disted path when doing a local install or if it's in the same directory as the
                // pathing jar
                Seq(localPath)
              else
                // local path should always be before the disted one
                Seq(localPath, distedPath)
            } else
              // specifically requested no relative paths (because this installation will never be used on NFS-style builds)
              Seq(distedPath)

          paths.map(_.uriString)
        case None =>
          val localMavenPath = getMavenPath(classJar).map(pathingDir.relativize(_))
          // should only be called when localMavenPath exist
          def distedMavenPath: JarAsset = getMavenPath(classJar, disted = true).get

          localMavenPath match {
            case Some(localPath) =>
              val paths = if (includeRelativePaths) {
                if (isLocalOrInSameDir(localPath))
                  Seq(localPath)
                else Seq(localPath, distedMavenPath)
              } else Seq(distedMavenPath)
              paths.map(_.uriString)
            case None =>
              val wip = PathUtils.workspaceIndependentPath(classJar.path)
              Seq(PathUtils.uriString(wip.pathString, wip.absolute))
          }
      }
    }.distinct
  }

}

class RelativeInstallPathBuilder(override protected val installVersion: String) extends CommonDirs {
  type Path = RelativePath

  override protected val distDir: Directory = NamingConventions.AfsDist

  override def dirForMetaBundle(
      metaBundle: MetaBundle,
      leaf: String,
      branch: String
  ): RelativePath = InstallPathBuilder.pathForMetaBundle(metaBundle, installVersion, leaf, branch)

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
      metaBundle: MetaBundle,
      leaf: String = "",
      branch: String = ""
  ): Directory = {
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

  /** Builder with layout for NFS style builds. This includes normal releases but not Docker. */
  def dev(installDir: Directory, installVersion: String): InstallPathBuilder =
    new DevInstallPathBuilder(installDir, installVersion)

  /** Builder with layout for maven (i.e. /packages/maven) style builds. */
  def mavenRelease(installDir: Directory, installVersion: String): InstallPathBuilder =
    new MavenInstallPathBuilder(installDir, installVersion)

  /** This one infers the version. */
  def runtime(installDir: Directory): InstallPathBuilder = {
    val root = inferInstallRoot(installDir)
    val version = inferVersion(root, installDir)
    log.info(s"Inferred root '$root' and version '$version' from $installDir for the installation.")
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

  def inferVersion(installRoot: Directory, installDir: Directory): String = {
    // Not using NamingConventions.MsDist here since it prevents testing on different filesystems
    val msDist = Directory(installDir.fileSystem.getPath(NamingConventions.AfsDistStr))
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
  def inferInstallRoot(installLocation: Directory): Directory = {
    def getParts(upToIndex: Int): Seq[Path] =
      (0 until upToIndex.min(installLocation.path.getNameCount)).map(installLocation.path.getName)
    def buildPath(parts: Seq[Path]): Path = {
      val root = installLocation.path.getRoot
      log.info(s"Building path from root $root with (${parts.map(_.toString).mkString(", ")})")
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
    if (pathToInstallCommon.startsWith(NamingConventions.AfsDistStr) && parts.size >= thisOSPartsForAfsRoot) {
      Directory(buildPath(getParts(thisOSPartsForAfsRoot)))
    } else if (
      pathToInstallCommon.endsWith(NamingConventions.InstallPattern) &&
      parts.size > NamingConventions.InstallPathComponents
    ) {
      Directory(buildPath(getParts(pathToInstallCommon.getNameCount - NamingConventions.InstallPathComponents)))
    } else {
      log.debug(
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
