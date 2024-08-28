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
package optimus.config

import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.DiagnosticSettings
import optimus.graph.GCNative
import optimus.platform.utils.ClassPathUtils

import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Properties
import scala.util.matching.Regex

/**
 * a utility for resolving important resource paths e.g. GC Native library.
 */
object InstallPathLocator {
  private[this] val log = getLogger(getClass)

  private def libName(base: String) =
    if (DiagnosticSettings.useDebugCppAgent) s"$base-g" else base

  private val gcNativeName =
    if (System.getProperty("os.name").toLowerCase().contains("windows")) "gcnative" else libName("gcnative")
  private val pToolsName = libName("ptools")
  private val interceptorName = libName("fileinterceptor")

  lazy val system: Resolver = {
    new Resolver {
      override def disableGCNative: Boolean = GCNative.disableGCNative
      override def gcNativePathOverride: Option[String] = cppagent.gcNativePathOverride
      override def pToolsPathOverride: Option[String] = cppagent.pToolsPathOverride
      override def interceptorPathOverride: Option[String] = cppagent.interceptorPathOverride
      override def logStatus(): Unit = {
        log.info(s"=== Overridden paths ===")
        super.logStatus()
      }
    }
  }

  /**
   * the locator which only uses cppagent
   */
  val cppagent: Resolver = CppAgentResolver

  private[config] object CppAgentResolver extends Resolver {
    override def pToolsPathOverride: Option[String] =
      Option(System.getProperty(GCNative.PTOOLS_PATH_OVERRIDE_PROP)) orElse sys.env.get("PTOOLS_PATH")
    override def interceptorPathOverride: Option[String] =
      Option(System.getProperty(GCNative.INTERCEPTOR_PATH_OVERRIDE_PROP)) orElse sys.env.get("FILE_INTERCEPTOR_PATH")
    override def gcNativePathOverride: Option[String] =
      Option(System.getProperty(GCNative.GCNATIVE_PATH_OVERRIDE_PROP)) orElse sys.env.get("GCNATIVE_PATH")
  }

  trait Resolver {
    def gcNativePathOverride: Option[String] = None
    def disableGCNative: Boolean = false
    def pToolsPathOverride: Option[String] = None
    def interceptorPathOverride: Option[String] = None

    def logStatus(): Unit = {
      log.info(s"gcNativePathOverride = $gcNativePathOverride")
      log.info(s"disableGCNative = $disableGCNative")
      log.info(s"pToolsPathOverride = $pToolsPathOverride")
      log.info(s"interceptorPathOverride = $interceptorPathOverride")
    }

    final def loadGCNative(): Unit = loadLibrary(gcNativePathOverride, gcNativeName)
    final def loadPTools(): Unit = loadLibrary(pToolsPathOverride, pToolsName)
    final def loadInterceptor(): Unit = loadLibrary(interceptorPathOverride, interceptorName)

    private def loadLibrary(pathOverride: Option[String], libraryName: String): Unit = pathOverride match {
      case Some(path) =>
        log.info(s"Loading {} from path({})", libraryName, path)
        System.load(path)
      case None =>
        log.info(s"Loading {} at runtime", libraryName)
        System.loadLibrary(libraryName)
    }
  }

  def main(args: Array[String]): Unit = {
    system.logStatus()
  }

  private val BuildObtJar = """(.*)/build_obt/[^/]*/[^/]*/[^/]*\.jar""".r
  private def installJar(meta: String, bundle: String) =
    s"(.*)/$meta/$bundle/([^/]*)/install/common/lib/[^/]*\\.jar".r

  private val AfsInstallDir = // eg. `//a/b/meta/c/bundle/d
    """(//?[^/]*/[^/]*/)[^/]*(/[^/]*/)[^/]*(/[^/]*)""".r

  def inferredInstallDir: Path = inferInstallDir(utilsJarPath, "optimus", "platform")

  private[config] def inferInstallDir(jarPath: Path, meta: String, bundle: String): Path = {
    val installJar = this.installJar(meta, bundle)
    pathString(jarPath.normalize) match {
      case BuildObtJar(root)   => jarPath.getFileSystem.getPath(root).resolve("install")
      case installJar(root, _) => jarPath.getFileSystem.getPath(root)
      case _ => // eg. disted path
        val parent = jarPath.getParent
        if (parent.getFileName.toString == "lib") {
          val grandparent = parent.getParent
          if (grandparent.getFileName.toString == "common") grandparent.getParent // //a/b/common/lib/foo.jar => //a/b
          else grandparent // //a/b/lib/foo.jar => //a/b
        } else parent // //a/b/c/foo.jar => //a/b/c
    }
  }

  def inferredLocalInstallVersion: Option[String] = inferLocalInstallVersion(utilsJarPath, "optimus", "platform")

  private[config] def inferLocalInstallVersion(jarPath: Path, meta: String, bundle: String): Option[String] = {
    val installJar = this.installJar(meta, bundle)
    pathString(jarPath.normalize) match {
      case BuildObtJar(_)         => Some("local")
      case installJar(_, version) => Some(version)
      case _                      => None
    }
  }

  private lazy val utilsJarPath = enclosingJarPath(getClass)
  private def enclosingJarPath(clazz: Class[_]): Path =
    Paths.get(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)

  def allInstalledPaths(installDir: Path): Seq[Path] = {
    val propFile = installDir.resolve("common/etc/build.properties")
    if (Files.exists(propFile)) {
      val props = new Properties
      val is = Files.newInputStream(propFile)
      try props.load(is)
      finally is.close()

      val bundles = props.getProperty("bundles").split(',')
      allInstalledPaths(installDir, bundles)
    } else Seq(installDir)
  }

  private[config] def allInstalledPaths(installDir: Path, bundles: Seq[String]): Seq[Path] = {
    val matcher = AfsInstallDir.pattern.matcher(pathString(installDir.normalize))
    if (matcher.matches) {
      bundles.map { b =>
        val Array(meta, bundle) = b.split('.')
        Paths.get(matcher.replaceFirst(s"$$1$meta$$2$bundle$$3"))
      }
    } else Seq(installDir)
  }

  private val PathingJarSuffix = "-runtimeAppPathing.jar"
  def classLoader(
      installDirs: Seq[Path],
      localInstallVersion: Option[String],
      filters: Seq[JarFilter] = Nil,
      includeDependencies: Boolean = false,
      isolated: Boolean = true
  ): URLClassLoader = {
    def depFilter(jarName: String) = includeDependencies || !jarName.endsWith(PathingJarSuffix)

    val installedJars = installDirs.toArray.flatMap { d =>
      Files
        .find(
          d,
          Int.MaxValue,
          (p, _) => {
            val pathStr = pathString(p)
            pathStr.endsWith(".jar") && depFilter(pathStr) &&
            localInstallVersion.forall { v =>
              p.getParent.endsWith(s"$v/install/common/lib")
            } &&
            (filters.isEmpty || filters.exists(_.include(pathStr)))
          }
        )
        .toArray[Path](i => Array.ofDim(i))
    }

    log.info(s"Expanding classpath of ${installedJars.size} elements")
    val classPath: Seq[Path] =
      if (includeDependencies)
        ClassPathUtils
          .expandClasspath(installedJars, normalize = true, recurse = false)
          .filter(!_.getFileName.toString.endsWith(PathingJarSuffix))
      else installedJars

    log.info(s"Classpath expanded to ${classPath.size} elements")

    val urls = classPath.map(_.toUri.toURL).toArray
    if (isolated) new URLClassLoader(urls, null)
    else new URLClassLoader(urls)
  }

  def pathString(path: Path): String = pathString(path.toString)

  def pathString(path: String): String = {
    // convert \ to / (which works in Java APIs on both Linux and Windows)
    val str = path.replace('\\', '/')
    // if path starts with /, convert to // (since this works as a UNC path on windows and is the same as / on Linux)
    if (str.length > 1 && str.charAt(0) == '/' && str.charAt(1) != '/') "/" + str
    else str
  }
}

final case class JarFilter(inclusions: Seq[Regex] = Nil, exclusions: Seq[Regex] = Nil) {
  def include(path: String): Boolean =
    (inclusions.isEmpty || inclusions.exists(_.pattern.matcher(path).matches)) &&
      !exclusions.exists(_.pattern.matcher(path).matches)
}
