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
package optimus.buildtool.compilers.zinc

import java.nio.file.Path
import java.nio.file.Paths
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app.OptimusBuildToolImpl
import optimus.buildtool.compilers.zinc.ZincClassPaths.sourceRequiredFor
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.generators.ZincGenerator._
import optimus.buildtool.resolvers.CoursierArtifactResolver
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.utils.AssetUtils.readInputStreamLines
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

object ZincInstallationLocator {
  private val log = getLogger(getClass)
  private[buildtool] val InJarZincDepsFilePaths: Map[String, String] =
    ScalaVersions.map(sVer => zincDepsFileName(sVer) -> zincDepsFilePath(sVer)).toMap

  private def zincRuntimeJarPath: Path = Utils.findJar(classOf[sbt.internal.inc.InvalidationProfiler])

  private[buildtool] def zincRuntimeVersion: String = {
    val zincJar = zincRuntimeJarPath
    val lib = zincJar.getParent.getFileName.toString
    val pat = s"$lib-(.+)\\.jar".r
    zincJar.getFileName.toString match {
      case pat(v) => v // afs
      case _      => lib // maven
    }
  }
}

abstract class ZincInstallationLocator(scalaRuntimeMajorVersion: String) extends Log {
  import optimus.buildtool.compilers.zinc.ZincInstallationLocator._

  @node def getZincJars: Seq[JarAsset] = getZincArtifactsFromObtCompile

  protected def getZincArtifactsFromObtCompile: Seq[JarAsset] =
    InJarZincDepsFilePaths.values.find(_.contains(scalaRuntimeMajorVersion)) match {
      case Some(inJarZincDepsFile) =>
        // try search generated zinc path info from OBT build
        val zincRuntimeDep = OptimusBuildToolImpl.getClass.getResource(inJarZincDepsFile)
        val runtimeZincDeps: Seq[String] = if (zincRuntimeDep != null) {
          try {
            val zincDepInput = zincRuntimeDep.openStream()
            try readInputStreamLines(zincDepInput)
            finally zincDepInput.close()
          } catch {
            case NonFatal(e) =>
              log.debug(s"can't find zinc deps from obt runtime classpaths", e)
              Nil
          }
        } else Nil

        if (runtimeZincDeps.nonEmpty) {
          val runtimeObtAppDir = Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getParent
          val loadedJars = {
            val distinctPaths = runtimeZincDeps.map { pathStr =>
              if (pathStr.startsWith("..")) runtimeObtAppDir.resolve(pathStr).normalize()
              else Paths.get(pathStr).normalize()
            }.distinct
            val validJars = distinctPaths.map(JarAsset(_)).filter(_.exists)
            validJars
          }
          val loadedZinc =
            loadedJars.find(_.name.contains("zinc-core")).map(_.name).getOrElse("can't find zinc-core")
          val loadedCompilerBridge =
            loadedJars.find(_.name.contains(sourceRequiredFor)).map(_.name).getOrElse("can't find compiler-bridge")
          log.debug(s"Found runtime zinc deps file: $zincRuntimeDep, with $loadedZinc:$loadedCompilerBridge")
          log.info(s"Using zinc_$scalaRuntimeMajorVersion with ${loadedJars.size} resolved zinc artifacts")
          loadedJars
        } else {
          val runtimeZincDepsFile = Utils.findJar(OptimusBuildToolImpl.getClass)
          log.info(s"No zinc found from runtime classloader & zinc generator: $runtimeZincDepsFile")
          searchZincArtifactsFromDir(scalaRuntimeMajorVersion)
        }
      case None =>
        log.info(s"Can't find in jar zinc-deps file for scala version: $scalaRuntimeMajorVersion")
        searchZincArtifactsFromDir(scalaRuntimeMajorVersion)
    }

  /**
   * this method should not be called by normal build, it's the backup file scanner for unit test
   */
  protected def searchZincArtifactsFromDir(scalaVer: String): Seq[JarAsset] = {
    val (zincDirPath: Path, zincDirVersion: String) = {
      // maven-all/org/scala-sbt/zinc-core_2.12/1.10.0/zinc-core_2.12-1.10.0.jar
      val jarPath = zincRuntimeJarPath
      val sbtDir = jarPath.getParent.getParent.getParent
      val version = zincRuntimeVersion
      (sbtDir, version)
    }
    val dirZincJars = ZincClassPaths
      .zincClassPath(zincDirPath, zincDirVersion, scalaVer, forZincRun = false)
      .to(Seq)
    log.info(s"Using zinc_$scalaVer libs from dir: $zincDirPath with ${dirZincJars.size} artifacts")
    dirZincJars
  }

}

class ZincClasspathResolver(
    workspaceRoot: Directory,
    dependencyResolver: CoursierArtifactResolver,
    dependencyCopier: DependencyCopier,
    val externalDeps: ExternalDependencies,
    scalaMajorVersion: String)
    extends ZincInstallationLocator(scalaMajorVersion) {

  private[buildtool] def findZincDep(group: String, name: String): Seq[DependencyDefinition] = {
    val allScalaVerNames = ScalaVersions.map(sVer => s"${name}_$sVer")
    // search the predefined zinc
    allScalaVerNames
      .flatMap { n => externalDeps.mavenDependencies.allMavenDeps.find { d => d.group == group && d.name == n } }
      .to(Seq)
  }

  @node override def getZincJars: Seq[JarAsset] = {
    val runtimeZincJars = getZincArtifactsFromObtCompile
    if (runtimeZincJars.nonEmpty) runtimeZincJars
    else resolveZincJars(scalaMajorVersion)
  }

  @node private def doCoursierZincResolve(zinc: DependencyDefinition): Seq[JarAsset] = {
    // we skip mapping validation for zinc generator only, in order to resolve zinc for multiple scala versions
    val coursierResult =
      dependencyResolver.resolveDependencies(DependencyDefinitions(Seq(zinc), Nil), validate = false)
    val depCopied = coursierResult.resolvedArtifacts.apar.map(
      dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing(_))
    depCopied.flatMap { a =>
      val source =
        if (a.source.exists(_.asFile.name.contains(sourceRequiredFor)))
          a.source
        else None
      a.file +: source.to(Seq)
    }
  }

  private def nameWithScalaVer(name: String, scalaVer: String): String =
    // no zinc_2.11 for zinc version > 1.2.1
    if (scalaVer == "2.11") s"${name}_$scalaMajorVersion" else s"${name}_$scalaVer"

  @node private def resolveZinc(zincDep: DependencyDefinition, scalaVersion: String): Seq[JarAsset] = {
    val mavenZincResult = doCoursierZincResolve(zincDep)
    val zincJars =
      // for scala 2.11, we don't have zinc_2.11 release after zinc 1.2.1, therefore we can use current obt compile time
      // scala ver zinc with 2.11 compiler-bridge
      if (scalaVersion == "2.11") {
        val compilerBridge211 =
          DependencyDefinition(
            zincDep.group,
            s"${sourceRequiredFor}_2.11",
            zincDep.version,
            LocalDefinition,
            isMaven = true)
        val resolvedCompilerBridge = doCoursierZincResolve(compilerBridge211)
        if (resolvedCompilerBridge.isEmpty)
          log.warn(s"zinc generator: can't resolve compiler bridge ${compilerBridge211.key}")
        mavenZincResult.filterNot(_.name.contains(sourceRequiredFor)) ++ resolvedCompilerBridge
      } else mavenZincResult
    zincJars
  }

  @node private def resolveZincFromPredefinedVersion(
      group: String,
      name: String,
      scalaVersion: String): Seq[JarAsset] = {
    val predefinedZincMavenLib = findZincDep(group, name)
    predefinedZincMavenLib match {
      case mavenZinc :: otherScalaVersions =>
        val zincName = nameWithScalaVer(mavenZinc.name.substring(0, mavenZinc.name.indexOf("_")), scalaVersion)
        resolveZinc(mavenZinc.copy(name = zincName), scalaVersion)
      case _ => searchZincArtifactsFromDir(scalaVersion)
    }
  }

  @node private[buildtool] def resolveZincJars(
      scalaVer: String,
      group: Option[String] = None,
      name: Option[String] = None,
      version: Option[String] = None): Seq[JarAsset] =
    (group, name, version) match {
      case (Some(newGroup), Some(newName), Some(newMavenZincVer)) =>
        val predefinedZinc = findZincDep(newGroup, newName)
        val newZincDep = predefinedZinc match {
          // we only defined single version of zinc as dependency, should inherit zinc setting for all scala versions
          case loadedZinc :: _ if loadedZinc.group == newGroup && loadedZinc.name.contains(newName) =>
            loadedZinc.copy(name = nameWithScalaVer(newName, scalaVer), version = newMavenZincVer)
          case _ =>
            DependencyDefinition(
              newGroup,
              nameWithScalaVer(newName, scalaVer),
              newMavenZincVer,
              LocalDefinition,
              isMaven = true)
        }
        resolveZinc(newZincDep, scalaVer)
      case (Some(definedGroup), Some(definedName), None) =>
        resolveZincFromPredefinedVersion(definedGroup, definedName, scalaVer)
      case _ => searchZincArtifactsFromDir(scalaVer)
    }

}
