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

import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.compilers.Task
import optimus.buildtool.compilers.zinc.ScalacProvider.compilerBridge
import optimus.buildtool.compilers.zinc.ScalacProvider.sbtBridgeName
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.ScalaVersion
import optimus.buildtool.files.Directory
import sbt.internal.inc.AnalyzingCompiler
import sbt.internal.inc.ScalaInstance
import xsbti.compile.ClasspathOptionsUtil
import xsbti.compile.ZincCompilerUtil

import java.io.File
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.files.JarAsset
import optimus.buildtool.resolvers.CoursierArtifactResolver
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.utils.OptimusBuildToolProperties
import optimus.platform._
import optimus.platform.util.Log

import java.util.Properties
import scala.collection.compat._
import scala.math.Ordered.orderingToOrdered
import scala.util.Using

object ScalacProvider extends Log {
  private val forceCompilerBridgeCompilation = OptimusBuildToolProperties.getOrFalse("zinc.compileBridge")
  val firstSupportedScalaVersion = ScalaVersion("2.13.13")

  val zincVersionInJarPath = "incrementalcompiler.version.properties"
  val compilerBridge = "compiler-bridge"
  val sbtBridgeName = "scala2-sbt-bridge"

  lazy val zincVersion: String = {
    val props = new Properties()
    Using.resource(getClass.getClassLoader.getResourceAsStream(zincVersionInJarPath)) { in => props.load(in) }
    props.getProperty("version")
  }

  def instance(
      dependencyResolver: CoursierArtifactResolver,
      dependencyCopier: DependencyCopier,
      scalaVersionConfig: ScalaVersionConfig,
      classLoaderCaches: ZincClassLoaderCaches,
      interfaceDir: Directory,
  ): ScalacProvider = {
    if (scalaVersionConfig.scalaVersion >= firstSupportedScalaVersion && !forceCompilerBridgeCompilation) {
      log.debug(s"Fetching already compiled $sbtBridgeName")
      new ScalacProviderImpl(dependencyResolver, dependencyCopier, scalaVersionConfig, classLoaderCaches)
    } else {
      log.debug(s"Using legacy $compilerBridge compilation")
      new LegacyScalacProviderImpl(
        dependencyResolver,
        dependencyCopier,
        scalaVersionConfig,
        classLoaderCaches,
        interfaceDir)
    }
  }
}

trait ScalacProvider extends Log {

  val scalaVersionConfig: ScalaVersionConfig
  val classLoaderCaches: ZincClassLoaderCaches

  @node protected def getCompiledBridge(activeTask: => Task): File

  protected lazy val scalaLibrary: File =
    scalaVersionConfig.scalaJars
      .find { entry => entry.name.startsWith("scala-library") && entry.name.endsWith(".jar") }
      .map(_.path.toFile)
      .getOrElse(
        throw new RuntimeException(s"""Couldn't find scala-library jar.
                                      |Scala classpath: ${scalaVersionConfig.scalaJars.mkString(":")}""".stripMargin)
      )

  protected lazy val scalaInstance: ScalaInstance = {
    val classpath = scalaVersionConfig.scalaJars.map(_.path.toFile).distinct
    def classLoader: ClassLoader =
      classLoaderCaches.classLoaderFor((classpath diff Seq(scalaLibrary)).map(_.toPath), scalaLibraryLoader)
    def scalaLibraryLoader: ClassLoader = classLoaderCaches.classLoaderFor(Seq(scalaLibrary.toPath))

    new ScalaInstance(
      version = scalaVersionConfig.scalaVersion.value,
      loader = classLoader,
      loaderCompilerOnly = classLoader,
      loaderLibraryOnly = scalaLibraryLoader,
      libraryJars = Array(scalaLibrary),
      compilerJars = classpath.toArray,
      allJars = classpath.toArray,
      explicitActual = Option(scalaVersionConfig.scalaVersion.value)
    )
  }

  @node def scalac(activeTask: => Task): AnalyzingCompiler = {
    val classpathOptions = ClasspathOptionsUtil.javac( /*compiler =*/ false)
    val compilerInterfaceProvider =
      ZincCompilerUtil.constantBridgeProvider(scalaInstance, getCompiledBridge(activeTask))
    new AnalyzingCompiler(
      scalaInstance,
      compilerInterfaceProvider,
      classpathOptions,
      _ => (),
      classLoaderCaches.classLoaderCache
    )
  }

}

/**
 * New way to get compiler bridge instance.
 * It fetches already disted compiler bridge from artifactory since it is now a part of compiler itself.
 */
final class ScalacProviderImpl(
    dependencyResolver: CoursierArtifactResolver,
    dependencyCopier: DependencyCopier,
    val scalaVersionConfig: ScalaVersionConfig,
    val classLoaderCaches: ZincClassLoaderCaches,
) extends ScalacProvider {

  private lazy val compiledBridgeDefinition = DependencyDefinition(
    group = "org.scala-lang",
    name = sbtBridgeName,
    version = scalaVersionConfig.scalaVersion.value,
    kind = LocalDefinition,
    isMaven = true
  )

  @node override def getCompiledBridge(activeTask: => Task): File = {
    val result = dependencyResolver.resolveDependencies(DependencyDefinitions(Seq(compiledBridgeDefinition), Nil))
    val depCopied = {
      result.resolvedArtifacts.apar.map(dependencyCopier.atomicallyDepCopyExternalArtifactsIfMissing)
    }
    depCopied
      .find(_.pathString.contains(sbtBridgeName))
      .getOrElse {
        throw new RuntimeException(s"""Could not resolve ${sbtBridgeName}.
                                      |Result: ${depCopied.mkString}
                                      |Coursier messages: ${result.messages.mkString("\n")}
                                      |""".stripMargin)
      }
      .path
      .toFile
  }

}

/**
 * Legacy way to get compiler bridge instance.
 * It actually runs scala compiler for current version against the compiler bridge sources for configured zinc version.
 *
 * It is being kept in order to allow compilation of modules using Scala Version < 2.13.13
 */
final class LegacyScalacProviderImpl(
    dependencyResolver: CoursierArtifactResolver,
    dependencyCopier: DependencyCopier,
    val scalaVersionConfig: ScalaVersionConfig,
    val classLoaderCaches: ZincClassLoaderCaches,
    interfaceDir: Directory,
) extends ScalacProvider {

  private lazy val zincDependencyDefinition = DependencyDefinition(
    group = "org.scala-sbt",
    name = s"${compilerBridge}_${scalaVersionConfig.scalaMajorVersion}",
    version = ScalacProvider.zincVersion,
    kind = LocalDefinition,
    isMaven = true
  )

  // After protobuf changes we will be able to specify that we just need sources here
  @node private def doCoursierZincResolve(zinc: DependencyDefinition): Seq[JarAsset] = {
    val coursierResult =
      dependencyResolver.resolveDependencies(DependencyDefinitions(Seq(zinc), Nil))
    val depCopied =
      coursierResult.resolvedClassFileArtifacts.apar.map(dependencyCopier.atomicallyDepCopyClassFileArtifactsIfMissing)

    // MIGRATE TO THE SAME SOLUTION AS PROTOC
    depCopied.collect {
      case artifact: ExternalClassFileArtifact if artifact.source.exists(_.name.contains(compilerBridge)) =>
        artifact.source.get
      case other => other.file
    }
  }

  @node protected override def getCompiledBridge(activeTask: => Task): File = {
    val zincJars = doCoursierZincResolve(zincDependencyDefinition)
    CompilerBridgeCompiler
      .compile(
        interfaceDir,
        scalaInstance,
        ScalacProvider.zincVersion,
        zincJars,
        activeTask.trace.reportProgress("compiling zinc interface")
      )
      .path
      .toFile
  }
}
