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

import java.io.File
import java.io.InputStream
import java.util.Properties
import optimus.buildtool.app.BuildInstrumentation
import optimus.buildtool.cache.ArtifactReader
import optimus.buildtool.cache.ArtifactWriter
import optimus.buildtool.cache.SearchableArtifactStore
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.SyncCompilerFactory
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.platform._
import xsbti.VirtualFile
import xsbti.compile.IncOptions

import java.net.URI
import scala.util.control.NonFatal

private[buildtool] final case class ZincCompilerFactory(
    jdkPath: Directory,
    scalaConfig: ScalaVersionConfig,
    scalacProvider: ScalacProvider,
    workspaceRoot: Directory,
    buildDir: Directory,
    interfaceDir: Directory,
    depCopyRoot: Directory,
    cachePluginAndMacroClassLoaders: Boolean,
    zincIgnorePluginHash: Boolean = false,
    zincIgnoreChangesRegexp: String = "jre/lib/rt.jar$",
    analysisCache: ZincAnalysisCache,
    zincRecompileAllFraction: Double,
    instrumentation: BuildInstrumentation,
    bspServer: Boolean = false,
    localArtifactStore: SearchableArtifactStore,
    remoteArtifactReader: Option[ArtifactReader],
    remoteArtifactWriter: Option[ArtifactWriter],
    classLoaderCaches: ZincClassLoaderCaches,
    scalacProfileDir: Option[Directory],
    strictErrorTolerance: Boolean,
    zincOptionMutator: IncOptions => IncOptions = identity _,
    zincTrackLookups: Boolean,
    depCopyFileSystemAsset: Boolean,
    compilerThrottle: CompilerThrottle
) extends SyncCompilerFactory {

  @async override def throttled[T](sizeBytes: Int)(f: NodeFunction0[T]): T = compilerThrottle.throttled(sizeBytes)(f)

  private[zinc] lazy val scalaClassPath: Seq[JarAsset] = scalaConfig.scalaJars.toSeq

  private[zinc] lazy val jvmJars: Seq[VirtualFile] = {
    val jreLibPath = jdkPath.resolveDir("jre").resolveDir("lib")
    val jceJar = jreLibPath.resolveJar("jce.jar")
    val rtJar = jreLibPath.resolveJar("rt.jar")
    val toolsJar = jdkPath.resolveDir("lib").resolveJar("tools.jar")
    Seq(jdkPath, toolsJar, jceJar, rtJar).filter(_.existsUnsafe).map(j => SimpleVirtualFile(j.path))
  }

  def coreClasspath: Seq[VirtualFile] = jvmJars ++ scalaClassPath.map { j =>
    assert(j.existsUnsafe, s"$j does not exist")
    SimpleVirtualFile(j.path)
  }

  override def fingerprint(traceType: MessageTrace): Seq[String] = {
    val contentHash = Hashing.hashStrings(scalaConfig.scalaJars.map(Hashing.hashFileContent))
    val cat = traceType.categoryName
    val trackLookups = if (zincTrackLookups) List("[Zinc:Lookups]tracked") else Nil
    List(
      PathUtils
        .fingerprintElement(s"Zinc:$cat", Hashing.hashString(scalaConfig.scalaVersion.value), contentHash),
      s"[Zinc:$cat]${Utils.javaSpecVersionTag}",
      s"[Zinc:$cat]${Utils.javaClassVersionTag}",
      s"[Zinc:$cat]scalaMajorVersion=${scalaConfig.scalaMajorVersion}"
    ) ++ trackLookups
  }

  override def newCompiler(scopeId: ScopeId, traceType: MessageTrace): SyncCompiler =
    new ZincCompiler(this, scopeId, traceType)

  private def findScalaJar(baseJarName: String, scalaClasspath: Iterable[File]): File =
    scalaClasspath
      .find { entry =>
        entry.getName.startsWith(baseJarName) && entry.getName.endsWith(".jar")
      }
      .getOrElse(
        throw new RuntimeException(s"""Couldn't find $baseJarName jar.
                                      |Scala classpath: ${scalaClasspath.mkString(":")}""".stripMargin)
      )

  private def readScalaVersion(scalaClasspath: Seq[File]): String = {
    val scalaCompiler = findScalaJar("scala-compiler", scalaClasspath)
    readScalaVersion(scalaCompiler)
  }

  private def readScalaVersion(compilerJar: File): String = {
    var input: InputStream = null
    try {
      input = new URI(s"jar:${compilerJar.toURI}!/compiler.properties").toURL().openStream()
      val properties = new Properties
      properties.load(input)
      properties.getProperty("version.number")
    } catch {
      case NonFatal(_) => "unknown"
    } finally {
      if (input != null) input.close()
    }
  }
}
