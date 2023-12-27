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
package optimus.buildtool.compilers.cpp

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InternalCppArtifact
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileInJarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.ScopedCompilation

import java.io.File
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

final case class CppLibrary(
    scopeId: ScopeId,
    osVersion: String,
    buildType: BuildType,
    localFile: Option[FileInJarAsset],
    fallbackPath: Seq[Directory],
    preload: Boolean
)

object CppUtils {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  def libraries(artifacts: Seq[Artifact], transitiveDependencies: Seq[ScopedCompilation]): Seq[CppLibrary] = {
    val cppArtifacts = artifacts.collect { case c: InternalCppArtifact => c }

    val mappedArtifacts: Map[(ScopeId, String, BuildType), Option[FileInJarAsset]] = cppArtifacts.flatMap { a =>
      Seq(
        (a.scopeId, a.osVersion, BuildType.Release) -> a.releaseFile,
        (a.scopeId, a.osVersion, BuildType.Debug) -> a.debugFile
      )
    }.toMap

    for {
      s <- transitiveDependencies
      cfg <- s.config.cppConfigs
      osVersion = cfg.osVersion
      (buildCfg, buildType) <- cfg.release.map(r => (r, BuildType.Release)).toSeq ++
        cfg.debug.map(d => (d, BuildType.Debug)).toSeq if buildCfg.outputType.contains(OutputType.Library)
      fallbackPath = buildCfg.fallbackPath
      localFile = mappedArtifacts.get((s.id, osVersion, buildType)).flatten
    } yield CppLibrary(s.id, osVersion, buildType, localFile, fallbackPath, buildCfg.preload)
  }

  def linuxNativeExecutable(scopeId: ScopeId, buildType: BuildType): String =
    NamingConventions.scopeOutputName(scopeId, linuxMidfix(buildType), "")

  def linuxNativeLibrary(scopeId: ScopeId, buildType: BuildType): String =
    s"lib${NamingConventions.scopeOutputName(scopeId, linuxMidfix(buildType), "so")}"

  private def linuxMidfix(buildType: BuildType): String = buildType match {
    case BuildType.Release => ""
    case BuildType.Debug   => "-g"
  }

  def windowsNativeExecutable(scopeId: ScopeId, buildType: BuildType): String =
    NamingConventions.scopeOutputName(scopeId, windowsMidfix(buildType), "exe")

  def windowsNativeLibrary(scopeId: ScopeId, buildType: BuildType): String =
    NamingConventions.scopeOutputName(scopeId, windowsMidfix(buildType), "dll")

  private[buildtool] def windowsMidfix(buildType: BuildType): String = buildType match {
    case BuildType.Release => ""
    case BuildType.Debug   => "d"
  }

  private[cpp] def launch(
      prefix: String,
      cmdLine: Seq[String],
      inputFile: Option[RelativePath], // path is relative to local root (eg. <workspace>/src)
      workingDir: Option[Directory],
      paths: Map[String, Seq[Directory]],
      processType: String
  )(pf: PartialFunction[String, CompilationMessage]): Seq[CompilationMessage] = {
    val pb = new ProcessBuilder(cmdLine: _*)
    workingDir.foreach(d => pb.directory(d.path.toFile))
    paths.foreach { case (k, ps) =>
      pb.environment().put(k, ps.map(_.path.toString).mkString(File.pathSeparator))
    }

    val logging = mutable.Buffer[String]()
    logging += s"Command line: ${cmdLine.mkString(" ")}"
    val ret = Process(pb) ! ProcessLogger { s =>
      logging += s
    }
    logging += s"Return code: $ret"

    val messages = logging.collect(pf)

    val genericError = if (ret != 0 && !messages.exists(_.isError)) {
      logging.foreach(l => log.warn(s"$prefix $l"))
      Some(
        CompilationMessage(
          inputFile.map(f => MessagePosition(f.pathString)),
          s"$processType error\n${logging.mkString("\n")}",
          Severity.Error)
      )
    } else {
      logging.foreach(l => log.debug(s"$prefix $l"))
      None
    }

    messages.toIndexedSeq ++ genericError
  }
}
