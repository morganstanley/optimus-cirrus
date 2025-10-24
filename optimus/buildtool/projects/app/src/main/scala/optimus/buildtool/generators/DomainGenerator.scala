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
package optimus.buildtool.generators

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.EndsWithFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.DomainGenerator.CodecDomain
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedFiles.Template
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.PathUtils
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class DomainGenerator extends SourceGenerator {
  override val generatorType: String = "domain"

  val generatorDefaults: PortableAfsExecutable = {
    val linuxConf = StaticConfig.stringSeq("domainGeneratorLinuxAfsExecutable")
    val linuxExecutable = AfsExecutable(linuxConf.head, linuxConf(1), linuxConf(2), None)
    val windowsConf = StaticConfig.stringSeq("domainGeneratorWindowsAfsExecutable")
    val windowsExecutable = AfsExecutable(windowsConf.head, windowsConf(1), windowsConf(2), None)
    PortableAfsExecutable(windows = windowsExecutable, linux = linuxExecutable)
  }
  val generatorExecutableNameForLog = "domainGenerator"

  override def templateType(configuration: Map[String, String]): PathFilter = Directory.fileExtensionPredicate("xml")

  /**
   * This function calculates the command-line arguments required by the generator.
   *
   * Note: do not include the executable itself!
   */
  @async def args(inp: Inputs, outputDir: Directory): Seq[String] = {
    val templateFiles = inp.files.files(Template)
    val codecDomainFiles = inp.files.files(CodecDomain)

    val templateArgs = templateFiles.map(f => PathUtils.mappedPathString(f))
    val codecDomainArgs = codecDomainFiles.map(f => PathUtils.mappedPathString(f))
    Seq("-o", outputDir.path.toString) ++ Seq("-d") ++ templateArgs ++ Seq("-c") ++ codecDomainArgs
  }

  override type Inputs = DomainGenerator.Inputs

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val generator = generatorDefaults.configured(configuration)
    val execDep = generator.dependencyDefinition(scope)
    val executable = generator.file(execDep.version)

    // we don't want the platform-specific execDir to be part of the fingerprint
    val execFingerprint = generatorDefaults.linux.file(execDep.version)

    val files =
      configuration
        .get("codecDomains")
        .map { b =>
          val paths = b.split(",").map(RelativePath(_)).toIndexedSeq
          templates.withFiles(DomainGenerator.CodecDomain, EndsWithFilter(paths: _*))
        }
        .getOrElse(templates)

    val fingerprint =
      files.hashFingerprint(Map.empty, Seq(s"[$generatorExecutableNameForLog]${execFingerprint.pathString}"))

    DomainGenerator.Inputs(executable, files.toFiles, fingerprint)
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = DomainGenerator.oneAtTheTime {
    import inputs._

    val cmd = exec.path.toString

    writer.atomicallyWrite() { context =>
      val cmdLine = Seq(cmd) ++ args(inputs, context.outputDir)

      log.debug(s"[$scopeId:$generatorId] command line: ${cmdLine.mkString(" ")}")
      val logging = mutable.Buffer[String]()
      // Note: this is a blocking call. See Optimus-49290 if this gets too slow.
      val ret: Int =
        Process(cmdLine) ! ProcessLogger { s =>
          val line = s"[$scopeId:$generatorId] $s"
          logging += line
          log.debug(line)
        }

      val messages =
        if (ret == 0) Nil
        else logging.map(s => CompilationMessage(None, s, CompilationMessage.Error)).toSeq

      context.createArtifact(messages)
    }
  }
}

object DomainGenerator {
  private val oneAtTheTime = AdvancedUtils.newThrottle(1)

  case object CodecDomain extends SandboxedFiles.FileType

  final case class Inputs(
      exec: FileAsset,
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact
  ) extends SourceGenerator.Inputs
}
