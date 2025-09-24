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

import java.nio.file.Files
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.EndsWithFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.utils.TypeClasses.OptionTupleOps

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class DomainGenerator(workspaceSourceRoot: Directory) extends SourceGenerator {
  override val generatorType: String = "domain"

  val generatorDefaults: PortableAfsExecutable = {
    val linuxConf = StaticConfig.stringSeq("domainGeneratorLinuxAfsExecutable")
    val linuxExecutable = AfsExecutable(linuxConf.head, linuxConf(1), linuxConf(2), None)
    val windowsConf = StaticConfig.stringSeq("domainGeneratorWindowsAfsExecutable")
    val windowsExecutable = AfsExecutable(windowsConf.head, windowsConf(1), windowsConf(2), None)
    PortableAfsExecutable(windows = windowsExecutable, linux = linuxExecutable)
  }
  val generatorExecutableNameForLog = "domainGenerator"
  val sourcePredicate: Directory.FileFilter = Directory.fileExtensionPredicate("xml")

  /**
   * This function calculates the command-line arguments required by the generator.
   *
   * Note: do not include the executable itself!
   */
  @node def args(inp: Inputs, outputDir: Directory): Seq[String] = {
    val templateFiles = inp.templateFiles.apar.flatMap { case (root, files) =>
      SourceGenerator.validateFiles(root, files).files
    }
    val codecDomainFiles = inp.codecDomains.apar.flatMap { case (root, files) =>
      SourceGenerator.validateFiles(root, files).files
    }

    val templateArgs = templateFiles.map(f => PathUtils.mappedPathString(f))
    val codecDomainArgs = codecDomainFiles.map(f => PathUtils.mappedPathString(f))
    Seq("-o", outputDir.path.toString) ++ Seq("-d") ++ templateArgs ++ Seq("-c") ++ codecDomainArgs
  }

  override type Inputs = DomainGenerator.Inputs

  @node override protected def _inputs(
      generatorName: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val generator = generatorDefaults.configured(configuration)
    val execDep = generator.dependencyDefinition(scope)
    val executable = generator.file(execDep.version)

    // we don't want the platform-specific execDir to be part of the fingerprint
    val execFingerprint = generatorDefaults.linux.file(execDep.version)

    val filter = sourceFilter && sourcePredicate
    val (templates, templateFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$generatorName"
    )

    val (codecDomains, codecFingerprint) =
      configuration
        .get("codecDomains")
        .map { b =>
          val paths = b.split(",").map(RelativePath(_)).toIndexedSeq
          SourceGenerator.rootedTemplates(
            internalFolders,
            externalFolders,
            EndsWithFilter(paths: _*),
            scope,
            workspaceSourceRoot,
            s"CodecDomain:$generatorName"
          )
        }
        .unzipOption

    val fingerprint = s"[${generatorExecutableNameForLog}]${execFingerprint.pathString}" +:
      (templateFingerprint ++ codecFingerprint.getOrElse(Nil))
    val fingerprintHash =
      scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint, Some(generatorName))

    DomainGenerator.Inputs(generatorName, executable, templates, codecDomains.getOrElse(Nil), fingerprintHash)
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templateFiles.flatMap(_._2).nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = DomainGenerator.oneAtTheTime {
    val resolvedInputs = inputs()
    import resolvedInputs._
    ObtTrace.traceTask(scopeId, GenerateSource) {

      val cmd = resolvedInputs.exec.path.toString

      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
        val outputDir = tempDir.resolveDir(DomainGenerator.SourcePath)
        Utils.createDirectories(outputDir)
        val cmdLine = Seq(cmd) ++ args(resolvedInputs, outputDir)

        log.debug(s"[$scopeId:$generatorName] command line: ${cmdLine.mkString(" ")}")
        val logging = mutable.Buffer[String]()
        // Note: this is a blocking call. See Optimus-49290 if this gets too slow.
        val ret: Int =
          Process(cmdLine) ! ProcessLogger { s =>
            val line = s"[$scopeId:$generatorName] $s"
            logging += line
            log.debug(line)
          }

        val sourcePath = DomainGenerator.SourcePath
        val messages =
          if (ret == 0) Nil
          else logging.map(s => CompilationMessage(None, s, CompilationMessage.Error))
        val a = GeneratedSourceArtifact.create(
          scopeId,
          tpe,
          generatorName,
          outputJar,
          sourcePath,
          messages.toIndexedSeq
        )
        SourceGenerator.createJar(tpe, generatorName, sourcePath, a.messages, a.hasErrors, tempJar, tempDir)()
        a
      }
      Some(artifact)
    }
  }
}

object DomainGenerator {
  private val oneAtTheTime = AdvancedUtils.newThrottle(1)
  private val SourcePath = RelativePath("src")

  final case class Inputs(
      generatorName: String,
      exec: FileAsset,
      templateFiles: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      codecDomains: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      fingerprint: FingerprintArtifact
  ) extends SourceGenerator.Inputs
}
