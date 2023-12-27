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
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.PredicateFilter
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

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class ProtobufGenerator(val workspaceSourceRoot: Directory) extends AnyBufGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.ProtoBuf

  override val generatorDefaults: PortableAfsExecutable = {
    val fsfprotoc = AfsExecutable("fsf", "protoc", "exec/bin/protoc", None)
    PortableAfsExecutable(windows = fsfprotoc, linux = fsfprotoc)
  }
  override val generatorExecutableNameForLog = "protoc"
  override val sourcePredicate = Directory.fileExtensionPredicate("proto")

  @node override def args(inp: Inputs, outputDir: Directory): Seq[String] = {
    val (templateDirs, templateFiles) = inp.templates.apar.map { case (root, files) =>
      val validated = SourceGenerator.validateFiles(root, files)
      (validated.root, validated.files)
    }.unzip

    val dependencyDirs = inp.dependencies.apar.map { case (root, files) =>
      SourceGenerator.validateFiles(root, files).root
    }

    // protoc requires template and dependency directories to be specified as include arguments
    // use platform-appropriate Strings
    val includeArgs = (templateDirs ++ dependencyDirs).distinct.map(d => s"-I=${PathUtils.mappedPathString(d)}")
    val templateArgs = templateFiles.flatten.map(PathUtils.mappedPathString)
    Seq(s"--java_out=${outputDir.path.toString}") ++ includeArgs ++ templateArgs
  }
}

@entity class FlatbufferGenerator(val workspaceSourceRoot: Directory) extends AnyBufGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.FlatBuffer
  override val generatorDefaults: PortableAfsExecutable =
    PortableAfsExecutable(
      windows = AfsExecutable("dotnet3rd", "google-flatbuffers", "flatc", None),
      linux = AfsExecutable("fsf", "google-flatbuffers", "bin/flatc", None)
    )
  override val generatorExecutableNameForLog = "flatc"
  override val sourcePredicate: Directory.PathFilter = Directory.fileExtensionPredicate("fbs")

  @node override def args(inp: Inputs, outputDir: Directory): Seq[String] = {
    val templateFiles = inp.templates.apar.map { case (root, files) =>
      val validated = SourceGenerator.validateFiles(root, files)
      validated.files
    }

    Seq("--gen-mutable", "--java", "-o", outputDir.path.toString) ++ templateFiles.flatten.map(_.path.toString)
  }
}

@entity trait AnyBufGenerator extends SourceGenerator {
  val workspaceSourceRoot: Directory
  val generatorDefaults: PortableAfsExecutable
  val sourcePredicate: PathFilter
  val generatorExecutableNameForLog: String

  /**
   * This function calculates the command-line arguments required by the *buf generator.
   *
   * Note: do not include the executable itself!
   */
  @node def args(inp: Inputs, outputDir: Directory): Seq[String]

  override type Inputs = AnyBufGenerator.Inputs

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
    val allTemplatePaths = templates.flatMap(_._2.keys).map(_.path).toSet

    // Proto files may depend on proto files in upstream scopes
    val upstreamTemplateFolders =
      scope.upstream.allCompileDependencies.apar.flatMap(_.transitiveScopeDependencies).apar.flatMap { d =>
        d.config.generatorConfig.apar.collect {
          case (tpe, cfg) if tpe == this.tpe =>
            val internalTemplateFolders = cfg.internalTemplateFolders.apar.map { f =>
              scope.directoryFactory
                .lookupSourceFolder(d.config.paths.workspaceSourceRoot, d.config.paths.absScopeRoot.resolveDir(f))
            }
            val externalTemplateFolders = cfg.externalTemplateFolders.apar.map(scope.directoryFactory.reactive)
            (d.id, internalTemplateFolders, externalTemplateFolders)
        }
      }

    // Protoc also needs the template directories to be on the include path, so fingerprint any other files
    // in the template directories (and capture the file content in case we're in a sparse workspace)
    val (localDependencies, localDependencyFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      sourcePredicate && PredicateFilter(p => !allTemplatePaths.contains(p)),
      scope,
      workspaceSourceRoot,
      s"Template:$generatorName"
    )

    val (upstreamDependencies, upstreamDependencyFingerprints) = upstreamTemplateFolders.apar.map {
      case (id, internalDepFolders, externalDepFolders) =>
        SourceGenerator.rootedTemplates(
          internalDepFolders,
          externalDepFolders,
          sourcePredicate,
          scope,
          workspaceSourceRoot,
          "Dependency",
          id.toString
        )
    }.unzip

    val dependencies = localDependencies ++ upstreamDependencies.flatten

    val fingerprint = s"[${generatorExecutableNameForLog}]${execFingerprint.pathString}" +:
      (templateFingerprint ++ localDependencyFingerprint ++ upstreamDependencyFingerprints.flatten)
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint)

    AnyBufGenerator.Inputs(generatorName, executable, templates, dependencies, fingerprintHash)
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templates.flatMap(_._2).nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    ObtTrace.traceTask(scopeId, GenerateSource) {

      val cmd = resolvedInputs.exec.path.toString

      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
        val outputDir = tempDir.resolveDir(AnyBufGenerator.SourcePath)
        Utils.createDirectories(outputDir)
        val cmdLine = Seq(cmd) ++ args(resolvedInputs, outputDir)

        log.debug(s"[$scopeId:$generatorName] command line: ${cmdLine.mkString(" ")}")
        val logging = mutable.Buffer[String]()
        // Note: this is a blocking call. See Optimus-49290 if this gets too slow.
        val ret = Process(cmdLine) ! ProcessLogger { s =>
          val line = s"[$scopeId:$generatorName:protoc] $s"
          logging += line
          log.debug(line)
        }

        val sourcePath = AnyBufGenerator.SourcePath
        val messages = if (ret == 0) Nil else logging.map(s => CompilationMessage(None, s, CompilationMessage.Error))
        val a = GeneratedSourceArtifact.create(
          scopeId,
          generatorName,
          artifactType,
          outputJar,
          sourcePath,
          messages.toIndexedSeq
        )
        SourceGenerator.createJar(generatorName, sourcePath, a.messages, a.hasErrors, tempJar, tempDir)()
        a
      }
      Some(artifact)
    }
  }
}

object AnyBufGenerator {
  private val SourcePath = RelativePath("src")

  final case class Inputs(
      generatorName: String,
      exec: FileAsset,
      templates: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      dependencies: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      fingerprintHash: String
  ) extends SourceGenerator.Inputs
}
