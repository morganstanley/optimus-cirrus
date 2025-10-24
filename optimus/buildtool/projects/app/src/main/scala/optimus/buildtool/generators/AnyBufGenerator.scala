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
import optimus.buildtool.artifacts.ExternalBinaryArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.PredicateFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedFiles.Template
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.resolvers.CoursierArtifactResolver
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.MavenUtils
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.PathUtils
import optimus.platform._

import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class ProtobufGenerator(
    dependencyResolver: CoursierArtifactResolver,
    dependencyCopier: DependencyCopier
) extends AnyBufGenerator {
  import AnyBufGenerator._

  override val generatorType: String = "protobuf"

  override val generatorDefaults: PortableMavenExecutable =
    PortableMavenExecutable(
      dependencyResolver,
      dependencyCopier,
      windows = MavenExecutable(
        organisation = "com.google.protobuf",
        project = "protoc",
        classifier = Some(Platforms.Windows.classifier)),
      linux = MavenExecutable(
        organisation = "com.google.protobuf",
        project = "protoc",
        classifier = Some(Platforms.Linux.classifier))
    )

  override val generatorExecutableNameForLog = "protoc"
  override val filePredicate: PathFilter = Directory.fileExtensionPredicate("proto")

  @async override def args(inp: Inputs, outputDir: Directory): Seq[String] = {

    val templateFiles = inp.files.files(Template)

    val pluginArgs = inp.plugins.apar.flatMap { case (pluginName, pluginMavenDep) =>
      val res =
        dependencyResolver.resolveDependencies(DependencyDefinitions(List(pluginMavenDep), Nil))
      res.resolvedArtifacts.toList match {
        case (exec: ExternalBinaryArtifact) :: Nil =>
          val depCopyExec = dependencyCopier.atomicallyDepCopyFileIfMissing(exec.file)
          MavenUtils.setExecutablePermissions(depCopyExec)
          Seq(
            s"--plugin=protoc-gen-$pluginName=${depCopyExec.path.toString}",
            s"--${pluginName}_out=${outputDir.path.toString}")
        case rest =>
          throw new IllegalStateException(s"There should be exactly one artifact downloaded but was $rest")
      }
    }

    // protoc requires template and dependency directories to be specified as include arguments
    // use platform-appropriate Strings
    val includeArgs =
      Seq(Template, Include, Upstream).apar.flatMap(inp.files.roots).map(r => s"-I=${PathUtils.mappedPathString(r)}")
    val templateArgs = templateFiles.map(PathUtils.mappedPathString)

    val mainArgLine = Seq(s"--java_out=${outputDir.path.toString}") ++ includeArgs

    mainArgLine ++ pluginArgs ++ templateArgs
  }
}

@entity class FlatbufferGenerator extends AnyBufGenerator {
  override val generatorType: String = "flatbuffer"
  override val generatorDefaults: PortableExecutable =
    PortableAfsExecutable(
      windows = AfsExecutable("dotnet3rd", "google-flatbuffers", "flatc", None),
      linux = AfsExecutable("fsf", "google-flatbuffers", "bin/flatc", None)
    )
  override val generatorExecutableNameForLog = "flatc"
  override val filePredicate: PathFilter = Directory.fileExtensionPredicate("fbs")

  @async override def args(inp: Inputs, outputDir: Directory): Seq[String] = {
    val templateFiles = inp.files.files(Template)

    val args = Seq("--gen-mutable", "--java", "-o", outputDir.path.toString) ++ templateFiles.map(_.path.toString)
    Seq(args.mkString(" "))
  }
}

@entity trait AnyBufGenerator extends SourceGenerator {
  import AnyBufGenerator._

  val generatorDefaults: ResolvableResource
  val generatorExecutableNameForLog: String

  override def templateType(configuration: Map[String, String]): PathFilter = filePredicate
  protected val filePredicate: PathFilter

  /**
   * This function calculates the command-line arguments required by the *buf generator.
   *
   * Note: do not include the executable itself!
   */
  @async def args(inp: Inputs, outputDir: Directory): Seq[String]

  override type Inputs = AnyBufGenerator.Inputs

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): AnyBufGenerator.Inputs = {
    val executable = generatorDefaults.resolve(configuration, scope)
    val execFingerprint = generatorDefaults.fingerprint(configuration, scope)

    val allTemplatePaths = templates.content().keySet.map(_.path)

    // Protoc also needs the template directories to be on the include path, so fingerprint any other files
    // in the template directories (and capture the file content in case we're in a sparse workspace)
    val templatesWithIncludes = templates.withFiles(
      Include,
      filePredicate && PredicateFilter(p => !allTemplatePaths.contains(p)),
      allowEmpty = true
    )

    // Proto files may depend on proto files in upstream scopes
    val upstreamTemplateFolders: Seq[UpstreamFolders] =
      scope.upstream.allCompileDependencies.apar.flatMap(_.transitiveScopeDependencies).apar.flatMap { d =>
        d.config.generatorConfig.apar.collect {
          case (tpe, cfg) if tpe == this.tpe =>
            val internalTemplateFolders = cfg.internalTemplateFolders.apar.map { f =>
              scope.directoryFactory
                .lookupSourceFolder(d.config.paths.workspaceSourceRoot, d.config.paths.absScopeRoot.resolveDir(f))
            }
            val externalTemplateFolders = cfg.externalTemplateFolders.apar.map(scope.directoryFactory.reactive)
            UpstreamFolders(d.id, internalTemplateFolders, externalTemplateFolders)
        }: Seq[UpstreamFolders] // help intellij work out the types
      }

    val files = upstreamTemplateFolders.foldLeft(templatesWithIncludes) { (accum, upstream) =>
      // TODO (OPTIMUS-79166): Remove `allowEmpty = true` once we have a better solution for
      // built-in proto files than just putting them in the templates
      accum.withFolders(
        Upstream,
        upstream.internal,
        upstream.external,
        filePredicate,
        fingerprintTypeSuffix = upstream.scopeId.toString,
        allowEmpty = true
      )
    }

    val plugins: Seq[(String, DependencyDefinition)] = configuration
      .collect { case (k, v) if k.startsWith(pluginKey) => (k, v) }
      .map { case (pluginName, pluginMavenKey) =>
        val maybeDep = scope.externalDependencyResolver.dependencyDefinitions.find(p => pluginMavenKey == p.key)
        maybeDep match {
          case Some(dep) => (pluginName.stripPrefix(pluginKey), dep.withClassifier(Some(Platforms.current.classifier)))
          case None      => throw new IllegalStateException(s"Could not find matching dependency for $pluginMavenKey")
        }
      }
      .toSeq

    val pluginsFingerprint: Seq[String] = plugins.map { case (pluginName: String, b: DependencyDefinition) =>
      s"[$generatorExecutableNameForLog]:$pluginName:${b.fingerprint("Executable")}"
    }
    val extraFingerprint = s"[$generatorExecutableNameForLog]$execFingerprint" +: pluginsFingerprint
    val fingerprintHash = files.hashFingerprint(Map.empty, extraFingerprint)

    AnyBufGenerator.Inputs(executable, files.toFiles, fingerprintHash, plugins)
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: AnyBufGenerator.Inputs,
      writer: ArtifactWriter): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { context =>
    import inputs._
    val protocExecCmd: String = exec.path.toString

    val cmdLine = s"$protocExecCmd ${args(inputs, context.outputDir).mkString(" ")}"
    log.debug(s"[$scopeId:$generatorId] executing: '$cmdLine''")
    val logging = mutable.Buffer[String]()
    // Note: this is a blocking call. See Optimus-49290 if this gets too slow.
    val res: Int =
      Process(cmdLine) ! ProcessLogger { s =>
        val line = s"[$scopeId:$generatorId] $s"
        logging += line
        log.debug(line)
      }

    val messages = logging.toIndexedSeq
      .map(s => CompilationMessage(None, s, if (res == 0) CompilationMessage.Info else CompilationMessage.Error))

    context.createArtifact(messages)
  }
}

object AnyBufGenerator {
  private val pluginKey = "plugins."

  case object Include extends SandboxedFiles.FileType
  case object Upstream extends SandboxedFiles.FileType

  final case class Inputs(
      exec: FileAsset,
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact,
      plugins: Seq[(String, DependencyDefinition)]
  ) extends SourceGenerator.Inputs

  private final case class UpstreamFolders(
      scopeId: ScopeId,
      internal: Seq[SourceFolder],
      external: Seq[ReactiveDirectory])
}
