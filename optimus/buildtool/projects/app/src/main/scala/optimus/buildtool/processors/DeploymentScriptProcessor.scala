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
package optimus.buildtool.processors

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.processors.DeploymentScriptProcessor.DefaultFileName
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.process.ExternalProcessBuilder
import optimus.platform._
import optimus.platform.util.Log

import java.nio.file.Files
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * OBT Processor Task to generate deployment files as part of module build
 *
 * Sample Usage:
 * deploy {
 *    template = resources/deploy-template.jinja2
 *    objects = resources/deploy-params.yaml
 *    installLocation = deploy-dir/deploy.yaml
 * }
 */
@entity class DeploymentScriptProcessor(sandboxFactory: SandboxFactory, processBuilder: ExternalProcessBuilder)
    extends ScopeProcessor {

  override def artifactType: ProcessorArtifactType = ArtifactType.DeploymentScript

  // Task tracing is handled by the BackgroundProcess in DeploymentScriptWriter
  override val recordTask: Boolean = false

  override type Inputs = DeploymentScriptProcessor.Inputs
  @node override protected def _inputs(
      name: String,
      templateFile: FileAsset,
      templateHeaderFile: Option[FileAsset],
      templateFooterFile: Option[FileAsset],
      objectsFile: Option[FileAsset],
      installLocation: RelativePath,
      configuration: Map[String, String],
      scope: CompilationScope,
      javaAndScalaSources: JavaAndScalaCompilationSources): DeploymentScriptProcessor.Inputs = {

    val templateContent = content(templateFile, scope)
    val templateHeaderContent = None
    val templateFooterContent = None
    val objectsContent = objectsFile.flatMap(content(_, scope))

    // Java/Scala sources and dependencies are irrelevant for deployment script
    val fingerprint = ScopeProcessor.computeFingerprintHashWithoutSources(
      name,
      templateContent,
      templateHeaderContent,
      templateFooterContent,
      objectsContent,
      installLocation,
      configuration,
      scope)

    val workspaceSrcRoot = scope.config.paths.workspaceSourceRoot

    DeploymentScriptProcessor.Inputs(
      scopeId = scope.id,
      processorName = name,
      workspaceSrcRoot = workspaceSrcRoot,
      templateContent = templateContent,
      objectsContent = objectsContent,
      configuration = configuration,
      installLocation = installLocation,
      fingerprint = fingerprint
    )
  }

  @node override protected def generateContent(
      inputs: NodeFunction0[DeploymentScriptProcessor.this.Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact]): Either[CompilationMessage, (RelativePath, String)] = {

    val in = inputs()

    val template = toSourceFile("Template", in.workspaceSrcRoot, in.templateContent)
    val parameters = toSourceFile("Parameter", in.workspaceSrcRoot, in.objectsContent)

    val files = Seq(template, parameters).collect { case Right(f) => f }.toMap
    val sandbox = sandboxFactory(s"${in.scopeId.properPath}-deploy", files)

    val r = NodeTry {
      for {
        _ <-
          if (!DeploymentScriptProcessor.isCorrectFormatForDeployInstallation(in.installLocation.pathString)) {
            val msg =
              s"Install Location not in format ${StaticConfig.string("deploymentInstallPrefix")}{name-of-file}.yaml"
            Left(CompilationMessage.error(msg))
          } else {
            Right(())
          }

        outputDir <- Try {
          val outputDir = sandbox.buildDir
          Files.createDirectories(outputDir.path)
          outputDir
        }.toEither.left.map(t => CompilationMessage.error("Couldn't create sandbox directories", t))

        outputFile <- Try {
          val outputFile = sandbox.buildDir.resolveFile(DefaultFileName)
          Files.write(
            outputFile.path,
            "".getBytes()
          ) // force creation of file and folders
          outputFile
        }.toEither.left.map(t => CompilationMessage.error("Couldn't create output file", t))
        templateFile <- template
        parameterFile <- parameters
        result <- DeploymentScriptProcessor.generateDeploymentFiles(
          pathingArtifact.scopeId,
          in.processorName,
          sandbox.source(templateFile._1).file,
          sandbox.source(parameterFile._1).file,
          outputDir,
          new DeploymentScriptWriter(processBuilder)
        ) match {
          case Success(r) =>
            log.debug(s"Command output:\n${r.response}")
            Right(in.installLocation -> Files.readString(outputFile.path))
          case Failure(e) =>
            Left(CompilationMessage.error(e))
        }

      } yield result
    }
    sandbox.close()
    r.get
  }

  @node private def toSourceFile(context: String, dir: Directory, fileWithContent: Option[(FileAsset, HashedContent)])
      : Either[CompilationMessage, (SourceUnitId, HashedContent)] = {
    fileWithContent match {
      case None => Left(CompilationMessage.error(s"$context file not found"))
      case Some((f, hc)) =>
        Right(SourceFileId(dir.relativize(f), RelativePath(f.name)) -> hc)
    }
  }

}

object DeploymentScriptProcessor extends Log {
  final case class Inputs(
      scopeId: ScopeId,
      processorName: String,
      workspaceSrcRoot: Directory,
      templateContent: Option[(FileAsset, HashedContent)],
      objectsContent: Option[(FileAsset, HashedContent)],
      configuration: Map[String, String],
      installLocation: RelativePath,
      fingerprint: FingerprintArtifact
  ) extends ScopeProcessor.Inputs

  val DefaultFileName: String = StaticConfig.string("deploymentDefaultFileName")

  @async def generateDeploymentFiles(
      scopeId: ScopeId,
      processorName: String,
      templateFile: FileAsset,
      parameterFile: FileAsset,
      installLocation: Directory,
      writer: DeploymentScriptWriter
  ): Try[DeploymentResponse] = {

    log.info(s"Generating deployment scripts using $scopeId:$processorName")
    writer.generateDeploymentScripts(templateFile, parameterFile, installLocation, scopeId)
  }

  def isCorrectFormatForDeployInstallation(outputDir: String): Boolean = {
    outputDir.startsWith(StaticConfig.string("deploymentInstallPrefix")) && outputDir.endsWith(".yaml")
  }

}
