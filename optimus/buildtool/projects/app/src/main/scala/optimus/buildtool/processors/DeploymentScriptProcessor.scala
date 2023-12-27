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
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.SourceGenerator
import optimus.buildtool.processors.DeploymentScriptProcessor.DefaultFileName
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.SandboxFactory
import optimus.platform._
import optimus.platform.util.Log

import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * OBT Processor Task to generate deployment files as part of module build
 *
 * Sample Usage
 * ```
 * deploy {
 *    template = resources/deploy-template.jinja2
 *    objects = resources/deploy-params.yaml
 *    installLocation = deploy-dir/deploy.yaml
 * }
 * ```
 */
@entity class DeploymentScriptProcessor(sandboxFactory: SandboxFactory) extends ScopeProcessor {

  override def artifactType: ProcessorArtifactType = ArtifactType.DeploymentScript

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

    val fingerprintHash = ScopeProcessor.computeFingerprintHash(
      name,
      templateContent,
      templateHeaderContent,
      templateFooterContent,
      objectsContent,
      installLocation,
      configuration,
      javaAndScalaSources,
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
      fingerprintHash = fingerprintHash
    )
  }
  @node override protected def generateContent(
      inputs: NodeFunction0[DeploymentScriptProcessor.this.Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact]): Either[CompilationMessage, (RelativePath, String)] = {

    val in = inputs()
    val sandbox = sandboxFactory(s"${in.scopeId.properPath}-deploy", Map.empty)
    val validatedTemplateFile = validateFile("Template", in.workspaceSrcRoot, in.templateContent)
    val validatedParameterFile = validateFile("Parameter", in.workspaceSrcRoot, in.objectsContent)

    try {
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
        templateFile <- validatedTemplateFile
        parameterFile <- validatedParameterFile
        result <- DeploymentScriptProcessor.generateDeploymentFiles(
          pathingArtifact.scopeId,
          in.processorName,
          templateFile,
          parameterFile,
          outputDir,
          new DeploymentScriptWriter
        ) match {
          case Success(r) =>
            log.debug(s"Command output:\n${r.response}")
            Right(in.installLocation -> Files.readString(outputFile.path))
          case Failure(e) =>
            Left(CompilationMessage.error(e))
        }

      } yield result
    } finally sandbox.close()
  }

  @node private def validateFile(
      context: String,
      dir: Directory,
      fileWithContent: Option[(FileAsset, HashedContent)]): Either[CompilationMessage, FileAsset] = {
    fileWithContent match {
      case None => Left(CompilationMessage.error(s"$context file not found"))
      case Some((f, hc)) =>
        val foundFile = SourceGenerator.validateFile(dir, f, hc)
        foundFile.toRight(CompilationMessage.error(s"$f doesn't exist"))
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
      fingerprintHash: String
  ) extends ScopeProcessor.Inputs

  val DefaultFileName: String = StaticConfig.string("deploymentDefaultFileName")

  private val writer = new DeploymentScriptWriter
  def generateDeploymentFiles(
      scopeId: ScopeId,
      processorName: String,
      templateFile: FileAsset,
      parameterFile: FileAsset,
      installLocation: Directory
  ): Try[DeploymentResponse] =
    generateDeploymentFiles(scopeId, processorName, templateFile, parameterFile, installLocation, writer)

  def generateDeploymentFiles(
      scopeId: ScopeId,
      processorName: String,
      templateFile: FileAsset,
      parameterFile: FileAsset,
      installLocation: Directory,
      generator: DeploymentScriptWriter
  ): Try[DeploymentResponse] = {

    log.info(s"Generating deployment scripts using $scopeId:$processorName")
    generator.generateDeploymentScripts(templateFile, parameterFile, installLocation)
  }

  def isCorrectFormatForDeployInstallation(outputDir: String): Boolean = {
    outputDir.startsWith(StaticConfig.string("deploymentInstallPrefix")) && outputDir.endsWith(".yaml")
  }

}
