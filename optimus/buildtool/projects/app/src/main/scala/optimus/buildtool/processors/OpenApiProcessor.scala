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
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.builders.OpenApiCmdId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.trace.OpenApiApp
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTException
import optimus.platform._
import optimus.platform.obt.openapi.OpenApiSpecBuilder
import optimus.platform.util.Log

import java.nio.file.Files
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

/**
 * Invokes the optimus.platform.obt.openapi.OpenApiSpecBuilder (from optimus.buildtool.rest) against the targetClass to
 * extract an OpenAPI interface description file. The OpenApiSpecBuilder and the targetClass are loaded from the locally
 * built code in a separate JVM, so it's important that the OpenApiSpecBuilder's behavior is backward compatible the
 * currently configured OBT version.
 */
@entity class OpenApiProcessor(logDir: Directory) extends ScopeProcessor {
  override val artifactType: ProcessorArtifactType = ArtifactType.OpenApi

  override type Inputs = OpenApiProcessor.Inputs

  @node override protected def _inputs(
      name: String,
      targetClass: FileAsset,
      templateHeaderFile: Option[FileAsset],
      templateFooterFile: Option[FileAsset],
      objectsFile: Option[FileAsset],
      installLocation: RelativePath,
      configuration: Map[String, String],
      scope: CompilationScope,
      javaAndScalaSources: JavaAndScalaCompilationSources): Inputs = {

    val fingerprintHash = ScopeProcessor.computeFingerprintHash(
      s"${name}_${targetClass.name}", // this will change if the name of the target class changes
      templateContent = None, // We don't really have a template file, just a target class
      templateHeaderContent = None,
      templateFooterContent = None,
      objectsContent = None,
      installLocation,
      configuration,
      javaAndScalaSources, // this will change if the content of the target class changes
      scope
    )

    OpenApiProcessor.Inputs(
      name,
      installLocation,
      fingerprintHash,
      targetClass,
      configuration
    )
  }

  @node override def dependencies(scope: CompilationScope): Seq[Artifact] =
    super.dependencies(scope) ++ scope.upstream.allUpstreamArtifacts
      .filter(a =>
        a.id == OpenApiProcessor.EntityAgentPathingArtifactId || a.id == OpenApiProcessor.BuildToolRestScalaArtifactId)

  @node override protected def generateContent(
      inputs: NodeFunction0[OpenApiProcessor.Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact]): Either[CompilationMessage, (RelativePath, String)] = {
    val in: Inputs = inputs()
    NodeTry {
      val content =
        OpenApiProcessor.generateContent(
          pathingArtifact,
          dependencyArtifacts,
          logDir,
          in
        )
      Right(in.installLocation -> content)
    }.getOrRecover { case ex @ RTException =>
      val id = s"${pathingArtifact.scopeId}:${in.processorName}"
      log.error(s"[$id]", ex)
      Left(CompilationMessage.error(s"[$id] ${ex.getMessage}"))
    }
  }

}

object OpenApiProcessor extends Log {

  final case class Inputs(
      processorName: String,
      installLocation: RelativePath,
      fingerprintHash: String,
      targetClass: FileAsset,
      configuration: Map[String, String]
  ) extends ScopeProcessor.Inputs

  // it looks a little odd to hardcode the names of codetree scopes here, but the OpenApiProcessor is inherently
  // coupled to the existance of these scopes so that it can invoke the OpenApiSpecBuilder. Note that if
  // OpenApiProcessor is not enabled for a scope, none of this code runs and therefore these scopes need not exist.
  val EntityAgentPathingArtifactId =
    InternalArtifactId(ScopeId("optimus", "platform", "entityagent", "main"), ArtifactType.Pathing, None)
  val BuildToolRestScalaArtifactId =
    InternalArtifactId(ScopeId("optimus", "buildtool", "rest", "main"), ArtifactType.Pathing, None)

  @async def generateContent(
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact],
      logDir: Directory,
      input: Inputs): String = {
    val name = input.processorName
    val targetClass = input.targetClass
    val configMemory = input.configuration.get("memory")
    val javaMemoryStr: String = configMemory match {
      case Some(memStr) => s"-Xmx$memStr"
      case None         => "-Xmx250m"
    }
    val scopeId = pathingArtifact.scopeId
    val outputFile = FileAsset(Files.createTempFile(s"${name}_$scopeId", ".tmp"))

    val buildtoolRestPathingArtifact = dependencyArtifacts
      .collect { case InternalArtifact(BuildToolRestScalaArtifactId, p: PathingArtifact) => p }
      .singleDistinctOption
      .getOrElse(failWithMissingBuildtoolRest())

    // note that optimus.buildtool.rest depends on optimus.platform.entityagent so we can reasonably expect this
    // to be present if that is
    val entityAgent = dependencyArtifacts.collect {
      case InternalArtifact(EntityAgentPathingArtifactId, p: PathingArtifact) => p
    }.singleDistinct

    val id = OpenApiCmdId(scopeId, name)
    val logFile = id.logFile(logDir)
    val (durationInNanos, (_, outputContent)) = AdvancedUtils.timed {
      aseq(
        BackgroundProcessBuilder
          .java(
            id,
            logFile = logFile,
            classpathArtifacts = Seq(pathingArtifact, buildtoolRestPathingArtifact),
            javaAgentArtifacts = Seq(entityAgent),
            // a fairly small environment should be sufficient
            javaOpts = Seq("-Doptimus.gthread.ideal=2", javaMemoryStr),
            mainClass = OpenApiSpecBuilder.getClass.getName.stripSuffix("$"),
            mainClassArgs = Seq("--target", targetClass.name, "--outputFile", outputFile.pathString)
          )
          .build(scopeId, OpenApiApp(name), lastLogLines = 20),
        try Files.readAllLines(outputFile.path).asScala.to(Seq).mkString(System.lineSeparator())
        finally Files.deleteIfExists(outputFile.path)
      )
    }

    log.info(s"[$id] Completed in ${Utils.durationString(durationInNanos / 1000000L)}")

    outputContent
  }

  private def failWithMissingBuildtoolRest(): Nothing =
    throw new IllegalArgumentException(
      "You must add optimus.buildtool.rest as a compileOnly (or compile) dependency of all scopes using the openapi processor")
}
