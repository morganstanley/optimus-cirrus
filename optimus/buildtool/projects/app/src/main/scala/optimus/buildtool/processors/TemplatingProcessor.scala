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
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.HashedContent
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import scala.util.control.NonFatal

@entity trait TemplatingProcessor extends ScopeProcessor {

  override type Inputs = TemplatingProcessor.Inputs

  @node override protected def _inputs(
      name: String,
      templateFile: FileAsset,
      templateHeaderFile: Option[FileAsset],
      templateFooterFile: Option[FileAsset],
      objectsFile: Option[FileAsset],
      installLocation: RelativePath,
      configuration: Map[String, String],
      scope: CompilationScope,
      javaAndScalaSources: JavaAndScalaCompilationSources): TemplatingProcessor.Inputs = {

    val templateContent = content(templateFile, scope)
    val templateHeaderContent = templateHeaderFile.flatMap(content(_, scope))
    val templateFooterContent = templateFooterFile.flatMap(content(_, scope))
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

    TemplatingProcessor.Inputs(
      workspaceSrcRoot = workspaceSrcRoot,
      processorName = name,
      templateContent = templateContent,
      templateHeaderContent = templateHeaderContent,
      templateFooterContent = templateFooterContent,
      objectsContent = objectsContent,
      installLocation = installLocation,
      configuration = configuration,
      fingerprintHash = fingerprintHash
    )
  }

  @node override protected def generateContent(
      inputs: NodeFunction0[TemplatingProcessor.Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact]): Either[CompilationMessage, (RelativePath, String)] = {
    val in = inputs()
    val classLoader = new URLClassLoader(Seq(pathingArtifact.path.toUri.toURL), null)
    try {
      val objects = loadAndInitializeClasses(in.objectsContent.map(_._2), classLoader)
      val templateWithHeaderAndFooter = (in.templateHeaderContent ++ in.templateContent ++ in.templateFooterContent)
        .map(_._2.utf8ContentAsString)
        .mkString("")
      val content =
        generateContentFromTemplate(
          pathingArtifact.scopeId,
          in.processorName,
          in.templateContent.map(content => in.workspaceSrcRoot.relativize(content._1)).head,
          classLoader,
          objects,
          in.configuration,
          templateWithHeaderAndFooter
        )
      Right(in.installLocation -> content)
    } catch {
      case NonFatal(ex) =>
        log.debug(s"[${pathingArtifact.scopeId}:${in.processorName}] $ex")
        Left(CompilationMessage.error(s"[${in.processorName}] ${ex.getMessage}"))
    } finally classLoader.close()
  }

  protected def generateContentFromTemplate(
      scopeId: ScopeId,
      processorName: String,
      templateFile: RelativePath,
      classLoader: ClassLoader,
      objects: Seq[Any],
      configuration: Map[String, String],
      templateWithHeaderAndFooter: String): String
}

object TemplatingProcessor extends Log {
  final case class Inputs(
      workspaceSrcRoot: Directory,
      processorName: String,
      templateContent: Option[(FileAsset, HashedContent)],
      templateHeaderContent: Option[(FileAsset, HashedContent)],
      templateFooterContent: Option[(FileAsset, HashedContent)],
      objectsContent: Option[(FileAsset, HashedContent)],
      installLocation: RelativePath,
      configuration: Map[String, String],
      fingerprintHash: String
  ) extends ScopeProcessor.Inputs

}
