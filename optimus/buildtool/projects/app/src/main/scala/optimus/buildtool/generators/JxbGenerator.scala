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
package optimus.buildtool
package generators

import java.io.IOException

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity class JxbGenerator(workspaceSourceRoot: Directory) extends SourceGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.Jxb

  override type Inputs = JxbGenerator.Inputs

  import JxbGenerator._

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: Directory.PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val filter = sourceFilter && JxbGenerator.SourcePredicate

    val (templateFiles, templateFingerprint) = SourceGenerator.templates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )

    val fingerprintHash = scope.hasher.hashFingerprint(templateFingerprint, ArtifactType.GenerationFingerprint)

    JxbGenerator.Inputs(name, templateFiles, fingerprintHash)
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templateFiles.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs0: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    val inputs = inputs0()
    import inputs._

    Some {
      ObtTrace.traceTask(scopeId, trace.GenerateSource) {
        Utils.atomicallyWrite(outputJar) { tmpJar =>
          val messageBuilder = List.newBuilder[CompilationMessage]
          val infos = templateFiles.flatMap { case (f, c) =>
            delegateInfo(scopeId, f, c) match {
              case Left(message) => messageBuilder += message; Nil
              case Right(infos)  => infos
            }
          }
          val a = GeneratedSourceArtifact.create(
            scopeId,
            generatorName,
            artifactType,
            outputJar,
            SourcePath,
            messageBuilder.result()
          )
          SourceGenerator.createJar(generatorName, SourcePath, a.messages, a.hasErrors, JarAsset(tmpJar)) { jarOut =>
            infos.foreach { info =>
              jarOut.writeFile(info.source, SourcePath resolvePath info.name)
            }
          }
          a
        }
      }
    }
  }

}

object JxbGenerator {
  private val SourcePath = RelativePath("src")
  private val SourcePredicate = Directory.fileExtensionPredicate("jxb")

  final case class Inputs(
      generatorName: String,
      templateFiles: SortedMap[FileAsset, HashedContent],
      fingerprintHash: String
  ) extends SourceGenerator.Inputs

  final case class DelegateInfo(name: RelativePath, source: String)
  object DelegateInfo {
    implicit val ordering: Ordering[DelegateInfo] = Ordering.by(_.name)
  }

  def delegateInfo(
      id: ScopeId,
      src: FileAsset,
      content: HashedContent
  ): Either[CompilationMessage, Seq[DelegateInfo]] = {
    import xml._

    lazy val filename = src.name

    def textattr(attr: String)(node: Node): Option[String] =
      node.attribute("name").collect { case Text(`attr`) =>
        node.text
      }

    def parseNode(node: Node): Option[DelegateInfo] = {
      val attrs = node \ "Attributes" \ "Attribute"

      val className = attrs.flatMap(textattr("delegateClass")).singleOption
      val classCode = attrs.flatMap(textattr("code")).singleOption

      // ^(className, classCode)(DelegateInfo.apply)
      PartialFunction.condOpt(className, classCode) { case (Some(className), Some(classCode)) =>
        val path = RelativePath(className.replace('.', '/') + ".java")
        DelegateInfo(path, classCode)
      }
    }

    try {
      val root = XML.load(content.contentAsInputStream)
      Right((root \ "MessageFlowDef" \ "Nodes" \ "Node").flatMap(parseNode))
    } catch {
      case e @ (_: IOException | _: SAXParseException) =>
        Left(CompilationMessage(None, s"[$id:jxb] cannot parse $filename: $e", CompilationMessage.Error))
    }

  }
}
