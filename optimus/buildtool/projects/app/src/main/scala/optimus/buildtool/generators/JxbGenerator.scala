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
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform._
import optimus.platform.util.xml.XmlLoader

@entity class JxbGenerator extends SourceGenerator {
  override val generatorType: String = "jxb"

  override type Inputs = JxbGenerator.Inputs

  import JxbGenerator._

  override def templateType(configuration: Map[String, String]): Directory.PathFilter = JxbGenerator.SourcePredicate

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): JxbGenerator.Inputs = JxbGenerator.Inputs(templates.toFiles, templates.hashFingerprint(Map.empty, Nil))

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: JxbGenerator.Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite { tempJar =>
    import inputs._

    val messageBuilder = List.newBuilder[CompilationMessage]
    val infos = files.content().flatMap { case (f, c) =>
      delegateInfo(scopeId, f, c) match {
        case Left(message) => messageBuilder += message; Nil
        case Right(infos)  => infos
      }
    }
    val sourcePath = SourceGenerator.SourcePath
    writer.streamToArtifact(tempJar, messageBuilder.result(), sourcePath) { jarStream =>
      infos.foreach { info =>
        jarStream.writeFile(info.source, sourcePath.resolvePath(info.name))
      }
    }
  }

}

object JxbGenerator {
  private val SourcePredicate = Directory.fileExtensionPredicate("jxb")

  final case class Inputs(
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact
  ) extends SourceGenerator.Inputs

  final case class DelegateInfo(name: RelativePath, source: String)

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
      val root = XmlLoader.load(content.contentAsInputStream)
      Right((root \ "MessageFlowDef" \ "Nodes" \ "Node").flatMap(parseNode))
    } catch {
      case e @ (_: IOException | _: SAXParseException) =>
        Left(CompilationMessage(None, s"[$id:jxb] cannot parse $filename: $e", CompilationMessage.Error))
    }

  }
}
