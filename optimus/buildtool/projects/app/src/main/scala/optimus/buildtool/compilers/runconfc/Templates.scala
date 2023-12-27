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
package optimus.buildtool.compilers.runconfc

import java.nio.file.Paths

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.files.RelativePath
import optimus.buildtool.runconf.compile.InputFile
import optimus.buildtool.runconf.plugins.ScriptTemplates
import optimus.buildtool.scope.sources.RunconfCompilationSources.appScriptsFolderName
import optimus.platform._

import scala.collection.immutable.Seq

object Templates {
  val disabledTemplateSpec: String = "disabled"
  val defaultTemplateSpec: String = "default"

  val linuxTemplateName: String = "linux"
  val windowsTemplateName: String = "windows"
  val dockerTemplateName: String = "docker"

  private val defaultScriptTemplates: Map[String, String] = Map(
    linuxTemplateName -> defaultTemplateSpec,
    windowsTemplateName -> defaultTemplateSpec,
    dockerTemplateName -> disabledTemplateSpec
  )

  val templateFileExtensions: Seq[String] = Seq("txt", "template")

  private val unixTemplateFileNames: Seq[String] =
    templateFileExtensions.map(extension => s"unixStartScript.$extension")
  private val windowsTemplateFileNames: Seq[String] =
    templateFileExtensions.map(extension => s"windowsStartScript.$extension")
  private val dockerTemplateFileNames: Seq[String] =
    templateFileExtensions.map(extension => s"dockerStartScript.$extension")

  private def isMatchingPlatformTemplateName(template: InputFile, names: Seq[String]): Boolean =
    names.exists(template.origin.toString.endsWith(_))

  @node def defaultTemplates(templates: Seq[InputFile]): Seq[TemplateDescription] = templates.collect {
    case templateInput if isMatchingPlatformTemplateName(templateInput, unixTemplateFileNames) =>
      TemplateDescription(linuxTemplateName, templateInput, "")
    case templateInput if isMatchingPlatformTemplateName(templateInput, windowsTemplateFileNames) =>
      TemplateDescription(windowsTemplateName, templateInput, ".bat", "\r\n")
    case templateInput if isMatchingPlatformTemplateName(templateInput, dockerTemplateFileNames) =>
      TemplateDescription(dockerTemplateName, templateInput, ".dckr.sh")
  }

  def linuxShellVariableNameWrapper(varName: String): String = s"$${$varName}"
  def windowsBatchVariableNameWrapper(varName: String): String = s"%$varName%"

  def isDisabledTemplate(templateSpec: String): Boolean =
    templateSpec == disabledTemplateSpec || templateSpec.isEmpty

  def potentialLocationsFromScopeConfigDir: Seq[RelativePath] =
    Seq(RelativePath(Paths.get(appScriptsFolderName)))

  @node def getTemplateDescriptions(
      templates: Seq[InputFile],
      runConfName: String,
      scriptTemplates: ScriptTemplates,
      useDefaultTemplates: Boolean = true): Seq[Either[TemplateDescription, CompilationMessage]] = {
    val supportedTemplates = Templates.defaultTemplates(templates)
    val combinedTemplates: Map[String, String] =
      (if (useDefaultTemplates) defaultScriptTemplates else Map.empty[String, String]) ++ scriptTemplates.templates
    combinedTemplates.toMap.map {
      case (platform, templateSpec) if isDisabledTemplate(templateSpec) =>
        // Disabled use-case
        Right(
          CompilationMessage.message(
            s"RunConf '$runConfName': skipping template '$platform' generation since it is marked as disabled",
            CompilationMessage.Info))

      case (platform, templateResourceName)
          if supportedTemplates.exists(_.name == platform) && templateResourceName == defaultTemplateSpec =>
        // Default template use-case
        Left(supportedTemplates.find(_.name == platform).get)

      case (platform, templateResourceName) if supportedTemplates.exists(_.name == platform) =>
        templates.find(_.origin.toString.endsWith(templateResourceName)) match {
          case Some(template) =>
            Left(
              supportedTemplates
                .find(_.name == platform)
                .get
                .copy(templateInput = template))
          case None =>
            Right(
              CompilationMessage.message(
                s"RunConf '$runConfName': template '$templateResourceName' cannot be found in the sources (src/$appScriptsFolderName, src/<meta>/<bundle>/projects/<project>/$appScriptsFolderName)",
                CompilationMessage.Error
              ))
        }

      case (customUseCase, _) =>
        Right(
          CompilationMessage.message(
            s"RunConf '$runConfName': custom template '$customUseCase' generation is not supported",
            CompilationMessage.Error))
    }.toList
  }
}
