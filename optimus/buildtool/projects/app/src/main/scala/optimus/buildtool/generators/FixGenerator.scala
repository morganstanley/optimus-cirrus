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
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.generators.FixGenerator.FIX_GENERATOR_VERSION
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.platform._
import optimus.platform.util.Log
import org.w3c.dom.Document
import org.w3c.dom.Element

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Path
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.stream.StreamSource
import scala.collection.mutable
import scala.util.control.NonFatal

@entity class FixGenerator extends SourceGenerator {
  override val generatorType: String = "fix"
  override type Inputs = FixGenerator.Inputs

  override def templateType(configuration: Map[String, String]): Directory.PathFilter = Directory.NoFilter

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {

    val extraFingerprint = Seq(s"[fix][transform:$FIX_GENERATOR_VERSION]")

    val fingerprintHash = templates.hashFingerprint(configuration, extraFingerprint)

    FixGenerator.Inputs(
      files = templates.toFiles,
      fingerprint = fingerprintHash,
      configuration = configuration
    )
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { writeContext =>
    import inputs._

    val allTemplates = files.files()
    val compilationMessages = mutable.Buffer[CompilationMessage]()

    allTemplates.foreach { f =>
      {
        try {
          val document = FixGenerator.getSpecificationDocument(f)

          val mpkg =
            configuration.getOrElse("messagePackage", throw FixGeneratorException("Message package is not defined!"))
          val fpkg =
            configuration.getOrElse("fieldPackage", throw FixGeneratorException("Field package is not defined!"))
          val messageDir = writeContext.outputDir.path.toAbsolutePath.resolve(mpkg.replaceAll("\\.", "/"))
          val fieldsDir = writeContext.outputDir.path.toAbsolutePath.resolve(fpkg.replaceAll("\\.", "/"))

          val ctx = FixCodeGeneratorContext(
            generatorId = generatorId,
            specification = f,
            messagePackage = mpkg,
            fieldPackage = fpkg,
            overwrite = configuration.getOrElse("overwrite", "true").toBoolean,
            orderedFields = configuration.getOrElse("orderedFields", "true").toBoolean,
            useDecimal = configuration.getOrElse("orderedFields", "false").toBoolean,
            outputDir = writeContext.outputDir,
            document = document,
            messageDir = messageDir,
            fieldsDir = fieldsDir,
            compilationMessages = compilationMessages
          )

          compilationMessages.addOne(
            CompilationMessage(None, s"Generating FIX packages ${mpkg}, ${fpkg}.", CompilationMessage.Info))

          FixGenerator.generateFixCode(ctx)
        } catch {
          case e: FixGeneratorException =>
            compilationMessages.addOne(CompilationMessage.error(e.getMessage))
          case NonFatal(t) =>
            compilationMessages.addOne(CompilationMessage.error(t))
        }
      }
    }
    writeContext.createArtifact(compilationMessages.toSeq)
  }

}

final case class FixCodeGeneratorContext(
    generatorId: String,
    specification: FileAsset,
    messagePackage: String,
    fieldPackage: String,
    overwrite: Boolean,
    orderedFields: Boolean,
    useDecimal: Boolean,
    outputDir: Directory,
    document: Document,
    messageDir: Path,
    fieldsDir: Path,
    compilationMessages: mutable.Buffer[CompilationMessage]
)

final case class FixGeneratorException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

object FixGenerator extends Log {

  private val XSLPARAM_SERIAL_UID = "serialVersionUID"

  private val SERIAL_UID_STR = "20050617"

  private val UTF8 = "UTF-8"

  private val FIX_GENERATOR_VERSION = 1

  final case class Inputs(
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact,
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs

  def generateFixCode(ctx: FixCodeGeneratorContext): Unit = {
    generateFieldClasses(ctx)
    generateMessageBaseClass(ctx)
    generateMessageFactoryClass(ctx)
    generateMessageCrackerClass(ctx)
    generateComponentClasses(ctx)
    generateMessageSubclasses(ctx)
  }

  private def generateFieldClasses(ctx: FixCodeGeneratorContext): Unit = {
    log.debug(s"${ctx.generatorId}: generating field classes")

    val fieldNames = getNames(ctx.document.getDocumentElement(), "fields/field")

    val transformer = createTransformer("Fields.xsl")
    for (fieldName <- fieldNames) {
      val outputFile = ctx.fieldsDir.resolve(fieldName + ".java").toFile

      if (ctx.overwrite || !outputFile.exists()) {
        log.debug(s"field: ${fieldName}")

        val parameters = mutable.Map[String, String](
          "fieldName" -> fieldName,
          "fieldPackage" -> ctx.fieldPackage,
          XSLPARAM_SERIAL_UID -> SERIAL_UID_STR
        )
        if (ctx.useDecimal) {
          parameters += ("decimalType" -> "java.math.BigDecimal")
          parameters += ("decimalConverter" -> "Decimal")
        }
        generateCodeFile(ctx.document, parameters.toMap, outputFile, transformer, ctx.overwrite)
      }
    }
  }

  private def generateMessageBaseClass(ctx: FixCodeGeneratorContext): Unit = {
    log.debug("${ctx.name}: generating message base class")
    val parameters = Map[String, String](
      XSLPARAM_SERIAL_UID -> SERIAL_UID_STR
    )

    generateClassCode(ctx, "Message", parameters)
  }

  private def generateMessageFactoryClass(ctx: FixCodeGeneratorContext): Unit = {
    generateClassCode(ctx, "MessageFactory", Map.empty)
  }

  private def generateMessageCrackerClass(ctx: FixCodeGeneratorContext): Unit = {
    generateClassCode(ctx, "MessageCracker", Map.empty)
  }

  private def generateClassCode(
      ctx: FixCodeGeneratorContext,
      className: String,
      parameters: Map[String, String]): Unit = {
    log.debug(s"generating ${className} for ${ctx.generatorId}")
    val params = Map[String, String](
      "messagePackage" -> ctx.messagePackage,
      "fieldPackage" -> ctx.fieldPackage
    ) ++ parameters

    val outputFile = ctx.messageDir.resolve(className + ".java").toFile

    generateCodeFile(
      ctx.document,
      params,
      outputFile,
      createTransformer(className + ".xsl"),
      ctx.overwrite
    )
  }

  private def generateComponentClasses(ctx: FixCodeGeneratorContext): Unit = {
    log.debug("${ctx.name}: generating component classes")

    val componentsOutputDir = ctx.messageDir.resolve("component")

    val componentNames = getNames(ctx.document.getDocumentElement(), "components/component")

    val transformer = createTransformer("MessageSubclass.xsl")
    for (componentName <- componentNames) {
      val outputFile = componentsOutputDir.resolve(componentName + ".java").toFile

      log.debug(s"component: ${componentName}")

      val parameters = Map[String, String](
        "itemName" -> componentName,
        "baseClass" -> "quickfix.MessageComponent",
        "subpackage" -> ".component",
        "fieldPackage" -> ctx.fieldPackage,
        "messagePackage" -> ctx.messagePackage,
        "orderedFields" -> ctx.orderedFields.toString,
        XSLPARAM_SERIAL_UID -> SERIAL_UID_STR
      )
      generateCodeFile(ctx.document, parameters, outputFile, transformer, ctx.overwrite)
    }
  }

  private def generateMessageSubclasses(ctx: FixCodeGeneratorContext): Unit = {
    log.debug("${ctx.name}: generating message subclasses")

    val messageNames = getNames(ctx.document.getDocumentElement(), "messages/message")

    val transformer = createTransformer("MessageSubclass.xsl")
    for (messageName <- messageNames) {
      val outputFile = ctx.messageDir.resolve(messageName + ".java").toFile

      log.debug(s"generating message class: ${messageName}")

      val parameters = Map[String, String](
        "itemName" -> messageName,
        "fieldPackage" -> ctx.fieldPackage,
        "messagePackage" -> ctx.messagePackage,
        "orderedFields" -> ctx.orderedFields.toString,
        XSLPARAM_SERIAL_UID -> SERIAL_UID_STR
      )
      generateCodeFile(ctx.document, parameters, outputFile, transformer, ctx.overwrite)
    }
  }

  private def createTransformer(transformFile: String) = {
    val styleSource = new StreamSource(
      getClass.getResourceAsStream("/optimus/buildtool/generators/fix/" + transformFile))
    val transformerFactory = TransformerFactory.newInstance
    transformerFactory.newTransformer(styleSource)
  }

  private def getSpecificationDocument(f: FileAsset): Document = {
    val factory = DocumentBuilderFactory.newInstance
    val builder = factory.newDocumentBuilder
    builder.parse(new File(f.pathString))
  }

  private def getNames(element: Element, path: String): List[String] = getNames(element, path, mutable.ListBuffer())

  private def getNames(element: Element, path: String, names: mutable.ListBuffer[String]): List[String] = {
    val separatorOffset = path.indexOf('/')
    if (separatorOffset == -1) {
      val fieldNodeList = element.getElementsByTagName(path)
      var i = 0
      while (i < fieldNodeList.getLength) {
        names.addOne(fieldNodeList.item(i).asInstanceOf[Element].getAttribute("name"))

        i += 1
      }
    } else {
      val tag = path.substring(0, separatorOffset)
      val subnodes = element.getElementsByTagName(tag)
      var i = 0
      while (i < subnodes.getLength) {
        getNames(subnodes.item(i).asInstanceOf[Element], path.substring(separatorOffset + 1), names)

        i += 1
      }
    }
    names.toList
  }

  private def generateCodeFile(
      document: Document,
      parameters: Map[String, String],
      outputFile: File,
      transformer: Transformer,
      overwrite: Boolean): Unit = {

    for ((key, value) <- parameters) {
      transformer.setParameter(key, value)
    }

    if (!outputFile.getParentFile.exists && !outputFile.getParentFile.mkdirs)
      throw FixGeneratorException("Could not create " + outputFile.getParentFile)

    if (overwrite || !outputFile.exists) {
      val source = new DOMSource(document)
      val output = new BufferedOutputStream(new FileOutputStream(outputFile))
      try {
        val result = new StreamResult(output)
        transformer.transform(source, result)
      } finally output.close()
    }
  }
}
