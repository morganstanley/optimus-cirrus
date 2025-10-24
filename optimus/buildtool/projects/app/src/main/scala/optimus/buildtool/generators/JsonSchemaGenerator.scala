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

import com.sun.codemodel.JCodeModel
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.generators.JaxbGenerator.ObtCodeWriter
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedFiles.Template
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.platform.util.Log
import optimus.platform._
import org.jsonschema2pojo.AnnotationStyle
import org.jsonschema2pojo.Jackson2Annotator
import org.jsonschema2pojo.SchemaGenerator
import org.jsonschema2pojo.SchemaMapper
import org.jsonschema2pojo.SchemaStore
import org.jsonschema2pojo.SourceType
import org.jsonschema2pojo.rules.RuleFactory

@entity class JsonSchemaGenerator extends SourceGenerator {
  override val generatorType: String = "json-schema"

  override type Inputs = JsonSchemaGenerator.Inputs
  import JsonSchemaGenerator._

  override def templateType(configuration: Map[String, String]): PathFilter = Directory.fileExtensionPredicate("json")

  @async protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {

    val schemaSourceType = JsonSchema2PojoSource.parse(configuration.get("sourceType"))

    val pkg = configuration.get("targetPackage")

    val extraFingerprint = Seq(s"[JsonSchema2Pojo]${GeneratorUtils.location[JCodeModel].pathFingerprint}")

    val fingerprintHash = templates.hashFingerprint(configuration, extraFingerprint)

    JsonSchemaGenerator.Inputs(
      sourceType = schemaSourceType,
      pkg = pkg,
      files = templates.toFiles,
      fingerprintHash,
      configuration
    )
  }

  @async def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { context =>
    import inputs._

    val model = createJsonSchemaCompiler()

    val config = new JsonSchemaConfiguration(configuration)
    val ruleFactory = new RuleFactory(config, new Jackson2Annotator(config), new SchemaStore())
    ruleFactory.setLogger(new JsonSchema2PojoLogger(log))

    val mapper = new SchemaMapper(ruleFactory, new SchemaGenerator())

    files.content(Template).foreach { case (f, c) =>
      val jsonContent = c.utf8ContentAsString

      // if configuration.useTitleAsClassname=true, we don't pass any class name; otherwise use the file name as class name
      val className =
        if (config.isUseTitleAsClassname) "" else f.asFile.name.split("\\.").headOption.getOrElse("")
      mapper.generate(model, className, pkg.getOrElse("placeholder.package"), jsonContent)
    }

    val codeWriter = new ObtCodeWriter(context.outputDir.path.toFile, "UTF-8")
    model.build(codeWriter)

    context.createArtifact(Nil)
  }

  protected def createJsonSchemaCompiler(): JCodeModel = new JCodeModel()
}

object JsonSchemaGenerator extends Log {

  private[buildtool] def extractBoolean(config: Map[String, String], key: String, default: => Boolean): Boolean =
    extract[Boolean](config, key, JsonSchema2PojoSource.parseBooleanConfig, default)

  private[buildtool] def extractString(config: Map[String, String], key: String, default: => String): String =
    extract[String](config, key, JsonSchema2PojoSource.parseStringConfig, default)

  private[buildtool] def extract[T](
      config: Map[String, String],
      key: String,
      extractor: (Option[String], T) => T,
      default: => T): T = extractor(config.get(key), default)

  final case class Inputs(
      sourceType: SourceType,
      pkg: Option[String],
      files: SandboxedFiles,
      fingerprint: FingerprintArtifact,
      configuration: Map[String, String]
  ) extends SourceGenerator.Inputs

  sealed trait JsonSchema2PojoSource
  object JsonSchema2PojoSource {
    def parse(typeStr: Option[String]): SourceType = typeStr match {
      case Some("yamlschema" | "yaml") =>
        throw new IllegalArgumentException("yaml sources not currently supported for jsonschema2pojo")
      case _ => SourceType.JSONSCHEMA
    }

    def parseBooleanConfig(cfgStr: Option[String], defaultValue: Boolean): Boolean = cfgStr match {
      case Some("true" | "false") => cfgStr.get.toBoolean
      case _                      => defaultValue
    }

    def parseStringConfig(cfgStr: Option[String], defaultValue: String): String = cfgStr match {
      case Some(_) => cfgStr.get
      case _       => defaultValue
    }

    def parseAnnotationStrConfig(cfgStr: Option[String], defaultValue: AnnotationStyle): AnnotationStyle =
      cfgStr match {
        case Some("gson") => AnnotationStyle.GSON
        case Some("none") => AnnotationStyle.NONE
        case _            => defaultValue
      }

    def parseSourceTypeStrConfig(cfgStr: Option[String], defaultValue: SourceType): SourceType =
      cfgStr match {
        case Some("yamlschema" | "yaml") =>
          throw new IllegalArgumentException("yaml sources not currently supported for jsonschema2pojo")
        case Some("jsonschema" | "json") => SourceType.JSONSCHEMA
        case _                           => defaultValue
      }
  }
}
