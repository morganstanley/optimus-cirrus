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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform.node
import optimus.platform.util.Log
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.JaxbGenerator.ObtCodeWriter
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.platform.NodeFunction0
import optimus.platform.entity
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import org.jsonschema2pojo.AnnotationStyle
import org.jsonschema2pojo.Jackson2Annotator
import org.jsonschema2pojo.SchemaGenerator
import org.jsonschema2pojo.SchemaMapper
import org.jsonschema2pojo.SchemaStore
import org.jsonschema2pojo.SourceType
import org.jsonschema2pojo.rules.RuleFactory

import java.nio.file.Files
import scala.collection.immutable.SortedMap

@entity class JsonSchemaGenerator(directoryFactory: DirectoryFactory, workspaceSourceRoot: Directory)
    extends SourceGenerator {
  override val generatorType: String = "json-schema"

  override type Inputs = JsonSchemaGenerator.Inputs
  import JsonSchemaGenerator._

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {

    val schemaSourceType = JsonSchema2PojoSource.parse(configuration.get("sourceType"))

    val filter = sourceFilter && Directory.fileExtensionPredicate("json")

    val (templateFiles, templateFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )

    val pkg = configuration.get("targetPackage")

    val fingerprint =
      s"[JsonSchema2Pojo]${SourceGenerator.location[JCodeModel].pathFingerprint}" +: (configuration.map { case (k, v) =>
        s"[Config]$k=$v"
      }.toIndexedSeq ++ templateFingerprint)

    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint, Some(name))

    JsonSchemaGenerator.Inputs(
      generatorName = name,
      sourceType = schemaSourceType,
      pkg = pkg,
      sourceFiles = templateFiles,
      fingerprintHash,
      configuration
    )
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().sourceFiles.map(_._2).merge.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val allTemplates = sourceFiles.map(_._2).merge

    ObtTrace.traceTask(scopeId, GenerateSource) {
      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
        val outputDir = tempDir.resolveDir(JsonSchemaGenerator.SourcePath)
        Utils.createDirectories(outputDir)

        val model = createJsonSchemaCompiler()

        if (model != null) {
          val config = new JsonSchemaConfiguration(configuration)
          val ruleFactory = new RuleFactory(config, new Jackson2Annotator(config), new SchemaStore())
          ruleFactory.setLogger(new JsonSchema2PojoLogger(log))

          val mapper = new SchemaMapper(ruleFactory, new SchemaGenerator())

          allTemplates.foreach { case (f, c) =>
            val jsonContent = c.utf8ContentAsString

            // if configuration.useTitleAsClassname=true, we don't pass any class name; otherwise use the file name as class name
            val className =
              if (config.isUseTitleAsClassname) "" else f.asFile.name.split("\\.").headOption.getOrElse("")
            mapper.generate(model, className, pkg.getOrElse("placeholder.package"), jsonContent)
          }

          val writer = new ObtCodeWriter(outputDir.path.toFile, "UTF-8")
          model.build(writer)
          log.info(s"[$scopeId:$generatorName] Jsonschema2pojo generation successful")
        } else
          log.error(s"[$scopeId:$generatorName] Jsonschema2pojo generation failed")

        val artifact = GeneratedSourceArtifact.create(
          scopeId,
          tpe,
          generatorName,
          outputJar,
          JsonSchemaGenerator.SourcePath,
          Seq.empty
        )
        SourceGenerator.createJar(
          tpe,
          generatorName,
          JsonSchemaGenerator.SourcePath,
          artifact.messages,
          artifact.hasErrors,
          tempJar,
          tempDir)()
        artifact
      }
      Some(artifact)
    }
  }

  protected def createJsonSchemaCompiler(): JCodeModel = new JCodeModel()
}

object JsonSchemaGenerator extends Log {
  private[buildtool] val SourcePath = RelativePath("src")

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
      generatorName: String,
      sourceType: SourceType,
      pkg: Option[String],
      sourceFiles: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
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
        case _            => AnnotationStyle.JACKSON
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
