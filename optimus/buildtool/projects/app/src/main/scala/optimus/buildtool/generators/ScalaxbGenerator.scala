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

import java.io.File
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform._
import optimus.platform.util.Log
import scalaxb.compiler.ConfigEntry._
import scalaxb.compiler._
import scalaxb.compiler.xsd.Driver

import scala.collection.compat._
import scala.xml.Elem
import scala.xml.Node

@entity class ScalaxbGenerator extends SourceGenerator {
  override val generatorType: String = "scalaxb"

  override type Inputs = ScalaxbGenerator.Inputs

  override def templateType(configuration: Map[String, String]): PathFilter = ScalaxbGenerator.SourcePredicate

  @async override protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): ScalaxbGenerator.Inputs = {

    val pkg = configuration("package")
    val wrappedComplexTypes = configuration.get("wrappedComplexTypes").map(_.split(",").toIndexedSeq).getOrElse(Nil)
    val generateRuntime = configuration.get("generateRuntime").exists(_.toBoolean)
    val ignoreUnknown = configuration.get("ignoreUnknown").exists(_.toBoolean)

    val extraFingerprint = Seq(s"[scalaxb]${GeneratorUtils.location[Driver].pathFingerprint}")
    val hashedFingerprint = templates.hashFingerprint(configuration, extraFingerprint)

    ScalaxbGenerator.Inputs(
      pkg,
      templates.toFiles,
      wrappedComplexTypes,
      generateRuntime,
      ignoreUnknown,
      hashedFingerprint
    )
  }

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: ScalaxbGenerator.Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { context =>
    import inputs._

    val cfg = {
      val cfg = Config.default
        .update(PackageNames(Map(None -> Some(pkg))))
        .update(GeneratePackageDir)
        .update(NamedAttributes)
        .update(Outdir(context.outputDir.path.toFile))
        .update(WrappedComplexTypes(wrappedComplexTypes.toList))
      val ops = Seq[Config => Config](
        if (ignoreUnknown) _.update(IgnoreUnknown) else identity,
        if (!generateRuntime) _.remove(GenerateRuntime) else identity
      )
      ops.foldLeft(cfg) { (c, op) => op(c) }
    }

    val driver = new Driver
    implicit val schema: CanBeRawSchema[(FileAsset, HashedContent), Node] = ScalaxbGenerator.Schema
    implicit val writer: CanBeWriter[File] = ScalaxbGenerator.fileWriter(driver, cfg)
    val generatedFiles = driver.processReaders(files.content().toSeq, cfg)._2

    val message =
      CompilationMessage(
        None,
        s"${generatedFiles.size} file(s) generated in package $pkg",
        CompilationMessage.Info
      )

    context.createArtifact(Seq(message))
  }
}

object ScalaxbGenerator extends Log {
  private val SourcePredicate = Directory.fileExtensionPredicate("xsd")

  final case class Inputs(
      pkg: String,
      files: SandboxedFiles,
      wrappedComplexTypes: Seq[String],
      generateRuntime: Boolean,
      ignoreUnknown: Boolean,
      fingerprint: FingerprintArtifact
  ) extends SourceGenerator.Inputs

  private val Schema: CanBeRawSchema[(FileAsset, HashedContent), Node] =
    new CanBeRawSchema[(FileAsset, HashedContent), Node] {
      override def toRawSchema(value: (FileAsset, HashedContent)): Elem = CustomXML.load(value._2.contentAsInputStream)
      override def toURI(value: (FileAsset, HashedContent)): URI = value._1.path.toUri
    }

  private def fileWriter(driver: Driver, config: Config) = new CanBeWriter[File] {
    override def toWriter(value: File) =
      new PrintWriter(new OutputStreamWriter(new java.io.FileOutputStream(value), StandardCharsets.UTF_8.displayName()))

    override def newInstance(packageName: Option[String], fileName: String): File = {
      val dir = if (config.packageDir) driver.packageDir(packageName, config.outdir) else config.outdir
      dir.mkdirs()
      new File(dir, fileName)
    }
  }

}
