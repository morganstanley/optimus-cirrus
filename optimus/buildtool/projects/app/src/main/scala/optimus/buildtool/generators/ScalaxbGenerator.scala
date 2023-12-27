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
import java.nio.file.Files

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log
import scalaxb.compiler.ConfigEntry._
import scalaxb.compiler._
import scalaxb.compiler.xsd.Driver

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.xml.Elem
import scala.xml.Node

@entity class ScalaxbGenerator(workspaceSourceRoot: Directory) extends SourceGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.Scalaxb

  override type Inputs = ScalaxbGenerator.Inputs

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val filePredicate = sourceFilter && ScalaxbGenerator.SourcePredicate

    val (templateFiles, templateFingerprint) = SourceGenerator.templates(
      internalFolders,
      externalFolders,
      filePredicate,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )

    val pkg = configuration("package")
    val wrappedComplexTypes = configuration.get("wrappedComplexTypes").map(_.split(",").toIndexedSeq).getOrElse(Nil)
    val generateRuntime = configuration.get("generateRuntime").exists(_.toBoolean)
    val ignoreUnknown = configuration.get("ignoreUnknown").exists(_.toBoolean)

    val fingerprint =
      Seq(s"[scalaxb]${SourceGenerator.location[Driver].pathFingerprint}") ++
        templateFingerprint ++
        configuration.to(Seq).sorted.map { case (k, v) => s"[Configuration:$k]$v" }
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint)

    ScalaxbGenerator.Inputs(
      name,
      pkg,
      templateFiles,
      wrappedComplexTypes,
      generateRuntime,
      ignoreUnknown,
      fingerprintHash
    )
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templateFiles.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    ObtTrace.traceTask(scopeId, GenerateSource) {
      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
        val outputDir = tempDir.resolveDir(ScalaxbGenerator.SourcePath)
        Utils.createDirectories(outputDir)

        val cfg = {
          val cfg = Config.default
            .update(PackageNames(Map(None -> Some(pkg))))
            .update(GeneratePackageDir)
            .update(NamedAttributes)
            .update(Outdir(outputDir.path.toFile))
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
        val generatedFiles = driver.processReaders(templateFiles.toSeq, cfg)._2

        val sourcePath = ScalaxbGenerator.SourcePath
        val message =
          CompilationMessage(
            None,
            s"scalaxb[$generatorName]: ${generatedFiles.size} file(s) generated in package $pkg",
            CompilationMessage.Info
          )
        val a = GeneratedSourceArtifact.create(
          scopeId,
          generatorName,
          artifactType,
          outputJar,
          sourcePath,
          Seq(message)
        )
        SourceGenerator.createJar(generatorName, sourcePath, a.messages, a.hasErrors, tempJar, tempDir)()
        a
      }
      Some(artifact)
    }
  }
}

object ScalaxbGenerator extends Log {
  private val SourcePath = RelativePath("src")
  private val SourcePredicate = Directory.fileExtensionPredicate("xsd")

  final case class Inputs(
      generatorName: String,
      pkg: String,
      templateFiles: SortedMap[FileAsset, HashedContent],
      wrappedComplexTypes: Seq[String],
      generateRuntime: Boolean,
      ignoreUnknown: Boolean,
      fingerprintHash: String
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
