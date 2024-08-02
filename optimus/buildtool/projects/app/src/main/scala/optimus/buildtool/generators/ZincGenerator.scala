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

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.builders.postbuilders.installer.Installer.writeFile
import optimus.buildtool.compilers.zinc.ZincClasspathResolver
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.compat._
import java.nio.file.Files
import scala.collection.immutable.Seq

@entity class ZincGenerator(zincResolver: ZincClasspathResolver, depCopyRoot: Directory) extends SourceGenerator {
  import optimus.buildtool.generators.ZincGenerator._

  override type Inputs = ZincGenerator.Inputs

  override def artifactType: GeneratedSourceArtifactType = ArtifactType.Zinc

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope): Inputs = {
    val zincGroup = configuration.get("group")
    val zincName = configuration.get("name")
    val version = configuration.get("version")
    // should re-resolve zinc libs when zinc dep in dependencies.obt changed or depCopyRoot changed
    val predefinedZincDeps =
      zincResolver.findZincDep(zincGroup.getOrElse(""), zincName.getOrElse("")).mkString(", ")
    val fingerprint = Seq(
      s"[ZINC]$zincGroup:$zincName:$version, depCopyDir:${depCopyRoot.pathString} predefined:$predefinedZincDeps")
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint, Some(name))
    ZincGenerator.Inputs(zincGroup, zincName, version, fingerprintHash)
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().name.nonEmpty && inputs().group.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset): Option[GeneratedSourceArtifact] = ObtTrace.traceTask(scopeId, GenerateSource) {
    val zincInputs = inputs()
    val zincJars: Map[String, Seq[JarAsset]] =
      zincInputs.name match {
        case Some(zincName) =>
          scalaVersions.apar.map { scalaVer =>
            // these are dep copied jars, when install the class jar we will convert them to disted paths
            val zincJars = zincResolver.resolveZincJars(scalaVer, zincInputs.group, zincInputs.name, zincInputs.version)
            zincDepsFileName(scalaVer) -> zincJars
          }.toMap
        case None =>
          log.warn("zinc generator is missing a name!")
          Map.empty
      }

    val artifact = Utils.atomicallyWrite(outputJar, replaceIfExists = true) { tempOut =>
      val tempJar = JarAsset(tempOut)
      // Use a short temp dir name to avoid issues with too-long paths for generated .java files
      val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
      val srcPath = RelativePath(ZincSourceDir)
      val outputDir = tempDir.resolveDir(srcPath)
      Utils.createDirectories(outputDir)
      zincJars.foreach { case (zincDepsFileName, jars) =>
        writeFile(outputDir.resolveFile(zincDepsFileName), jars.map(_.pathString))
      }

      val msgs =
        if (zincJars.nonEmpty)
          Seq(CompilationMessage(None, resolveZincMsg(zincInputs.id, zincJars), CompilationMessage.Info))
        else Seq(CompilationMessage(None, "zinc dependencies generation failed", CompilationMessage.Error))
      val a = GeneratedSourceArtifact.create(
        scopeId,
        zincInputs.generatorName,
        artifactType,
        outputJar,
        srcPath,
        msgs
      )
      SourceGenerator.createJar(zincInputs.generatorName, srcPath, a.messages, a.hasErrors, tempJar, tempDir)()
      a
    }
    Some(artifact)
  }
}

object ZincGenerator extends Log {
  val scalaVersions = Seq("2.11", "2.12", "2.13")
  val ZincGeneratorName = "zinc"
  val ZincSourceDir = "src"

  def zincDepsFilePath(scalaVersion: String) = s"/${zincDepsFileName(scalaVersion)}"

  def zincDepsFileName(scalaVersion: String) = s"zinc-deps-$scalaVersion.txt"

  def resolveZincMsg(id: String, zincJars: Map[String, Seq[JarAsset]]) = {
    val saved = zincJars
      .to(Seq)
      .sortBy(_._1)
      .map { case (name, jars) =>
        jars match {
          case Seq(jar) => s"$name (1 dependency)"
          case _        => s"$name (${jars.size} dependencies)"
        }
      }
      .mkString(", ")
    s"Resolved zinc $id: $saved"
  }

  final case class Inputs(
      group: Option[String],
      name: Option[String],
      version: Option[String],
      fingerprint: FingerprintArtifact,
      generatorName: String = ZincGeneratorName)
      extends SourceGenerator.Inputs {
    def id: String = s"${group.getOrElse("None")}.${name.getOrElse("None")}.${version.getOrElse("None")}"
  }
}
