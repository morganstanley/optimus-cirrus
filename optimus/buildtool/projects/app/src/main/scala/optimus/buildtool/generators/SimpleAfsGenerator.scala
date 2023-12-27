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
import optimus.buildtool.artifacts.GeneratedSourceArtifact
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

import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

/**
 * A source generator that generates source by running a binary from AFS over the templates.
 *
 * This is for cases where the source generation is entirely on local templates without templates from upstream.
 * Basically SimpleAfsGenerator generates source by running a binary like `generator --some-args $TEMPLATES` where
 * $TEMPLATES is the list of templates file. The executable can be different based on the OS.
 *
 * TODO (OPTIMUS-53476): Check if this is needed still.
 */
@entity trait SimpleAfsGenerator extends SourceGenerator {

  def workspaceSourceRoot: Directory
  def generatorDefaults: PortableAfsExecutable
  def generatorExecutableNameForLog: String
  def sourcePredicate: PathFilter
  def sourcePath: RelativePath
  override type Inputs = SimpleAfsGenerator.Inputs

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): SimpleAfsGenerator.Inputs = {
    // get the executable that we are running
    val exec = generatorDefaults.configured(configuration)
    val version = exec.dependencyDefinition(scope).version

    // actual executable
    val generatorExecutable = exec.file(version)

    // Linux defaults are used for the fingerprint to avoid contaminating it with local execution information. This of
    // course relies on the generators producing always the same results, even when running with different binaries on
    // different systems.
    val generatorFingerprint = generatorDefaults.linux.file(version)

    val filter = sourceFilter && sourcePredicate
    val (templates, templateFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )
    val fingerprint = (s"[$generatorExecutableNameForLog]" + generatorFingerprint.pathString) +: templateFingerprint
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint)

    SimpleAfsGenerator.Inputs(name, generatorExecutable, templates, fingerprintHash)
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[SimpleAfsGenerator.Inputs]): Boolean =
    inputs().templates.flatMap(_._2).nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = {

    val resolvedInputs = inputs()
    import resolvedInputs._

    if (templates.flatMap(_._2).nonEmpty) {
      ObtTrace.traceTask(scopeId, GenerateSource) {

        val cmd = resolvedInputs.executable.path.toString

        val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
          val tempJar = JarAsset(tempOut)
          // Use a short temp dir name to avoid issues with too-long paths for generated .java files
          val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
          val outputDir = tempDir.resolveDir(sourcePath)
          Utils.createDirectories(outputDir)

          val templateFiles = templates.apar.flatMap { case (root, files) =>
            val validated = SourceGenerator.validateFiles(root, files)
            validated.files
          }

          val cmdLine = cmd +: computeArgs(outputDir, templateFiles)
          log.debug(
            s"[$scopeId:$generatorName] ${generatorExecutableNameForLog.capitalize} command line: ${cmdLine.mkString(" ")}")
          var logging = mutable.Buffer[String]()
          // Note: this is a blocking call. See Optimus-49290 if this gets too slow.
          val ret = Process(cmdLine) ! ProcessLogger { s =>
            val line = s"[$scopeId:$generatorName:$generatorExecutableNameForLog] $s"
            logging += line
            log.debug(line)
          }

          val messages = if (ret == 0) Nil else logging.map(s => CompilationMessage(None, s, CompilationMessage.Error))
          val a = GeneratedSourceArtifact.create(
            scopeId,
            generatorName,
            artifactType,
            outputJar,
            sourcePath,
            messages.toIndexedSeq
          )
          SourceGenerator.createJar(generatorName, sourcePath, a.messages, a.hasErrors, tempJar, tempDir)()
          a
        }
        Some(artifact)
      }
    } else None
  }

  /**
   * This method must compute command line arguments to pass to the executable in order.
   * @param outputDir
   *   the output directory relative to which to generate the output files
   * @param files
   *   input files (e.g. schema files) to use as input to the code generation
   * @return
   *   the command line as a Seq[String] with executable path as the starting element followed by command line arguments
   *   to pass to the executable in order.
   */
  @node protected def computeArgs(outputDir: Directory, files: Seq[FileAsset]): Seq[String]

}

object SimpleAfsGenerator {
  final case class Inputs(
      generatorName: String,
      executable: FileAsset,
      templates: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      fingerprintHash: String
  ) extends SourceGenerator.Inputs
}
