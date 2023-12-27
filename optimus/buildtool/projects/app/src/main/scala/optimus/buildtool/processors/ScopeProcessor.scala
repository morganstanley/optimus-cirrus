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
package optimus.buildtool.processors

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CachedMetadata
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.ProcessorArtifact
import optimus.buildtool.artifacts.ProcessorArtifactType
import optimus.buildtool.artifacts.ProcessorMetadata
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ProcessScope
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.AsyncUtils
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.platform._

import java.nio.file.Files
import scala.collection.immutable.Seq

@entity trait ScopeProcessor {
  import ScopeProcessor._

  def artifactType: ProcessorArtifactType
  def tpe: ProcessorType = ProcessorType(artifactType.name)

  type Inputs <: ScopeProcessor.Inputs

  @node def dependencies(scope: CompilationScope): Seq[Artifact] =
    scope.upstream.allCompileDependencies.apar.map(_.transitiveExternalDependencies) ++
      scope.upstream.signaturesForOurCompiler

  // Returned as a NodeFunction0 to prevent Inputs (which will often contain full sources) being inadvertently
  // cached as an argument to other @nodes
  final def inputs(
      name: String,
      templateFile: FileAsset,
      templateHeaderFile: Option[FileAsset],
      templateFooterFile: Option[FileAsset],
      objectsFile: Option[FileAsset],
      installLocation: RelativePath,
      configuration: Map[String, String],
      scope: CompilationScope,
      javaAndScalaSources: JavaAndScalaCompilationSources
  ): NodeFunction0[Inputs] =
    asNode(() =>
      _inputs(
        name,
        templateFile,
        templateHeaderFile,
        templateFooterFile,
        objectsFile,
        installLocation,
        configuration,
        scope,
        javaAndScalaSources))

  @node protected def _inputs(
      name: String,
      templateFile: FileAsset,
      templateHeaderFile: Option[FileAsset],
      templateFooterFile: Option[FileAsset],
      objectsFile: Option[FileAsset],
      installLocation: RelativePath,
      configuration: Map[String, String],
      scope: CompilationScope,
      javaAndScalaSources: JavaAndScalaCompilationSources
  ): Inputs

  @node def processInputs(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact],
      outputJar: JarAsset
  ): Option[ProcessorArtifact] = ObtTrace.traceTask(scopeId, ProcessScope) {
    val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
      // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
      val tempJar = new ConsistentlyHashedJarOutputStream(Files.newOutputStream(tempOut), None, compressed = true)
      val errorOrContent = generateContent(inputs, pathingArtifact, dependencyArtifacts)
      AsyncUtils.asyncTry {
        val msgs = errorOrContent match {
          case Right((installLocation, content)) =>
            tempJar.writeFile(content, baseRelPath.resolvePath(installLocation))
            Nil
          case Left(generationError) => Seq(generationError)
        }
        val a = ProcessorArtifact.create(scopeId, inputs().processorName, artifactType, outputJar, msgs)
        import optimus.buildtool.artifacts.JsonImplicits._
        AssetUtils.withTempJson(ProcessorMetadata(a.processorName, a.messages, a.hasErrors))(
          tempJar.writeFile(_, CachedMetadata.MetadataFile)
        )
        a
      } asyncFinally tempJar.close()
    }
    Some(artifact)
  }

  @node protected def content(file: FileAsset, scope: CompilationScope): Option[(FileAsset, HashedContent)] = {
    scope.directoryFactory
      .lookupSourceFolder(
        scope.config.paths.workspaceSourceRoot,
        file.parent
      )
      .findSourceFiles(Directory.exactMatchPredicate(file), maxDepth = 1)
      .map { case (_, content) =>
        file -> content
      }
      .singleOption
  }

  @node protected def generateContent(
      inputs: NodeFunction0[Inputs],
      pathingArtifact: PathingArtifact,
      dependencyArtifacts: Seq[Artifact]): Either[CompilationMessage, (RelativePath, String)]

  protected def loadAndInitializeClasses(objectFile: Option[HashedContent], classLoader: ClassLoader): Seq[Any] =
    objectFile.fold(Seq.empty[Any]) { file =>
      val classNames = file.utf8ContentAsString
        .split("\\n")
        .toList
        .filterNot { line =>
          val cleanLine = line.trim
          cleanLine.startsWith("#") || cleanLine.startsWith("//")
        }
      classNames.map { name =>
        val clazz = Class.forName(name, true, classLoader)
        if (clazz.isEnum) {
          // java enums are particular, as we cannot initialize them...
          // so cheating the system here a bit by initializing their first value
          clazz.getEnumConstants.head
        } else clazz.getDeclaredConstructor().newInstance()
      }
    }
}

object ScopeProcessor {
  val baseRelPath: RelativePath = RelativePath("processed")

  trait Inputs {
    def processorName: String
    def fingerprintHash: String
    def installLocation: RelativePath
  }

  @node def computeFingerprintHash(
      name: String,
      templateContent: Option[(FileAsset, HashedContent)],
      templateHeaderContent: Option[(FileAsset, HashedContent)],
      templateFooterContent: Option[(FileAsset, HashedContent)],
      objectsContent: Option[(FileAsset, HashedContent)],
      installLocation: RelativePath,
      configuration: Map[String, String],
      javaAndScalaSources: JavaAndScalaCompilationSources,
      scope: CompilationScope): String = {

    val fingerprint =
      fingerprintComponents(
        name,
        templateContent,
        templateHeaderContent,
        templateFooterContent,
        objectsContent,
        installLocation,
        configuration,
        javaAndScalaSources,
        scope.config.paths.workspaceSourceRoot
      )
    scope.hasher.hashFingerprint(fingerprint, ArtifactType.ProcessingFingerprint)
  }

  @node private def fingerprintComponents(
      name: String,
      templateContent: Option[(FileAsset, HashedContent)],
      templateHeaderContent: Option[(FileAsset, HashedContent)],
      templateFooterContent: Option[(FileAsset, HashedContent)],
      objectsContent: Option[(FileAsset, HashedContent)],
      installLocation: RelativePath,
      configuration: Map[String, String],
      javaAndScalaSources: JavaAndScalaCompilationSources,
      workspaceSourceRoot: Directory): Seq[String] = {
    val compilationFingerprint = s"[Sources]${javaAndScalaSources.compilationInputsHash}"
    val installLocationFingerprint = s"[InstallLocation:$name]${installLocation.pathFingerprint}"
    val configFingerprint = configuration.toSeq.sorted.map { case (k, v) => s"[Config:$name]$k=$v" }

    val templateFingerprints =
      fingerprintElement(s"Template:$name", templateContent, workspaceSourceRoot)
    val templateHeaderFingerprints =
      fingerprintElement(s"TemplateHeader:$name", templateHeaderContent, workspaceSourceRoot)
    val templateFooterFingerprints =
      fingerprintElement(s"TemplateFooter:$name", templateFooterContent, workspaceSourceRoot)
    val objectsFingerprint =
      fingerprintElement(s"Object:$name", objectsContent, workspaceSourceRoot)

    Seq(compilationFingerprint, installLocationFingerprint) ++ configFingerprint ++ templateFingerprints ++
      templateHeaderFingerprints ++ templateFooterFingerprints ++ objectsFingerprint
  }

  def fingerprintElement(
      tpe: String,
      content: Option[(FileAsset, HashedContent)],
      workspaceSourceRoot: Directory
  ): Option[String] =
    content.map { case (file, content) =>
      PathUtils.fingerprintElement(tpe, workspaceSourceRoot.relativize(file).pathFingerprint, content.hash)
    }
}
