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

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.process.ExternalProcessBuilder
import optimus.buildtool.utils.process.ProcessExitException
import optimus.platform._
import optimus.platform.util.Log

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.reflect.internal.MissingRequirementError
import scala.util.Using

@entity class PodcastGenerator(
    workspaceSourceRoot: Directory,
    obtConfig: ObtConfig,
    directoryFactory: DirectoryFactory,
    processBuilder: ExternalProcessBuilder
) extends SourceGenerator {
  import PodcastGenerator._

  override val generatorType: String = "podcast"
  private val domainFile = "domains.properties"

  override type Inputs = PodcastGenerator.Inputs

  override def templateType(configuration: Map[String, String]): PathFilter = Directory.fileExtensionPredicate("java")

  @async override protected[generators] def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    val obtWorkspaceProperties = obtConfig.properties.fold(ConfigFactory.empty)(_.config)
    val podcastVersion = obtWorkspaceProperties.getString("versions.podcast")
    val podcastLibPath = getPodcastLibPath(obtWorkspaceProperties)

    val templatesWithDomainFile =
      templates.withFiles(Domain, Directory.fileNamePredicate(domainFile), allowEmpty = true)

    // Get app name and subapp name from properties file if available
    val propertiesStream = templatesWithDomainFile.content(Domain).singleOption.map(_._2.contentAsInputStream)

    val properties = propertiesStream
      .map { stream =>
        Using.resource(stream) { s =>
          val props = new Properties()
          props.load(s)
          props.asScala.toMap
        }
      }
      .getOrElse(Map.empty[String, String])

    val appName = properties.getOrElse("appName", "default")
    val subAppName = properties.getOrElse("subAppName", appName)

    // find 'domains' source set from dependency source sets
    val domainsScope = scope.upstream.allCompileDependencies.apar
      .flatMap(d => d.transitiveScopeDependencies)
      .find(_.id.tpe.equals("domains")) // TODO (OPTIMUS-77939) read the config value for the source set name here,

    val files = domainsScope
      .map { s =>
        val allSourceFolders =
          s.config.absSourceRoots.apar.map(directoryFactory.lookupSourceFolder(workspaceSourceRoot, _))
        val sourceFolders = allSourceFolders.apar.filter(_.exists)
        templatesWithDomainFile.withFolders(Schema, sourceFolders, Nil, fingerprintTypeSuffix = s.id.toString)
      }
      .getOrElse(templatesWithDomainFile)

    val externalDeps = scope.upstream.allCompileDependencies.apar
      .flatMap(_.transitiveExternalDependencies)
      .apar
      .map(scope.dependencyCopier.atomicallyDepCopyArtifactsIfMissing)

    val (externalJars, externalDepsFp) = externalDeps.apar.collect { case p: ExternalClassFileArtifact =>
      (p.file, p.fingerprint)
    }.unzip

    val (upstreamJars, upstreamDepsFp): (Seq[JarAsset], Seq[String]) =
      scope.upstream.classesForOurCompiler.apar.collect { case x: InternalClassFileArtifact =>
        (x.file, x.fingerprint)
      }.unzip

    val extraFingerprint =
      Seq(s"[Podcast]$podcastVersion", s"[AppName]$appName", s"[SubAppName]$subAppName") ++
        upstreamDepsFp ++ externalDepsFp
    val fingerprintHash = files.hashFingerprint(Map.empty, extraFingerprint)

    PodcastGenerator.Inputs(
      files = files.toFiles, // TODO (OPTIMUS-77939): get schema from config
      podcastVersion = podcastVersion,
      podcastLibPath = podcastLibPath,
      appName = appName,
      subAppName = subAppName,
      fingerprint = fingerprintHash,
      scope = scope,
      dependencies = upstreamJars ++ externalJars
    )
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean = true

  @async override def _generateSource(
      scopeId: ScopeId,
      inputs: PodcastGenerator.Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = writer.atomicallyWrite() { context =>
    import inputs._
    val tempDir = context.outputDir.parent.resolveDir(RelativePath("tmp"))
    Files.createDirectories(tempDir.path)

    val messages =
      NodeTry {
        // Step 1: Generate metadata
        val metadataFile = tempDir.resolveFile(RelativePath("metadata.json"))
        Files.createFile(metadataFile.path)
        val schemaDir = files.internalRoot(Schema)
        val metadataMessages = generateMetadata(
          scopeId,
          generatorId,
          // find 'domains' source set from dependency source sets
          schemaDir,
          tempDir,
          metadataFile,
          podcastLibPath,
          inputs.dependencies
        )

        val generationMessages = if (metadataMessages.exists(_.isError)) {
          Nil
        } else {
          // Step 2: Run code generation
          val generatorScript = getGeneratorScript(podcastVersion)
          val msgs = runCodeGeneration(
            scopeId,
            generatorId,
            generatorScript,
            appName,
            subAppName,
            schemaDir,
            tempDir,
            metadataFile
          )

          if (!msgs.exists(_.isError)) {
            // Copy the generated code to the output directory
            copyGeneratedCode(schemaDir, context.outputDir)
          }
          msgs
        }
        metadataMessages ++ generationMessages
      }.recover { case e: MissingRequirementError =>
        Seq(CompilationMessage(None, s"Error during code generation: ${e.getMessage}", CompilationMessage.Error))
      }.get

    context.createArtifact(messages)
  }

  private def getGeneratorScript(podcastVersion: String): FileAsset = {
    val isWindows = System.getProperty("os.name").toLowerCase.contains("windows")
    val scriptDir = if (podcastVersion == PodcastGenerator.LOCAL_VERSION) {
      val buildtoolJar = FileAsset(Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.getPath))
      buildtoolJar.parent
    } else {
      getBasePodcastPath().resolveDir("scripts/bin/java")
    }
    if (isWindows) scriptDir.resolveFile("generator.cmd")
    else scriptDir.resolveFile("generator.sh")
  }

  @async def generateMetadata(
      scopeId: ScopeId,
      generatorId: String,
      schemaDir: Directory,
      tempDir: Directory,
      metadataFile: FileAsset,
      podcastLibPath: JarAsset,
      dependencies: Seq[JarAsset]
  ): Seq[CompilationMessage] = {
    val classPathFile = tempDir.resolveFile(RelativePath("classpath.txt"))
    val separator = if (OsUtils.isWindows) ";" else ":"
    val classPathFileContent =
      (podcastLibPath +: dependencies).map(_.pathString).mkString(separator)
    AssetUtils.atomicallyWrite(classPathFile, replaceIfExists = true) { p =>
      Files.write(p, classPathFileContent.getBytes("UTF-8"))
    }

    val cmdLine = Seq(
      "java",
      "-cp",
      s"@${classPathFile.pathString}",
      "ets.podcast.metadata.MetadataGenerator",
      schemaDir.path.toString, // need to use a regular path string here (without the leading '//' on linux)
      metadataFile.pathString
    )
    val process = processBuilder.build(
      scopeId,
      generatorId,
      cmdLine,
    )

    asyncResult(process.start()) match {
      case NodeSuccess(()) =>
        Nil
      case NodeFailure(e: ProcessExitException) =>
        log.error(e.getMessage)
        Seq(CompilationMessage.error(s"Metadata generation failed with exit code: ${e.exitCode}"))
      case NodeFailure(t) =>
        throw new RuntimeException(s"Failed to generate metadata: ${t.getMessage}", t)
    }
  }

  @async private def runCodeGeneration(
      scopeId: ScopeId,
      generatorId: String,
      generatorScript: FileAsset,
      appName: String,
      subAppName: String,
      schemaDir: Directory,
      tempDir: Directory,
      metadataFile: FileAsset
  ): Seq[CompilationMessage] = {
    val cmdLine = Seq(
      generatorScript.pathString,
      appName,
      subAppName,
      schemaDir.pathString,
      tempDir.pathString,
      metadataFile.pathString
    )
    val process = processBuilder.build(
      scopeId,
      generatorId,
      cmdLine
    )

    asyncResult(process.start()) match {
      case NodeSuccess(()) =>
        Nil
      case NodeFailure(e: ProcessExitException) =>
        log.error(e.getMessage)
        Seq(CompilationMessage.error(s"Code generation failed with exit code: ${e.exitCode}"))
      case NodeFailure(t) =>
        throw new RuntimeException(s"Failed to run code generation: ${t.getMessage}", t)
    }
  }

  private def copyGeneratedCode(sourceDir: Directory, targetDir: Directory): Unit = {
    val generatedDirs = findGeneratedDirectories(sourceDir)
    generatedDirs.foreach { dir =>
      copyDirectory(dir, targetDir.resolveDir(sourceDir.relativize(dir)))
    }
  }

  private def getBasePodcastPath(
      config: Config = obtConfig.properties.fold(ConfigFactory.empty)(_.config)
  ): Directory = {
    val dirStr =
      if (OsUtils.isWindows) config.getString("podcast.windows.path")
      else config.getString("podcast.linux.path")

    Directory(Paths.get(dirStr))
  }

  private def getPodcastLibPath(config: Config): JarAsset = {
    val basePodcastPath = getBasePodcastPath(config)

    val podcastLibName = "podcast.jar"
    val legacyPodcastLibName = "ets.podcast.jar"

    val podcastLibPath = JarAsset(Paths.get(s"$basePodcastPath${File.separator}lib${File.separator}$podcastLibName"))
    // We don't expect the lib to be created or deleted while OBT is running, so ok to use `existsUnsafe` here
    if (podcastLibPath.existsUnsafe) {
      podcastLibPath
    } else {
      JarAsset(Paths.get(s"$basePodcastPath${File.separator}lib${File.separator}$legacyPodcastLibName"))
    }
  }

  private[generators] def findGeneratedDirectories(dir: Directory): Seq[Directory] = {
    try {
      val generatedDir = dir.resolveDir(RelativePath("generated"))
      if (generatedDir.existsUnsafe) {
        Seq(generatedDir)
      } else {
        val subdirs = Files
          .list(dir.path)
          .filter(Files.isDirectory(_))
          .map(path => Directory(path))
          .iterator()
          .asScala
          .toSeq

        subdirs.flatMap(findGeneratedDirectories)
      }
    } catch {
      case e: Exception =>
        log.warn(s"Error finding generated directories in ${dir.pathString}: ${e.getMessage}")
        Seq.empty
    }
  }

  private[generators] def copyDirectory(source: Directory, target: Directory): Unit = {
    Files.createDirectories(target.path)

    // Copy all files in this directory
    Files
      .list(source.path)
      .filter(Files.isRegularFile(_))
      .forEach { filePath =>
        val fileName = filePath.getFileName.toString
        val targetPath = target.path.resolve(fileName)
        Files.copy(filePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
      }

    // Recursively copy subdirectories
    Files
      .list(source.path)
      .filter(Files.isDirectory(_))
      .forEach { dirPath =>
        val dirName = dirPath.getFileName.toString
        val sourceSubDir = source.resolveDir(RelativePath(dirName))
        val targetSubDir = target.resolveDir(RelativePath(dirName))
        copyDirectory(sourceSubDir, targetSubDir)
      }
  }
}

object PodcastGenerator extends Log {
  private val LOCAL_VERSION = "Local"

  case object Domain extends SandboxedFiles.FileType
  case object Schema extends SandboxedFiles.FileType

  final case class Inputs(
      files: SandboxedFiles,
      podcastVersion: String,
      podcastLibPath: JarAsset,
      appName: String,
      subAppName: String,
      fingerprint: FingerprintArtifact,
      scope: CompilationScope,
      dependencies: Seq[JarAsset]
  ) extends SourceGenerator.Inputs
}
