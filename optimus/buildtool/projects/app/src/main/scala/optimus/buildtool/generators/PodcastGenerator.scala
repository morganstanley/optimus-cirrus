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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.config.ObtConfig
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
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Properties
import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._
import scala.reflect.internal.MissingRequirementError
import scala.util.Try
import scala.util.control.NonFatal

@entity class PodcastGenerator(workspaceSourceRoot: Directory, obtConfig: ObtConfig) extends SourceGenerator {
  override val generatorType: String = "podcast"

  override type Inputs = PodcastGenerator.Inputs

  @node override protected[generators] def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {
    // Determine which domain type we're processing
    val isTestDomain = name.toLowerCase.contains("test")

    // Find schema files
    val schemaFilter = sourceFilter && Directory.fileExtensionPredicate("java")
    val (schemaFiles, schemaFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      schemaFilter,
      scope,
      workspaceSourceRoot,
      s"Schema:$name"
    )
    val obtWorkspaceProperties = obtConfig.properties.fold(ConfigFactory.empty)(_.config)
    val podcastVersion = obtWorkspaceProperties.getString("versions.podcast")
    val podcastLibPath = getPodcastLibPath(podcastVersion, obtWorkspaceProperties)

    // Get app name and subapp name from properties file if available
    val propertiesFiles = internalFolders.flatMap { folder =>
      val propertiesPath = folder.workspaceSrcRootToSourceFolderPath.resolvePath("domains.properties")
      val propertiesFile = workspaceSourceRoot.resolveFile(propertiesPath)
      if (propertiesFile.existsUnsafe) Some(propertiesFile) else None
    }

    val properties = propertiesFiles.headOption
      .map { file =>
        val props = new Properties()
        Try {
          val fis = new FileInputStream(file.pathString)
          try {
            props.load(fis)
          } finally {
            fis.close()
          }
        }.recover { case NonFatal(e) =>
          log.warn(s"Failed to load properties file ${file.pathString}: ${e.getMessage}")
        }
        props.asScala.toMap
      }
      .getOrElse(Map.empty[String, String])

    val appName = properties.getOrElse("appName", "default")
    val subAppName = properties.getOrElse("subAppName", appName)

    val inputArtifacts: Seq[ExternalClassFileArtifact] =
      scope.upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies)

    val (copiedDeps, copiedDepsFp) = inputArtifacts.apar
      .map(scope.dependencyCopier.atomicallyDepCopyArtifactsIfMissing)
      .apar
      .collect { case p: ExternalClassFileArtifact =>
        (p.file.path, p.fingerprint)
      }
      .unzip

    val (internalDeps, internalDepsFp): (Seq[Path], Seq[String]) = scope.upstream.classesForOurCompiler.apar.collect {
      case x: InternalClassFileArtifact =>
        (x.file.path, x.fingerprint)
    }.unzip

    val fingerprint =
      s"[Podcast]$podcastVersion" +:
        s"[AppName]$appName" +:
        s"[SubAppName]$subAppName" +:
        (schemaFingerprint ++
          internalDepsFp ++
          copiedDepsFp)
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint, Some(name))

    PodcastGenerator.Inputs(
      generatorName = name,
      schemaFiles = schemaFiles, // TODO (OPTIMUS-77939): get schema from config
      podcastVersion = podcastVersion,
      podcastLibPath = podcastLibPath,
      appName = appName,
      subAppName = subAppName,
      fingerprint = fingerprintHash,
      configuration = configuration,
      scope = scope,
      depPaths = copiedDeps ++ internalDeps
    )
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().schemaFiles.map(_._2).merge.nonEmpty

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
        val outputDir = tempDir.resolveDir(PodcastGenerator.SourcePath)
        val tmpFolder = tempDir.resolveDir(RelativePath("tmp"))
        Files.createDirectories(outputDir.path)
        Files.createDirectories(tmpFolder.path)

        // find 'domains' source set from dependency source sets
        val domainsScope = scope.config.dependencies.compileDependencies.modules
          .find(_.tpe.equals("domains")) // TODO (OPTIMUS-77939) read the config value for the source set name here,

        val templateDomains =
          workspaceSourceRoot.path.toAbsolutePath.resolve(
            scope.factory.scopeConfigSource
              .scopeConfiguration(domainsScope.get)
              .paths
              .scopeRoot
              .resolvePath("java")
              .path)

        val messages =
          NodeTry {
            // Step 1: Prepare schema files
            val schemaDir = tempDir.resolveDir(RelativePath("schema"))
            Files.createDirectories(schemaDir.path)

            val allSchemaFiles = schemaFiles.map(_._2).merge
            allSchemaFiles.foreach { case (file, content) =>
              val relPath = workspaceSourceRoot.relativize(file)
              val targetFile = schemaDir.resolveFile(relPath)
              Files.createDirectories(targetFile.parent.path)
              Files.copy(content.contentAsInputStream, targetFile.path, StandardCopyOption.REPLACE_EXISTING)
            }

            // Step 2: Generate metadata
            val metadataFile = tmpFolder.resolveFile(RelativePath("metadata.json"))
            Files.createFile(metadataFile.path)
            val metadataResult = generateMetadata(
              // find 'domains' source set from dependency source sets
              templateDomains, // TODO (OPTIMUS-77939): we should probably go back to a temp dir so we can read these files the right way and not break in sparse
              metadataFile.pathString,
              podcastLibPath,
              resolvedInputs.depPaths
            )

            if (!metadataResult) {
              Seq(CompilationMessage(None, "Failed to generate metadata", CompilationMessage.Error))
            } else {
              // Step 3: Run code generation
              val generatorScript = getGeneratorScript(podcastVersion)
              val generationResult = runCodeGeneration(
                generatorScript,
                appName,
                subAppName,
                schemaDir.pathString,
                tmpFolder.pathString,
                metadataFile.pathString
              )

              if (generationResult) {
                // Copy the generated code to the output directory
                copyGeneratedCode(schemaDir, outputDir)
                Seq.empty[CompilationMessage]
              } else {
                Seq(CompilationMessage(None, "Code generation failed", CompilationMessage.Error))
              }
            }
          }.recover { case e: MissingRequirementError =>
            Seq(CompilationMessage(None, s"Error during code generation: ${e.getMessage}", CompilationMessage.Error))
          }.get

        val artifact = GeneratedSourceArtifact.create(
          scopeId,
          tpe,
          generatorName,
          outputJar,
          PodcastGenerator.SourcePath,
          messages
        )

        SourceGenerator.createJar(
          tpe,
          generatorName,
          PodcastGenerator.SourcePath,
          artifact.messages,
          artifact.hasErrors,
          tempJar,
          tempDir)()

        artifact
      }
      Some(artifact)
    }
  }

  def getGeneratorScript(podcastVersion: String): String = {
    val isWindows = System.getProperty("os.name").toLowerCase.contains("windows")
    if (podcastVersion == PodcastGenerator.LOCAL_VERSION) {
      val scriptDir = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      val scriptPath = new File(scriptDir).getParent
      if (isWindows) s"${PathUtils.platformIndependentString(scriptPath)}\\generator.cmd"
      else s"${PathUtils.platformIndependentString(scriptPath)}/generator.sh"

    } else {
      val podcastPathBase = getBasePodcastPath(podcastVersion)
      if (isWindows) {
        s"$podcastPathBase\\scripts\\bin\\java\\generator.cmd"
      } else {
        s"$podcastPathBase/scripts/bin/java/generator.sh"
      }
    }
  }

  @entersGraph def generateMetadata(
      schemaDir: Path,
      metadataFile: String,
      podcastLibPath: String,
      dependencies: Seq[Path]): Boolean = {
    try {
      // TODO (OPTIMUS-77939): this temp dir should be in build_obt, using that api
      val tempDir = Directory.temporary("podcast_metadata_temp")
      Utils.createDirectories(tempDir)
      // copies java schema files to temp domains dir
      Directory // TODO (OPTIMUS-77939): there's a way to read the files here that will fall back to git metadata, but we probably want to do that as we fingerprint to avoid a race
        .findFiles(Directory.apply(schemaDir))
        .map { schemaFile =>
          val relativePath = schemaDir.relativize(schemaFile.path)

          val targetFile = tempDir.resolveDir("domains").resolveFile(relativePath.toString)
          Files.createDirectories(targetFile.parent.path)
          Files.copy(schemaFile.path, targetFile.path, StandardCopyOption.REPLACE_EXISTING)
        }

      val classPathFile = tempDir.resolveFile(RelativePath("classpath.txt"))
      val separator = if (OsUtils.isWindows) ";" else ":"
      val classPathFileContent =
        Seq(podcastLibPath, dependencies.mkString(separator)).mkString(separator)
      AssetUtils.atomicallyWrite(classPathFile, replaceIfExists = true) { p =>
        Files.write(p, classPathFileContent.getBytes("UTF-8"))
      }

      // step 2: java process to invoke podcast
      val processBuilder = new ProcessBuilder(
        "java",
        "-cp",
        s"@${classPathFile.pathString}",
        "ets.podcast.metadata.MetadataGenerator",
        tempDir.resolveDir("domains").path.normalize().toString,
        metadataFile
      )
      processBuilder.redirectErrorStream(true)
      val process = processBuilder.start()

      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      var line: String = null
      while ({ line = reader.readLine(); line != null }) {
        log.debug(s"Podcast MetadataGenerator: $line")
      }

      val exitCode = process.waitFor()
      if (exitCode != 0) {
        log.error(s"Metadata generation failed with exit code: $exitCode")
      }
      exitCode == 0
    } catch {
      case e: Exception =>
        log.error(s"Failed to generate metadata: ${e.getMessage}", e)
        false
    }
  }

  def runCodeGeneration(
      generatorScript: String,
      appName: String,
      subAppName: String,
      schemaDir: String,
      tmpFolder: String,
      metadataFile: String
  ): Boolean = {
    try {
      val processBuilder = new ProcessBuilder(
        generatorScript,
        appName,
        subAppName,
        schemaDir,
        tmpFolder,
        metadataFile
      )

      processBuilder.redirectErrorStream(true)
      val process = processBuilder.start()

      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      var line: String = null
      while ({ line = reader.readLine(); line != null }) log.debug(s"PodcastGenerator: $line")

      val exitCode = process.waitFor()
      if (exitCode != 0) log.error(s"Code generation failed with exit code: $exitCode")
      exitCode == 0
    } catch {
      case e: Exception =>
        log.error(s"Failed to run code generation: ${e.getMessage}", e)
        false
    }
  }

  def copyGeneratedCode(sourceDir: Directory, targetDir: Directory): Unit = {
    val generatedDirs = findGeneratedDirectories(sourceDir)
    generatedDirs.foreach { dir =>
      copyDirectory(dir, targetDir.resolveDir(sourceDir.relativize(dir)))
    }
  }

  def getBasePodcastPath(
      podcastVersion: String,
      config: Config = obtConfig.properties.fold(ConfigFactory.empty)(_.config)): String = {
    if (OsUtils.isWindows) {
      config.getString("podcast.windows.path")
    } else {
      config.getString("podcast.linux.path")
    }
  }

  def getPodcastLibPath(podcastVersion: String, config: Config): String = {
    val basePodcastPath = getBasePodcastPath(podcastVersion, config)

    val podcastLibName = "podcast.jar"
    val legacyPodcastLibName = "ets.podcast.jar"

    val podcastLibPath = s"$basePodcastPath${File.separator}lib${File.separator}$podcastLibName"
    if (new File(podcastLibPath).exists()) {
      podcastLibPath
    } else {
      s"$basePodcastPath${File.separator}lib${File.separator}$legacyPodcastLibName"
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
  private[buildtool] val SourcePath = RelativePath("src")
  private val LOCAL_VERSION = "Local"

  sealed trait DomainType
  object DomainType {
    case object Domains extends DomainType
    case object TestDomains extends DomainType
  }

  final case class Inputs(
      generatorName: String,
      schemaFiles: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      podcastVersion: String,
      podcastLibPath: String,
      appName: String,
      subAppName: String,
      fingerprint: FingerprintArtifact,
      configuration: Map[String, String],
      scope: CompilationScope,
      depPaths: Seq[Path]
  ) extends SourceGenerator.Inputs
}
