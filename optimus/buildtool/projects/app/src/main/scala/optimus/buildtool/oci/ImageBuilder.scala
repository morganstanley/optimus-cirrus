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
package optimus.buildtool
package oci

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFileAttributeView
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.jar

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.google.cloud.tools.jib.api._
import com.google.cloud.tools.jib.api.buildplan._
import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postbuilders.installer.BaseInstaller
import optimus.buildtool.builders.postbuilders.installer.DisabledBundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.ManifestResolver
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.builders.postbuilders.installer.component.DockerApplicationScriptsInstaller
import optimus.buildtool.builders.postbuilders.installer.component.CopyFilesInstaller
import optimus.buildtool.builders.postbuilders.installer.component.CppInstaller
import optimus.buildtool.builders.postbuilders.installer.component.FileCopySpec
import optimus.buildtool.builders.postbuilders.installer.component.InstallJarMapping
import optimus.buildtool.config.DockerConfigurationSupport
import optimus.buildtool.config.DockerImage
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.OctalMode
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.format.docker.DockerConfiguration
import optimus.buildtool.format.docker.ExtraImageDefinition
import optimus.buildtool.format.docker.ImageLocation
import optimus.buildtool.runconf.AppRunConf
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Commit
import optimus.buildtool.utils.FileAttrs._
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.JarUtils.nme
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.PosixPermissionUtils
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.isWindows
import optimus.platform._
import optimus.platform.util.Log
import optimus.platform.util.Version

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.Properties
import scala.util.Try

object ImageBuilder {
  lazy val initSpecialSystemProperties: () => Unit = {
    sys.env.get("CINITCCNAME").filter(_.nonEmpty).foreach { certPath => // be safe and treat set-but-empty as unset
      sys.props("javax.net.ssl.keyStore") = s"$certPath/${StaticConfig.string("sslKeyStoreSuffix")}"
      sys.props("javax.net.ssl.keyStoreType") = StaticConfig.string("sslKeyStoreType")
      sys.props("javax.net.ssl.keyStorePassword") = {
        val pass = StaticConfig.string("sslKeyStorePassword")
        // It turns out that you can't just pass an empty value via the shell (the jvm will think of it as unset,
        // and ignore it), so you have to hack around with a  string that contains the null terminator.
        // And you can’t pass a \0 character via argv[] since  most/all shells are written in C and discard
        // null terminators... so you have to do it within the jvm runtime context.
        if (pass.nonEmpty) pass else "\u0000"
      }
    }
    () => ()
  }

  type LayersBuilder[Key] = LoadingCache[Key, FileEntriesLayer.Builder]
  def newLayersBuilder[Key <: AnyRef](): LayersBuilder[Key] =
    Caffeine
      .newBuilder()
      .build(id => FileEntriesLayer.builder().setName(id.toString))

  object Layers {
    val metadata = "<metadata>"
    val jdk = "<JDK>"
  }
}

final case class DependencyFile(bucketId: Int, absFilePath: Path, inOutputTarPath: Path)

class ImageBuilder(
    val scopeConfigSource: ScopeConfigurationSource with DockerConfigurationSupport,
    val versionConfig: VersionConfiguration,
    val sourceDir: WorkspaceSourceRoot,
    val dockerImage: DockerImage,
    val workDir: Directory,
    val scopedCompilationFactory: ScopedCompilationFactory,
    val stripDependencies: Boolean,
    val latestCommit: Option[Commit],
    val directoryFactory: DirectoryFactory,
    val layersForDependencies: Int = 30,
    val useMavenLibs: Boolean,
    val dockerImageCacheDir: Directory
) extends AbstractImageBuilder {

  override protected lazy val dynamicDependencyDetector: DynamicDependencyDetector = DynamicDependencyDetector
  override protected val bundleClassJars: Boolean = false

  type Result = JibContainer
  override protected lazy val jib: ImageBuilderDelegate[JibContainer] = {
    val creationTime = latestCommit.map(_.commitTime).getOrElse(Instant.EPOCH)
    ImageBuilderDelegate.real(from = dockerImage.baseImage, to = dstImage)(
      _.setCreationTime(creationTime), // there are a bunch of extra config values here but we need none right yet
      _.setAlwaysCacheBaseImage(true)
        .setApplicationLayersCache((workDir resolveDir "layers").path)
        .setBaseImageLayersCache((workDir resolveDir "base").path)
        .setToolName("Optimus Build Tool") // does nothing but boast to the Docker image repository
        .setToolVersion(Version.version)
        // .setAdditionalTags(...) <- do we need this to be configurable somehow?
        .addEventHandler(new JibEventHandler(log, dstImage.name))
    )
  }

  override protected def handleResult(res: JibContainer): Unit = {
    log.info(s"[${dstImage.name}] Built container: ${res.getTargetImage}:${res.getImageId}")
  }
}

abstract class AbstractImageBuilder extends PostBuilder with BaseInstaller with Log {
  import ImageBuilder.Layers

  val sourceDir: WorkspaceSourceRoot
  val workDir: Directory
  val scopedCompilationFactory: ScopedCompilationFactory
  val dockerImage: DockerImage
  val stripDependencies: Boolean
  val latestCommit: Option[Commit]
  val scopeConfigSource: ScopeConfigurationSource with DockerConfigurationSupport
  val layersForDependencies: Int
  val directoryFactory: DirectoryFactory
  val useMavenLibs: Boolean
  val dockerImageCacheDir: Directory
  val dstImage: ImageLocation = dockerImage.location
  val relevantScopes: Set[ScopeId] = dockerImage.scopeIds
  val extraImages: Set[ExtraImageDefinition] = dockerImage.extraImages

  private val relevantBundles: Set[MetaBundle] = relevantScopes.map(_.metaBundle)
  private val stagingDir = Directory.temporary()
  protected val pathBuilder: InstallPathBuilder = InstallPathBuilder.dist(installVersion)
  protected def dynamicDependencyDetector: DynamicDependencyDetector
  protected val dependencyStripper = new DependencyStripper(stagingDir.resolveDir("stripped-dependencies"))

  // The staging path builder is used by regular installers
  // and we then map those paths to the regular path builder.
  val stagingPathBuilder: InstallPathBuilder = InstallPathBuilder.staging(installVersion, stagingDir)
  private val fingerprintsCache = new DisabledBundleFingerprintsCache(stagingPathBuilder)

  private val manifestResolver = ManifestResolver(scopeConfigSource, versionConfig, pathBuilder)
  private val applicationScriptsInstaller = new DockerApplicationScriptsInstaller
  private val cppInstaller = new CppInstaller(stagingPathBuilder, fingerprintsCache)
  private val copyFileInstaller = new CopyFilesInstaller(
    cache = new DisabledBundleFingerprintsCache(stagingPathBuilder), // we don't support incremental installs for docker
    scopeConfigSource,
    sourceDir,
    stagingPathBuilder,
    directoryFactory
  )

  private def stagingFile(scope: ScopeId, hash: String, suffix: String): FileAsset = {
    stagingDir resolveFile s"$scope.$hash.$suffix"
  }
  private def stagingSubdir(scope: ScopeId, hash: String): Directory = {
    val dir = stagingDir resolveDir s"$scope.$hash"
    Files.createDirectories(dir.path)
    dir
  }

  type Result
  protected def jib: ImageBuilderDelegate[Result]

  protected val dockerConfiguration: DockerConfiguration = scopeConfigSource.dockerStructure.configuration

  private val pathExcludes = {
    val fileSystem = FileSystems.getDefault
    dockerConfiguration.excludes.map(f => fileSystem.getPathMatcher(s"glob:$f"))
  }

  private val internalLayers = ImageBuilder.newLayersBuilder[MetaBundle]()
  private val processedDependencies = new ConcurrentHashMap[Path, Set[DependencyFile]]()
  private val processedJDKs = new ConcurrentHashMap[Path, Set[Path]]()

  ImageBuilder.initSpecialSystemProperties()

  // This part require docker daemon to download source images
  private val dockerUtils = new DockerUtils(dockerImageCacheDir)

  private def getExtraImageNameFromCachePath(path: Path): String = {
    val imageName = "([^/]+)".r.findFirstIn(s"/$path".replace(dockerImageCacheDir.pathString, "")) match {
      case Some(name) => name
      case None =>
        log.debug(s"Can't resolve image name from path: $dockerImageCacheDir")
        dockerImageCacheDir.pathString
    }
    s"[$imageName]"
  }

  // try download extraImages at the beginning
  if (extraImages.nonEmpty) {
    val extraImageFilesPerImg: Set[(Path, Path)] = extraImages.flatten(dockerUtils.getExtraImageFilesMap)

    val extraImagesDuplications: Iterable[List[(Path, Path)]] =
      extraImageFilesPerImg.toList.groupBy(_._1).collect {
        case (_, fromImage) if fromImage.size > 1 => fromImage
      }

    // merge downloaded files into a single map, remove duplications from images
    val resolvedExtraImagesFiles: Map[Path, Path] = extraImageFilesPerImg.toMap // we don't control priority here

    def duplicateMsgForExtraImages(paths: List[(Path, Path)]): Unit = {
      val (Seq(finalSourceImage), ignoredImages) =
        paths.map(_._2).partition(p => resolvedExtraImagesFiles.values.toList.contains(p))
      log.warn(s"will use: '${paths.map(_._1).singleDistinct}' from ${getExtraImageNameFromCachePath(
          finalSourceImage)}, ignoring the one from ${ignoredImages.map(getExtraImageNameFromCachePath).mkString(", ")}")
    }

    // print duplications between extra images here
    if (extraImagesDuplications.nonEmpty) {
      extraImagesDuplications.foreach(duplicateMsgForExtraImages)
      log.warn(
        s"[${dockerImage.location.name}] totally found ${extraImagesDuplications.size} duplications between extraImages! Please consider optimize your config in docker.obt:${dockerImage.location.name}:extraImages{}.")
    }

    analyzeDependencies(
      resolvedExtraImagesFiles.keys,
      Some(resolvedExtraImagesFiles.map { case (inTarPath, file) => inTarPath -> file }))
  }

  analyzeDependencies(dockerConfiguration.extraDependencies)

  @async override def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit =
    if (relevantScopes.contains(id)) analyzeArtifacts(Set(id), artifacts, transitive = false)

  @async override def postProcessTransitiveArtifacts(ids: Set[ScopeId], artifacts: Seq[Artifact]): Unit = {
    val relevantIds = relevantScopes.intersect(ids)
    analyzeArtifacts(relevantIds, artifacts, transitive = true)
  }

  @async override def postProcessArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact], successful: Boolean): Unit =
    if (successful) {
      // add pathing bundle jars
      val relevantMetaBundles = relevantScopes.intersect(scopes).map(_.metaBundle)
      val transitiveScopes = Artifact.transitiveIds(scopes, artifacts)
      val relevantTransitivePathingScopes = transitiveScopes.apar.filter { s =>
        relevantMetaBundles.contains(s.metaBundle) && scopeConfigSource.scopeConfiguration(s).pathingBundle
      }
      analyzeArtifacts(relevantTransitivePathingScopes, artifacts, transitive = true)

      addMetadataLayer()
      addEnvVariables()
      addJdkLayer()
      addDependenciesLayers()

      // TODO (OPTIMUS-30564): sort by rough topological order?
      internalLayers.asMap().asScala.toSeq.sortBy(_._1).foreach { case (_, builder) =>
        jib.addFileLayerBuilder(builder)
      }
    }

  @async private def analyzeArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact], transitive: Boolean): Unit = {
    val allScopeArtifacts = this.allScopeArtifacts(artifacts)
    val installJarMapping = InstallJarMapping(allScopeArtifacts)
    val installable =
      InstallableArtifacts(scopes, artifacts, allScopeArtifacts, transitive)

    val relevantRunconfJars: Seq[(ScopeId, JarAsset)] = allScopeArtifacts.apar.flatMap {
      case scopeArtifacts if scopes.contains(scopeArtifacts.scopeId) =>
        scopeArtifacts.runconfJar.map(scopeArtifacts.scopeId -> _)
      case _ => None
    }

    apar(
      analyzeJars(scopes, allScopeArtifacts),
      analyzeRunconfJars(relevantRunconfJars),
      analyzePathingJars(scopes, allScopeArtifacts, relevantRunconfJars, installJarMapping),
      analyzeCppLibs(installable),
      analyzeCopyFileConfiguration(scopes)
    )
  }

  @async private def analyzeJars(scopes: Set[ScopeId], allScopeArtifacts: Seq[ScopeArtifacts]): Unit =
    allScopeArtifacts.apar.filter(a => scopes.contains(a.scopeId)).foreach { scopeArtifacts =>
      import scopeArtifacts._
      if (classJars.nonEmpty) {
        val manifest = manifestResolver.manifestFromConfig(scopeId)
        val fingerprint = Jars.fingerprint(manifest) ++ classJars.map(Hashing.hashFileContent)
        val hash = Hashing.hashStrings(fingerprint)

        val stagingJar = stagingFile(scopeId, hash, "jar").asJar
        writeClassJar(classJars, manifest, stagingJar)

        putFile(scopeId.metaBundle, stagingJar, installJar.jar, OctalMode.Default)

        val targetBundles = scopeConfigSource.scopeConfiguration(scopeId).targetBundles.toSet
        targetBundles.intersect(relevantBundles).foreach { targetBundle =>
          // path of installJar relative to our common dir (so basically lib/entityagent.jar)
          val relpath = pathBuilder.dirForScope(scopeId).relativize(installJar.jar)
          // path in target meta/bundle (so basically /AFS/path/to/bundle/$version/common/lib/entityagent.jar)
          val targetInstallJar = pathBuilder.dirForMetaBundle(targetBundle).resolveFile(relpath)
          putFile(targetBundle, stagingJar, targetInstallJar, OctalMode.Default)
        }
      }
    }

  @async private def analyzeRunconfJars(relevantRunconfJars: Seq[(ScopeId, JarAsset)]): Unit =
    relevantRunconfJars.apar.foreach { case (scopeId, runConfJar) =>
      val hash = Hashing.hashFileContent(runConfJar)
      val stagingDir = stagingSubdir(scopeId, hash) resolveDir "bin"
      val installDir = pathBuilder.dirForDist(scopeId.metaBundle, "bin")
      val stagedAppscripts = applicationScriptsInstaller.installDockerApplicationScripts(stagingDir, runConfJar)
      stagedAppscripts.foreach { staged =>
        // dropping the .dck extension when adding files in the container
        val dst = installDir.resolveFile(staged.name.replaceAll("\\.dckr\\.sh$", ""))
        putFile(scopeId.metaBundle, src = staged, dst = dst, mode = OctalMode.Execute)
      }

      scopedCompilationFactory.lookupScope(scopeId).foreach { compilation =>
        def isDockerEnabled(arc: AppRunConf): Boolean =
          arc.scriptTemplates.templates.get("docker").exists(_ != "disabled")

        val arcs = compilation.runConfigurations.collect { case arc: AppRunConf if isDockerEnabled(arc) => arc }.toSet

        val nativeLibraries = arcs.apar
          .flatMap { arc =>
            arc.nativeLibraries.defaults ++ arc.nativeLibraries.includes
          }
          .map(s => Paths.get(s))
        analyzeDependencies(nativeLibraries)

        val jdks = arcs.apar.flatMap(_.javaModule.pathOption)
        jdks.foreach(jdk => processedJDKs.computeIfAbsent(jdk, p => processPath(p, withDynamicDeps = false)))
      }
    }

  @async private def analyzePathingJars(
      scopes: Set[ScopeId],
      allScopeArtifacts: Seq[ScopeArtifacts],
      relevantRunconfJars: Seq[(ScopeId, JarAsset)],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)]
  ): Unit = {
    val runconfScopes = relevantRunconfJars.map(_._1).toSet
    val scopesNeedingPathing =
      scopes.apar.filter(s => runconfScopes.contains(s) || scopeConfigSource.scopeConfiguration(s).pathingBundle)
    println(s"Scopes needing pathing: ${scopesNeedingPathing}")

    allScopeArtifacts.apar.foreach {
      case scopeArtifacts if scopesNeedingPathing.contains(scopeArtifacts.scopeId) =>
        scopeArtifacts.pathingJar.foreach { pathingJar =>
          val scopeId = scopeArtifacts.scopeId
          val name = NamingConventions.pathingJarName(scopeId)
          val installedPathingJar = pathBuilder.dirForDist(scopeId.metaBundle, "lib").resolveJar(name)
          val manifest =
            Jars.mergeManifests(
              manifestResolver.manifestFromConfig(scopeId),
              manifestResolver
                .locationIndependentManifest(scopeId, Some(pathingJar), installJarMapping, includeRelativePaths = false)
                .get
            )

          val hash = Hashing.hashStrings(Jars.fingerprint(manifest))
          val stagingJar = stagingFile(scopeId, hash, "jar").asJar
          AssetUtils.atomicallyWrite(stagingJar, replaceIfExists = true) { tempJar =>
            Jars.writeManifestJar(JarAsset(tempJar), manifest)
          }
          putFile(scopeId.metaBundle, stagingJar, installedPathingJar, OctalMode.Default)

          analyzePathingJarDependencies(pathingJar, manifest)
        }

      case _ =>
        Nil
    }
  }

  @async private def analyzeCppLibs(installable: InstallableArtifacts): Unit = {
    val cppArtifacts = installable.includedArtifacts(ArtifactType.Cpp)
    val grouped = cppArtifacts.groupBy(_.scopeId.metaBundle)
    grouped.foreach { case (mb, artifacts) =>
      val stagedFiles = cppInstaller.install(artifacts)
      stagedFiles.foreach { staged =>
        // we need to put C++ libs in exec rather than .exec/sysname, since docker doesn't get the automatic
        // exec symlink that AFS provides
        val target = pathBuilder.dirForMetaBundle(mb, leaf = "lib", branch = "exec").resolveFile(staged.name)
        putFile(mb, staged, target, OctalMode.Default)
      }
    }
  }

  @async private def analyzeCopyFileConfiguration(scopeIds: Set[ScopeId]): Unit = scopeIds.apar.foreach { scopeId =>
    val into = pathBuilder.dirForMetaBundle(scopeId.metaBundle)
    def stagingDirWithHash(spec: FileCopySpec): Directory =
      stagingSubdir(scopeId, Hashing.consistentlyHashDirectory(spec.from))
    val filesToPut = for {
      (stagingDir, copiedFiles) <- copyFileInstaller.copyFilesToCustomDir(scopeId, stagingDirWithHash)
      copiedFile <- copiedFiles
    } yield {
      val dst = into.resolveFile(stagingDir.relativize(copiedFile))
      val mode = if (!isWindows) {
        // we just preserve the mode used by the copy file installer
        PosixPermissionUtils.toMode(Files.getPosixFilePermissions(copiedFile.path))
      } else OctalMode.Default
      FileToPut(src = copiedFile, dst = dst, mode)
    }
    putFiles(scopeId.metaBundle, filesToPut)
  }

  private def analyzePathingJarDependencies(jarAsset: JarAsset, manifest: jar.Manifest): Unit =
    analyzeDependencies(
      Jars.extractManifestClasspath(jarAsset, manifest).map(_.path) ++
        Seq(
          nme.JniFallbackPath,
          nme.PreloadReleaseFallbackPath,
          nme.PreloadDebugFallbackPath,
          nme.ExternalJniPath,
          nme.ExtraFiles
        ).flatMap { k => JarUtils.load(manifest, k, ";").map(Paths.get(_)) }
    )

  private def analyzeDependencies(paths: Iterable[Path], extraImagePaths: Option[Map[Path, Path]] = None): Unit = {
    def pickBucketId(path: Path): Int = {
      // Dependencies tend to be by far our biggest layer
      // here we try to split them in an idempotent way
      // and group things from the same parent (/AFS/path/to/meta/PROJ/project/version)) in the same bucket
      val initialPath =
        PathUtils
          .platformIndependentString(path)
          .split("/")
          .take(7)
          .mkString("/")
      initialPath.hashCode().abs % layersForDependencies
    }

    val filePaths =
      if (extraImagePaths.isDefined) paths // all extracted local disk files from extraImage.
      else paths.filter(_.getRoot ne null).filter(Files.exists(_))

    filePaths.foreach { dependency =>
      processedDependencies.computeIfAbsent(
        dependency,
        { d =>
          processPath(d, withDynamicDeps = true).map { inTarPathToAdd =>
            val fileAbsPath = extraImagePaths match {
              case Some(extraImagePathMap) if extraImagePathMap.keySet.contains(inTarPathToAdd) =>
                extraImagePathMap(inTarPathToAdd) // get local disk path
              case _ => // when not for extraImage(should also consider not in extraImagePathMap case when concurrent)
                inTarPathToAdd
            }
            DependencyFile(pickBucketId(inTarPathToAdd), fileAbsPath, inTarPathToAdd)
          }
        }
      )
    }
  }

  @async private def addJdkLayer(): Unit = {
    val layer = FileEntriesLayer.builder().setName(Layers.jdk)
    val pathsToAdd = processedJDKs.values().asScala.toSet.flatten
    pathsToAdd.toSeq.sorted.foreach(p => addEntryToLayer(layer, p, p))
    jib.addFileLayerBuilder(layer)
  }

  @async private def addMetadataLayer(): Unit = {
    val metadataDir = stagingDir.resolveDir("metadata")
    Files.createDirectories(metadataDir.path)
    val layer = FileEntriesLayer.builder().setName(Layers.metadata)

    val metadataFileEntry = {
      val file = metadataDir.resolveFile("version.properties")
      val properties =
        Map("codetree.tag" -> dstImage.tag, "commit.hash" -> latestCommit.map(_.hash).getOrElse("UNKNOWN"))
      Utils.writePropertiesToFile(file, properties)
      val dest = absoluteUnixPath(NamingConventions.dockerMetadataProperties)
      val readOnlyPerm = FilePermissions.fromOctalString("444") // making sure people cannot change it
      new FileEntry(file.path, dest, readOnlyPerm, Instant.EPOCH)
    }

    layer.addEntry(metadataFileEntry)
    jib.addFileLayerBuilder(layer)
  }

  // Here we take only one jdk because we are appending the path variable as a convenience to users, so that they have
  // java, jcmd etc on path when they log into the container. We don't care which jdk it is.
  @async private def addEnvVariables(): Unit = processedJDKs.keys().asScala.take(1).foreach { jdk =>
    // unfortunately appending to an existing env variable is not supported...
    val basePath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    // putting the jdk bin folder on the PATH just cause it makes life easier when debugging
    val jdkBin = PathUtils.platformIndependentString(jdk.resolve("bin"))
    jib.addEnvVariable("PATH", s"$basePath:$jdkBin")
  }

  @async private def addDependenciesLayers(): Unit = {
    val layers = ImageBuilder.newLayersBuilder[String]()

    val pathsWithBucketsToAdd = processedDependencies.values().asScala.toSet.flatten
    val (extraImagesPaths, depsPaths) = pathsWithBucketsToAdd.partition { depDef =>
      depDef.absFilePath.startsWith(dockerImageCacheDir.toString)
    }
    // extraImages files should be prior than dependencies extraFiles
    (extraImagesPaths.toSeq ++ depsPaths.toSeq.sortBy(e => (e.bucketId, e.absFilePath))).foreach { depDef =>
      addEntryToLayer(layers.get(f"<dependencies-${depDef.bucketId}%02d>"), depDef.absFilePath, depDef.inOutputTarPath)
    }

    layers.asMap.asScala.toSeq.sortBy(_._1).foreach { case (_, layer) =>
      jib.addFileLayerBuilder(layer)
    }
  }

  private def processPath(path: Path, withDynamicDeps: Boolean): Set[Path] = {
    // this is a horrid hack around the problem that we have broken symlinks in our dependencies
    // and a second, less horrid hack around https://github.com/GoogleContainerTools/jib/issues/2275 (symlinks don't work)
    // plus an expedient exclusion of debug-only files and other detritus while I figure out the principled way of doing so
    def isReallyRealFile(p: Path): Boolean = {
      def isReallyRealSymlink =
        Try(Files.readSymbolicLink(p)).toOption.exists { tgt =>
          isReallyRealFile(p.getParent.resolve(tgt)) // symlink targets can be relative
        }

      Files.exists(p) && (!Files.isSymbolicLink(p) || isReallyRealSymlink)
    }

    def unprincipledExclude(path: Path): Boolean = pathExcludes.exists(_.matches(path))

    path match {
      case dir if Files.isDirectory(dir) =>
        Files.list(dir).iterator().asScala.toSet.flatMap(processPath(_, withDynamicDeps))
      case file if isReallyRealFile(file) && !unprincipledExclude(file) =>
        val dynamicDeps =
          if (withDynamicDeps) dynamicDependencyDetector.getDynamicDependencies(file, unprincipledExclude)
          else Set.empty
        Set(file) ++ dynamicDeps
      case file if unprincipledExclude(file) =>
        log.debug(s"[${dstImage.name}] File $file excluded by the unprincipled filter")
        Set.empty
      case _ =>
        log.debug(s"[${dstImage.name}] Skipping $path as it is neither a directory nor a file nor a valid symlink")
        Set.empty
    }

  }

  private def addEntryToLayer(layer: FileEntriesLayer.Builder, file: Path, inTarDst: Path): Unit = {
    val dst = absoluteUnixPath(inTarDst)
    val perms = copyPermissions(file)
    val reduced = if (stripDependencies) dependencyStripper.copyWithoutDebugSymbols(file) else None
    val entryAlreadyInLayer = layer.build().getEntries.asScala.toSet.find { f =>
      f.getExtractionPath.toString == inTarDst.toString
    }
    entryAlreadyInLayer match {
      // instead of let Jib automatically remove duplications, it's better we drop it at the beginning
      case Some(entry) => // so we can ensure extraImages files should be prior than dependencies native extraFiles
        log.warn(
          s"[${dockerImage.location.name}] dropped native dependency file: '$file', which duplicated with extraImages: ${getExtraImageNameFromCachePath(
              entry.getSourceFile)}")
      case _ => layer addEntry new FileEntry(reduced.getOrElse(file), dst, perms, Instant.EPOCH)
    }
  }

  private def copyPermissions(srcPath: Path): FilePermissions =
    FilePermissions.fromPosixFilePermissions {
      srcPath.asView[PosixFileAttributeView] match {
        case Some(attrs) => attrs.readAttributes.permissions
        case None =>
          OptimusBuildToolAssertions.assert(Properties.isWin, s"Expected to find mode for $srcPath somehow")
          PosixPermissionUtils.NoPermissions
      }
    }

  override def complete(successful: Boolean): Unit =
    if (successful) {
      handleResult(jib.finish())
      log.info(s"[${dstImage.name}] Built image as $dstImage")
    } else log.warn(s"[${dstImage.name}] Build was not successful; skipping image creation")

  protected def handleResult(res: Result): Unit = {}

  // this returns correct answers on windows... otherwise AUP chops off initial 2 segments of an AFS path.
  private def absoluteUnixPath(path: Path): AbsoluteUnixPath =
    AbsoluteUnixPath.get(PathUtils.platformIndependentString(path))

  private case class FileToPut(src: FileAsset, dst: FileAsset, mode: OctalMode)

  private def putFile(mb: MetaBundle, src: FileAsset, dst: FileAsset, mode: OctalMode): Unit = {
    putFiles(mb, Seq(FileToPut(src, dst, mode)))
  }

  private def putFiles(mb: MetaBundle, filesToPut: Seq[FileToPut]): Unit = {
    import buildplan._
    val layer = internalLayers.get(mb)
    layer.synchronized { // "layer" is basically just ArrayList[FileEntry] so we need to sync ourselves. Sorry.
      filesToPut.foreach { fileToPut =>
        layer addEntry new FileEntry(
          fileToPut.src.path,
          absoluteUnixPath(fileToPut.dst.path),
          FilePermissions.fromPosixFilePermissions(PosixPermissionUtils.fromMode(fileToPut.mode)),
          Instant.EPOCH // keepin' it RT in here (or something like it)
        )
      }
    }
  }
}
