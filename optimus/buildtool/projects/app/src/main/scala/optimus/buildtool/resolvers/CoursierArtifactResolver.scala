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
package optimus.buildtool.resolvers

import java.net.URL
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystem
import java.nio.file.FileSystemException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.util.regex.Pattern
import coursier.Dependency
import coursier.Module
import coursier._
import coursier.core.ResolutionProcess.fetchOne
import coursier.core.Authentication
import coursier.core.Publication
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._
import coursier.core.ArtifactSource
import coursier.core.Configuration
import coursier.core.Extension
import coursier.core.Repository
import coursier.util.Artifact
import coursier.util.EitherT
import optimus.buildtool.artifacts.ExternalArtifactType
import optimus.buildtool.cache.NoOpRemoteAssetStore
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.DependencyCoursierKey
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.Exclude
import optimus.buildtool.config.ExternalDependency
import optimus.buildtool.config.ExtraLibDefinition
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.MappedDependencyDefinitions
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NamingConventions.UnzipMavenRepoExts
import optimus.buildtool.dependencies.AfsSource
import optimus.buildtool.dependencies.DependencySource
import optimus.buildtool.dependencies.MavenSource
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.resolvers.MavenUtils.downloadUrl
import optimus.buildtool.resolvers.MavenUtils.maxSrcDocDownloadSeconds
import optimus.buildtool.utils.StackUtils
import optimus.core.CoreAPI
import optimus.graph.Node
import optimus.stratosphere.artifactory.Credential
import org.xml.sax.SAXParseException
import optimus.scalacompat.collection._

import java.nio.file.Path
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.control.NonFatal

object NativeIvyConstants {
  private[resolvers] val JNIPathSepRegex: String = Pattern.quote("${PATH_SEP}")
  private[resolvers] val ModuleLoadAttrib = "moduleLoad"
  private[resolvers] val JNIPathAttrib = "jniPath"
}

object CoursierArtifactResolver {
  private implicit val graphAdaptor: CoursierGraphAdaptor.type = CoursierGraphAdaptor
  private val JvmDependenciesFilePathStr = JvmDependenciesConfig.path.toString

  private def jvmDepMsg(line: Int) = s"$JvmDependenciesFilePathStr:$line -"

  private[buildtool] def coursierDepToNameString(d: Dependency, withVersion: Boolean = false): String = {
    def toDepString(str: String) = if (str.contains(".")) s""""$str"""" else str
    val nameStr = s"${toDepString(d.module.organization.value)}.${toDepString(d.module.name.value)}"
    if (withVersion) s"$nameStr.${d.version}"
    else nameStr
  }

  private[buildtool] def invalidUnmappedDepMsg(
      line: Int,
      depName: String,
      unmappedResult: UnmappedResult,
      autoMappedResult: Option[AutoMappedResult] = None): String = {
    def toMsg(deps: Set[Dependency]): String =
      deps.map { d => s"${cleanModuleStr(d.module)}.${d.version}" }.mkString(", ")

    def toMappingRules(autoAfsToMavenMap: Map[Dependency, Set[Dependency]]): String = {

      def coursierDependencyToRule(deps: Set[Dependency], prefix: String): String =
        deps.map(d => s"$prefix.${coursierDepToNameString(d)}.version = ${d.version}").mkString("\n")

      autoAfsToMavenMap
        .map { case (afs, mavens) =>
          val multiSourceName = afs.module.name.value
          s"""|$multiSourceName {
              |  ${coursierDependencyToRule(Set(afs), "afs")}
              |  ${coursierDependencyToRule(mavens, "maven")}
              |}""".stripMargin
        }
        .toIndexedSeq
        .sorted
        .mkString("\n", "\n", "\n")
    }

    val leafMapMsg = toMsg(unmappedResult.leafs)
    val autoMappedMsg = autoMappedResult match {
      case Some(auto) if auto.autoAfsToMavenMap.nonEmpty =>
        s"|Found ${auto.afs.size} afs libs could potentially be mapped to same name maven equivalents: ${toMappingRules(auto.autoAfsToMavenMap)}"
      case _ => ""
    }

    s"""|${jvmDepMsg(line)} '$depName' depends on ${unmappedResult.all.size} transitive unmapped dependencies: ${toMsg(
         unmappedResult.all)}
        |please add mapping rules for ${unmappedResult.leafs.size} leaf dependencies first: $leafMapMsg
    $autoMappedMsg""".stripMargin
  }

  def cleanModuleStr(module: Module): String = s"${module.organization.value}:${module.name.value}"

  def coursierDependencyToInfo(d: Dependency): DependencyInfo =
    DependencyInfo(cleanModuleStr(d.module), d.configuration.value, d.version)

  def definitionToInfo(d: DependencyDefinition): DependencyInfo =
    DependencyInfo(s"${d.group}:${d.name}", d.configuration, d.version)

  def toDependencyCoursierKey(
      d: Dependency,
      configuration: Option[String] = None,
      version: Option[String] = None): DependencyCoursierKey =
    DependencyCoursierKey(
      d.module.organization.value,
      d.module.name.value,
      configuration.getOrElse(d.configuration.value),
      version.getOrElse(d.version))
}

@entity class CoursierArtifactResolver(
    resolvers: DependencyMetadataResolvers,
    externalDependencies: ExternalDependencies,
    dependencyCopier: DependencyCopier,
    dependencySource: DependencySource,
    globalExcludes: Seq[Exclude] = Nil,
    fileSystem: FileSystem = FileSystems.getDefault,
    credentials: Seq[Credential] = Nil,
    remoteAssetStore: RemoteAssetStore = NoOpRemoteAssetStore
) extends ExternalDependencyResolver(externalDependencies.definitions) {
  import CoursierArtifactResolver._

  // note that ivyConfiguration exclusions aren't supported natively by Coursier (we handle them ourselves in
  // applyExclusions instead)
  private val (globalConfigSpecificExcludes, globalNonConfigSpecificExcludes) =
    globalExcludes.partition(_.ivyConfiguration.isDefined)

  @node private def globalCoursierExcludes = toCoursierExcludes(globalNonConfigSpecificExcludes)

  @node private def dependencySpecificCoursierExcludes: Map[DependencyCoursierKey, Set[(Organization, ModuleName)]] =
    dependencyDefinitions.apar.map { d =>
      // note that JvmDependenciesLoader.readExcludes already checks for this and produces a nicer error - this check
      // is mostly here in case unit tests break the rules
      require(
        d.excludes.forall(_.ivyConfiguration.isEmpty),
        s"ivyConfiguration based excludes are only supported in global excludes (not in ${d.group}.${d.name})")

      DependencyCoursierKey(d.group, d.name, d.configuration, d.version) -> toCoursierExcludes(d.excludes)
    }.toMap

  private val defaultDefinitions: Map[Module, DependencyDefinition] = dependencyDefinitions
    .filter(_.variant.isEmpty)
    .map(d => Module(Organization(d.group), ModuleName(d.name)) -> d)
    .toMap
  private val availableResolversMap: Map[String, DependencyMetadataResolver] =
    resolvers.allResolvers.map(r => r.name -> r).toMap
  private[buildtool] val depModuleToResolversMap: Map[Module, Seq[DependencyMetadataResolver]] =
    (dependencyDefinitions ++ externalDependencies.mavenDependencies.noVersionMavenDeps).map { d =>
      val dependencyResolvers =
        if (d.resolvers.nonEmpty)
          d.resolvers.map(availableResolversMap)
        else if (d.isMaven) {
          resolvers.defaultMavenResolvers
        } else resolvers.defaultIvyResolvers
      Module(Organization(d.group), ModuleName(d.name)) -> dependencyResolvers
    }.toMap
  private val afsGroupNameToMavenMap: Map[MappingKey, Seq[DependencyDefinition]] = {
    val dependencyMappingMap: Map[DependencyDefinition, Seq[DependencyDefinition]] =
      dependencySource match {
        case AfsSource   => Map.empty
        case MavenSource => externalDependencies.afsToMavenMap
      }
    dependencyMappingMap.map { case (afs, maven) =>
      MappingKey(afs.group, afs.name, afs.configuration) -> maven
    }
  }
  private val localRepos: Seq[(String, MetadataPattern.Local)] =
    resolvers.defaultResolvers
      .flatMap(_.metadataPatterns)
      .collectAll[MetadataPattern.Local]
      .map(p => p.urlRepoRoot -> p)

  private def coursierDepToDefinition(coursierDep: Dependency): DependencyDefinition = dependencyDefinitions
    .find(d => d.group == coursierDep.module.organization.value && d.name == coursierDep.module.name.value)
    .getOrElse(
      DependencyDefinition(
        coursierDep.module.organization.value,
        coursierDep.module.name.value,
        coursierDep.version,
        LocalDefinition)
    )

  @node private def generateAutoMappingRules(
      afsDeps: Set[Dependency],
      mavenDeps: Set[Dependency]): Option[AutoMappedResult] = {
    def maybeSame(afs: String, maven: String) = afs == maven || maven.matches(s"$afs-(.*)")
    def mavenSameNames(afsName: String): Set[Dependency] =
      mavenDeps.filter(m => maybeSame(afsName, m.module.name.value))
    val sameNameAfsToMavenDeps: Map[Dependency, Set[Dependency]] = afsDeps.collect {
      case afs if mavenSameNames(afs.module.name.value).nonEmpty =>
        afs -> mavenSameNames(afs.module.name.value)
    }.toMap

    Some(AutoMappedResult(sameNameAfsToMavenDeps))
  }

  // note that we include all dependencyDefinitions (not just those directly used by deps) in the fingerprint because
  // any of them could affect the resolved versions of transitive dependencies, but the order doesn't matter so we
  // sort for consistency
  @node private def defaultDefinitionsFingerprint: Seq[String] =
    fingerprintDependencyDefinitions("default", defaultDefinitions.values.toSeq).sorted

  @node private def fingerprintDependencyDefinitions(tpe: String, deps: Iterable[DependencyDefinition]): Seq[String] = {
    deps.map { d =>
      s"$tpe-dependency-definition:${d.group}:${d.name}:${d.variant}:${d.version}:configuration=${d.configuration}:" +
        s"resolvers=${d.resolvers.mkString(",")}:ivyArtifacts=${d.ivyArtifacts.mkString(",")}:excludes=${d.excludes
            .mkString(",")}:" +
        s"transitive=${d.transitive}:force=${d.force}:macros=${d.containsMacros}:plugin=${d.isScalacPlugin}"
    }.toIndexedSeq
  }

  @node private def globalExcludesFingerprint: Seq[String] =
    globalExcludes.apar
      .map { exclude =>
        s"global-exclude:${exclude.group.getOrElse("")}:${exclude.name.getOrElse("")}:${exclude.ivyConfiguration.getOrElse("")}"
      }
      .toIndexedSeq
      .sorted

  @node private def multiSourceDependenciesFingerprint: Seq[String] =
    s"dependencySourceSetting=$dependencySource" +: (externalDependencies.multiSourceDependencies).apar
      .flatMap { dep =>
        fingerprintDependencyDefinitions("multi-source-afs", Seq(dep.definition)) ++
          fingerprintDependencyDefinitions("maven-equivalents", dep.equivalents.toIndexedSeq)
      }
      .toIndexedSeq
      .sorted

  @node private def noVersionMavenDependenciesFingerprint: Seq[String] =
    externalDependencies.mavenDependencies.noVersionMavenDeps.apar
      .flatMap { dep =>
        fingerprintDependencyDefinitions("no-version-maven", Seq(dep))
      }
      .toIndexedSeq
      .sorted

  @node override def fingerprintDependencies(deps: Seq[DependencyDefinition]): Seq[String] = {
    // the dependencies which were actually requested need to be fingerprinted in order (i.e. not sorted) because
    // their ordering affects the ordering of the output, but note that only the first variant of any given module matters
    // because subsequent ones are ignored in doResolveDependencies
    val requestedDepsFingerprint = fingerprintDependencyDefinitions("requested", distinctRequestedDeps(deps))
    resolvers.allResolvers.flatMap(
      _.fingerprint) ++ requestedDepsFingerprint ++ defaultDefinitionsFingerprint ++ globalExcludesFingerprint ++ multiSourceDependenciesFingerprint ++ noVersionMavenDependenciesFingerprint
  }

  // if there are multiple (variant) definitions requested for the same module and config, the first one wins.
  // (note that ScopeDependencies puts the ones specifically for this module first, before the transitive ones)
  @node private def distinctRequestedDeps(deps: Seq[DependencyDefinition]): Seq[DependencyDefinition] = {
    // carefully preserving order...
    deps
      .foldLeft(ListMap.empty[(Module, String), DependencyDefinition]) { (acc, d) =>
        val m = (toModule(d), d.configuration)
        // ...retaining only the first entry per module & config
        if (acc.contains(m)) acc else acc + ((m, d))
      }
      .values
      .toIndexedSeq // ensure we don't get a Stream
  }

  private def toModule(d: DependencyDefinition): Module =
    coursier.Module(Organization(d.group), ModuleName(d.name))

  private def getUnmappedResult(resolution: Resolution): UnmappedResult = {
    // configuration should be removed from coursier dep key, it will be changed in minDependencies
    val minDepsMap: Map[DependencyCoursierKey, Dependency] =
      resolution.minDependencies.map(d => toDependencyCoursierKey(d, Some("")) -> d).toMap

    def isMapped(dep: Dependency): Boolean = externalDependencies.mavenDependencies.allMappedMavenCoursierKey.exists(
      _.isSameName(toDependencyCoursierKey(dep)))

    // only need do mapping for minimum final in used libs
    def inMinDeps(resolvedMap: Map[Dependency, scala.Seq[Dependency]]): Set[Dependency] =
      resolvedMap.flatMap { case (k, v) => minDepsMap.get(toDependencyCoursierKey(k, Some(""))) }.toSet

    val allUnmappedDeps: Map[Dependency, scala.Seq[Dependency]] =
      resolution.finalDependenciesCache.collect { case (dep, child) if !isMapped(dep) => dep -> child }
    val leafs = allUnmappedDeps.filter { case (dep, child) => child.isEmpty }

    UnmappedResult(inMinDeps(allUnmappedDeps), inMinDeps(leafs))
  }

  /**
   * validates that all transitive dependencies of mix mode Maven dependencies are also mapped
   */
  @node def validateMixModeMavenDeps(
      mixedModeMavenDeps: Seq[DependencyDefinition],
      allVersions: Versions,
      mappedResult: MappedDependencyDefinitions): Seq[CompilationMessage] =
    mixedModeMavenDeps.apar.flatMap { mixedModeMavenDep =>
      val coursierMavenDep = toCoursierDep(mixedModeMavenDep, allVersions)
      val resolution = doResolution(Seq(coursierMavenDep), allVersions)
      val unmappedResult = getUnmappedResult(resolution)

      if (unmappedResult.all.nonEmpty) {
        val autoMap: Option[AutoMappedResult] = {
          val fromAfs = mappedResult.appliedAfsToMavenMap.apar
            .collect { case (k, v) if v.contains(mixedModeMavenDep) => k }
            .apar
            .map(toCoursierDep(_, allVersions))
            .to(Seq)
          val afsDeps = doResolution(fromAfs, allVersions).dependencies
          generateAutoMappingRules(afsDeps, unmappedResult.all)
        }
        val msg = invalidUnmappedDepMsg(mixedModeMavenDep.line, mixedModeMavenDep.key, unmappedResult, autoMap)
        Some(CompilationMessage(None, msg, CompilationMessage.Error))
      } else None
    }.sorted

  @node private def toCoursierDeps(defs: Seq[DependencyDefinition], allVersions: Versions): Seq[Dependency] =
    defs.apar.map { d => toCoursierDep(d, allVersions) }

  @node private def toCoursierDep(dep: DependencyDefinition, allVersions: Versions): Dependency = {
    val module = toModule(dep)
    val coursierDependency = coursier
      .Dependency(
        // tag the Module with these data so we can propagate them into the eventual ClassFileArtifact
        module = module,
        // load predefined version in multipleSourceDeps
        version = allVersions.allVersions.getOrElse(module, dep.version)
      )
      .withConfiguration(Configuration(dep.configuration))
      .withExclusions(globalCoursierExcludes ++ toCoursierExcludes(dep.excludes))
      .withTransitive(dep.transitive)
    CoursierInterner.internedDependency(dep.classifier match {
      case Some(str) =>
        coursierDependency.withAttributes(Attributes(classifier = Classifier(str)))
      case None => coursierDependency
    })
  }

  @node private def toCoursierExcludes(excludes: Iterable[Exclude]): Set[(Organization, ModuleName)] =
    excludes.apar.map(toCoursierExclude).toSet

  @node private def toCoursierExclude(exclude: Exclude): (Organization, ModuleName) =
    (Organization(exclude.group.getOrElse("*")), ModuleName(exclude.name.getOrElse("*")))

  @node def resolveDependencies(deps: DependencyDefinitions): ResolutionResult = {
    // extra libs shouldn't be included for resolution, and they will be included in metadata & fingerprint
    val distinctDeps = distinctRequestedDeps(deps.all).filter(_.kind != ExtraLibDefinition)
    val (mavenDeps, afsDeps) = distinctDeps.partition(_.isMaven)
    val mappedAfsDepsResult = MavenUtils.applyDirectMapping(afsDeps, afsGroupNameToMavenMap)
    val distinctDepsWithMapping = (mappedAfsDepsResult.allDepsAfterMapping ++ mavenDeps).to(Seq)

    // note that requested dependencies always overwrite the defaults (only matters if they are variants requested),
    // and the first requested dependency version takes precedence over the others
    def groupPerModule(deps: Seq[DependencyDefinition]): Map[Module, DependencyDefinition] =
      deps.groupBy(toModule).map { case (k, vs) => k -> vs.head }

    val allVersions = Versions((defaultDefinitions ++ groupPerModule(deps.directIds)).mapValuesNow(_.version))

    // validate in used mix mode maven-only deps from jvm-dependencies.obt, ensure all transitive name already be mapped

    val mixModeErrors = {
      val inUsedMixModeDeps =
        externalDependencies.mavenDependencies.mixModeMavenDeps
          .intersect(mavenDeps ++ mappedAfsDepsResult.allDepsAfterMapping.filter(_.isMaven))
          .to(Seq)
      validateMixModeMavenDeps(inUsedMixModeDeps, allVersions, mappedAfsDepsResult)
    }

    val extraPublications: Map[Module, Seq[Publication]] =
      (defaultDefinitions ++ groupPerModule(distinctDepsWithMapping)).collect {
        case (m, d) if d.ivyArtifacts.nonEmpty =>
          (
            m,
            d.ivyArtifacts.map(a =>
              Publication(name = a.name, `type` = Type(a.tpe), ext = Extension(a.ext), classifier = Classifier(""))))
      }

    val dependencies = toCoursierDeps(distinctDepsWithMapping, allVersions)

    val resolution = doResolution(dependencies, allVersions)

    val declaredPluginModules = dependencyDefinitions.withFilter(_.isScalacPlugin).map(toModule).toSet
    val macroModules = modulesDependedOnByMacros(resolution, deps.all)

    getArtifacts(
      resolution,
      dependencies,
      extraPublications,
      pluginModules = declaredPluginModules,
      macroModules = macroModules,
      mappingErrors = mixModeErrors,
      mappedDeps = mappedAfsDepsResult.appliedAfsToMavenMap
    )
  }

  @node private def parseRepo(ivy: String, artifactPatterns: Seq[String]): Repository = {
    if (NamingConventions.isHttpOrHttps(ivy)) {
      // note that the ivy pattern isn't a valid URI because it contains [variables] in the path part.
      // we strip those out so that we can extract the hostname without URI parsing failures.
      val host = new URI(ivy.replaceAllLiterally("[", "").replaceAllLiterally("]", "")).toURL.getHost
      val credentialOption = credentials.find(_.host == host)
      if (artifactPatterns.exists(p => UnzipMavenRepoExts.exists(ext => p.contains(s".$ext!")))) {
        UnzipFileRepository
          .parse(ivy, artifactPatterns, credentialOption, remoteAssetStore, dependencyCopier)
          .toOption
          .getOrElse(throw new Exception(s"UnzipFileRepository failed to parse $ivy -> $artifactPatterns"))
      } else {
        val auth = credentialOption.map(c => Authentication(user = c.user, password = c.password))
        MavenRepository(root = ivy, authentication = auth)
      }
    } else
      MsIvyRepository
        .parse(
          ivy,
          artifactPatterns,
          fileSystem = fileSystem,
          afsGroupNameToMavenMap = afsGroupNameToMavenMap
        )
        .toOption
        .getOrElse(throw new Exception(s"failed for $ivy -> $artifactPatterns"))
  }

  @node private def repos(module: Module): Seq[Repository] =
    for {
      resolver <- depModuleToResolversMap.getOrElse(module, resolvers.defaultResolvers).apar
      metadata <- resolver.metadataPatterns.apar
    } yield parseRepo(metadata.urlPattern, resolver.artifactPatterns.map(_.urlPattern))

  private val LocalRepoUrl = new Extractor((u: String) =>
    localRepos.collectFirst { case (p, r) if u startsWith p => r })

  private def loadContent(path: Path): Either[String, String] = Right(Files.readString(path))

  /**
   * This is a customized Coursier:fetch to make obt support download maven files(.xml/.pom) into SilverKing. By
   * default OBT would try resolve maven artifacts in this order:
   *   1. local disk 2. silverking 3. maven
   */
  @entersGraph def doHttpFetch(artifact: Artifact): Either[String, String] = {
    val url: URL = new URI(artifact.url).toURL
    downloadUrl(url, dependencyCopier, remoteAssetStore)(asNode(d => loadContent(d.path)))(Left(_))
  }

  /**
   * Looks up local ivy-repo files in our in-memory LocalIvyRepo (mainly to ensure that the file we lookup is the same
   * file that we hashed in the resolver fingerprint). Falls back to defaultFetch for non-local ivys or fetch poms from
   * remote maven server.
   */
  private def fetchDepMetadata(artifact: coursier.util.Artifact): EitherT[Node, String, String] = artifact.url match {
    // local ivy files are loaded from memory to avoid TOCTOU issues between hashing and reading
    case LocalRepoUrl(MetadataPattern.Local(_, urlRepoRoot, _, contents)) =>
      val relpath = RelativePath(artifact.url.stripPrefix(urlRepoRoot))
      EitherT.fromEither {
        contents.localMetadataContent(relpath).toRight(s"File not found in local repo: $relpath")
      }
    // optimized path for file:// URLs which avoids inefficient Files.exists probing in Coursier
    case f if f.startsWith("file://") =>
      EitherT(CoursierGraphAdaptor.delay {
        val path = PathUtils.uriToPath(f, fileSystem)
        try Right(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
        catch {
          // cheaper to ask for forgiveness than permission (i.e. try/catch rather than File.exists)
          case _: NoSuchFileException => Left(s"File not found: ${artifact.url}")
          case e: FileSystemException => Left(s"FileSystem error: ${artifact.url}, $e")
        }
      })
    case _ => EitherT(CoursierGraphAdaptor.delay(doHttpFetch(artifact)))
  }

  // try fetch dependency files by Coursier api
  @async private def doCoursierFetch(
      module: Module,
      version: String,
      repos: Seq[Repository]): Either[Seq[String], (ArtifactSource, Project)] = {
    val (durationInNanos, fetchedResult) = AdvancedUtils.timed {
      asyncResult {
        asyncGet { fetchOne(repos, module, version, fetchDepMetadata, Nil).run }.left.map(_.toVector)
      }.recover { // capture Coursier side exception, eventually will be transferred to error CompilationMessage
        case e: SAXParseException => // we already downloaded correct file from remote but it's broken
          val nonParseable = "Coursier got non parseable metadata"
          log.debug(s"$nonParseable! ${StackUtils.multiLineStacktrace(e)}")
          DependencyDownloadTracker.addBrokenMetadata(cleanModuleStr(module))
          Left(Seq(s"$nonParseable file: $e"))
        case NonFatal(e) =>
          val fetchFailed = "Courier fetch failed"
          log.debug(s"$fetchFailed! ${StackUtils.multiLineStacktrace(e)}")
          DependencyDownloadTracker.addFailedMetadata(cleanModuleStr(module))
          Left(Seq(s"$fetchFailed with exception: $e"))
      }.value
    }
    DependencyDownloadTracker.addFetchDuration(cleanModuleStr(module), durationInNanos)
    fetchedResult.map { case (source, project) =>
      val modifiedProject = project.withDependencies(project.dependencies.apar.flatMop(applyExclusions))
      (source, modifiedProject)
    }
  }

  @node private def applyExclusions(confToDep: (Configuration, Dependency)): Option[(Configuration, Dependency)] = {
    val (fromConf, dep) = confToDep
    // we handle configSpecific exclusions ourselves because Coursier doesn't support exclusion by ivy configuration
    if (globalConfigSpecificExcludes.exists(isExcludedBy(dep, _))) None
    else {
      // for local exclusions, Coursier automatically propagates down exclusions from the originally requested
      // dependency, but we also add in any exclusions specified for this dependency
      val exclusions = dependencySpecificCoursierExcludes.getOrElse(toDependencyCoursierKey(dep), Set.empty)
      val updatedDep = if (exclusions.nonEmpty) dep.withExclusions(exclusions ++ dep.exclusions) else dep
      Some((fromConf, updatedDep))
    }
  }

  private def isExcludedBy(dep: Dependency, ex: Exclude): Boolean = {
    // n.b. Option#forall always returns true on None
    ex.group.forall(_ == dep.module.organization.value) &&
    ex.name.forall(_ == dep.module.name.value) &&
    ex.ivyConfiguration.forall(_ == dep.configuration.value)
  }

  // it's a node so that we can cache the find across all repos - this is valuable because we'll probably request the
  // same module and version in many different scopes
  @node private def findModuleInRepos(module: Module, version: String): Either[Seq[String], (ArtifactSource, Project)] =
    doCoursierFetch(module, version, repos(module))

  @node private def doResolution(directDeps: Seq[Dependency], forceVersions: Versions): Resolution = {

    val res = Resolution(dependencies = directDeps.toSet.toSeq).withForceVersions(forceVersions.allVersions)

    asyncGet(res.process.run[Node] { modulesVersions =>
      CoreAPI.nodify(modulesVersions.apar.map { case (module, version) =>
        (module, version) -> findModuleInRepos(module, version)
      })
    })
  }

  private def jniPathsForDependency(
      resolution: Resolution,
      dep: Dependency
  ): Seq[String] = {
    resolution.projectCache
      .get(dep.moduleVersion)
      .map { case (_, origProj) =>
        origProj.module.attributes
          .get(NativeIvyConstants.JNIPathAttrib)
          .map(_.split(NativeIvyConstants.JNIPathSepRegex))
          .getOrElse(Array())
          .toIndexedSeq
      }
      .getOrElse(Nil)
  }

  private def moduleLoadsForDependency(resolution: Resolution, dependency: Dependency): Seq[String] = {
    resolution.projectCache
      .get(dependency.moduleVersion)
      .map { case (_, origProj) =>
        origProj.module.attributes.get(NativeIvyConstants.ModuleLoadAttrib).toIndexedSeq
      }
      .getOrElse(Nil)
  }

  @node private def artifactsForDependency(
      resolution: Resolution,
      dep: Dependency,
      extraPublications: Map[Module, Seq[Publication]],
      pluginModules: Set[Module],
      macroModules: Set[Module]): Seq[Either[CompilationMessage, ExternalClassFileArtifact]] = {
    resolution.projectCache
      .get(dep.moduleVersion)
      .map { case (source, origProj) =>
        if (!dep.optional) {
          val containsPlugin = pluginModules.contains(dep.module)
          val containsOrUsedByMacros = macroModules.contains(dep.module)
          val proj = extraPublications.get(dep.module) match {
            case Some(extra) => origProj.withPublications(origProj.publications ++ extra.map((Configuration.all, _)))
            case None        => origProj
          }

          val artifacts = source match {
            case msIvy: AsyncArtifactSource =>
              msIvy.artifactsNode(dep, proj, None)
            case _ =>
              source.artifacts(dep, proj, None).map { case (publication, artifact) =>
                Right(CoursierArtifact(artifact, publication))
              }
          }

          val hasMavenArtifacts = artifacts.exists { e =>
            e.exists(art => art.url.contains(NamingConventions.MavenUrlRoot))
          }

          artifacts.toIndexedSeq.apar.collect {
            case Right(artifact) if MsIvyRepository.isClassJar(artifact) =>
              Right(
                convertArtifact(
                  source,
                  dep,
                  proj,
                  artifact,
                  containsPlugin = containsPlugin,
                  containsOrUsedByMacros = containsOrUsedByMacros,
                  isMaven = hasMavenArtifacts)
              )
            case Left(error) =>
              Left(CompilationMessage.error(s"Failed to resolve ${proj.module}#${proj.version}: $error"))
          }
        } else Nil
      }
      .getOrElse(Nil)
  }

  private def toAsset(a: Artifact): JarAsset =
    if (NamingConventions.isHttpOrHttps(a.url)) JarAsset(new URI(a.url).toURL())
    else JarAsset(PathUtils.uriToPath(a.url, fileSystem))

  @node private def downloadMavenSrcDocClassifier(
      source: ArtifactSource,
      dep: Dependency,
      proj: Project,
      classifier: Classifier): Option[JarAsset] =
    source.artifacts(dep, proj, Some(Seq(classifier))).headOption.flatMap { case (p, art) =>
      val url = new URI(art.url).toURL
      // quick search on src and doc files, normal url should return within 1s
      downloadUrl(url, dependencyCopier, remoteAssetStore, timeoutSec = maxSrcDocDownloadSeconds)(asNode(f =>
        if (f.exists) Some(toAsset(art)) else None))(_ => None)
    }

  @node private def convertArtifact(
      source: ArtifactSource,
      dep: Dependency,
      proj: Project,
      artifact: CoursierArtifact,
      containsPlugin: Boolean,
      containsOrUsedByMacros: Boolean,
      isMaven: Boolean): ExternalClassFileArtifact = {
    val vid = VersionedExternalArtifactId(
      group = proj.module.organization.value,
      name = proj.module.name.value,
      version = proj.actualVersion,
      artifactName = artifact.url.split("/").last,
      ExternalArtifactType.ClassJar
    )

    val asset = toAsset(artifact.value)

    val sourceAsset =
      artifact.extra
        .get("src")
        .map(toAsset)
        .orElse(if (isMaven) downloadMavenSrcDocClassifier(source, dep, proj, Classifier.sources) else None)
    val javadocAsset = (artifact.extra.get("javadoc") orElse artifact.extra.get("doc"))
      .map(toAsset)
      .orElse(if (isMaven) downloadMavenSrcDocClassifier(source, dep, proj, Classifier.javadoc) else None)

    ExternalDependencyResolver
      .makeArtifact(
        vid,
        asset,
        sourceAsset,
        javadocAsset,
        containsPlugin = containsPlugin,
        containsOrUsedByMacros = containsOrUsedByMacros,
        isMaven = isMaven
      )
  }

  private def modulesDependedOnByMacros(
      resolution: Resolution,
      requestedDeps: Seq[DependencyDefinition]): Set[Module] = {
    val declaredModulesWithMacros =
      (dependencyDefinitions ++ requestedDeps).withFilter(_.containsMacros).map(toModule).toSet
    val depsWithMacros = resolution.dependencies.filter(d => declaredModulesWithMacros.contains(d.module))
    val transitiveDeps =
      depsWithMacros.flatMap(dep => resolution.dependenciesOf(dep, withRetainedVersions = true).toSet)
    declaredModulesWithMacros | transitiveDeps.map(_.module)
  }

  @node private def getArtifacts(
      resolution: Resolution,
      directDeps: Seq[Dependency],
      extraPublications: Map[Module, Seq[Publication]],
      pluginModules: Set[Module],
      macroModules: Set[Module],
      mappingErrors: Seq[CompilationMessage],
      mappedDeps: Map[DependencyDefinition, Seq[DependencyDefinition]]): ResolutionResult = {
    // keep direct dependencies in the order they are specified
    // (so that if we really need to force ordering we can do it by editing the order in the OBT files)
    val directArtifacts = directDeps.apar.flatMap { dependency =>
      artifactsForDependency(resolution, dependency, extraPublications, pluginModules, macroModules).map(
        (dependency, _))
    }
    // same for native paths and module loads
    val directJniPaths: Seq[String] = directDeps.flatMap(jniPathsForDependency(resolution, _))
    val directModuleLoads: Seq[String] = directDeps.flatMap(moduleLoadsForDependency(resolution, _))

    val indirectDependencies = resolution.minDependencies
      .filterNot(directDeps.toSet)
      .toIndexedSeq
      .sortBy(_.moduleVersion.toString())

    // sort the remaining artifacts by path (it's arbitrary but at least it's consistent)
    // get both from resolution and coursier resolver here.
    val indirectArtifacts =
      indirectDependencies.apar
        .flatMap { dependency =>
          artifactsForDependency(resolution, dependency, extraPublications, pluginModules, macroModules).map(
            (dependency, _))
        }
        .sortBy { case (dep, arts) =>
          (dep.module.toString, arts.right.toOption.map(_.file.pathFingerprint).getOrElse(""))
        }
    val indirectJniPaths: Seq[String] = indirectDependencies.apar.flatMap(jniPathsForDependency(resolution, _))
    val indirectModuleLoads: Seq[String] = indirectDependencies.apar.flatMap(moduleLoadsForDependency(resolution, _))

    val artifactsToDepInfos = mutable.LinkedHashMap.empty[ExternalClassFileArtifact, mutable.HashSet[DependencyInfo]]
    (directArtifacts ++ indirectArtifacts).foreach {
      case (d, Right(art)) =>
        artifactsToDepInfos.getOrElseUpdate(art, mutable.HashSet()) += coursierDependencyToInfo(d)
      case _ =>
    }
    val artifactsToDepInfosList = artifactsToDepInfos.toList
    val artifactsDependencies = artifactsToDepInfosList.flatMap(_._2)

    val allJniPaths = (directJniPaths ++ indirectJniPaths).distinct
    val allModuleLoads = (directModuleLoads ++ indirectModuleLoads).distinct

    val artifactMessages = directArtifacts.collect { case (d, Left(msg)) => msg }

    val conflictMessages = resolution.conflicts.groupBy(_.module).map { case (mod, deps) =>
      CompilationMessage(None, s"Conflicting dependency versions for $mod: $deps", CompilationMessage.Error)
    }

    val nonClassifierDeps = resolution.finalDependenciesCache.filter { case (dep, depends) =>
      dep.publication.classifier.isEmpty
    }

    // check self-dependent deps in coursier Resolution, we should not allow it.
    val selfDependentDeps = nonClassifierDeps.filter { case (dep, depends) => depends.contains(dep) }
    val selfDependentErrorMessages = selfDependentDeps.map { case (selfDep, deps) =>
      CompilationMessage.error(s"Self-dependent module ${selfDep.module}#${selfDep.version} detected! please fix it")
    }.toIndexedSeq

    val errorMessages = resolution.errors.map { case (mod, errors) =>
      CompilationMessage(
        None,
        s"Error resolving metadata for dependency $mod:${errors.mkString(start = "\n", sep = ";\n", end = "")}",
        CompilationMessage.Error)
    }.toIndexedSeq ++ selfDependentErrorMessages ++ mappingErrors.toIndexedSeq

    // coursier resolved transitive dependencies relationship map, be used for obt visualizer
    val finalDependencies: Map[DependencyInfo, Seq[DependencyInfo]] = {
      // some special dependencies like maven classifier would add duplications and self-dependent paths,
      // which not ideal for obtv and we should deduplicate them by ourself (Coursier won't help in this case)
      nonClassifierDeps.apar.map { case (d, seqd) =>
        (coursierDependencyToInfo(d), seqd.map(coursierDependencyToInfo).toIndexedSeq)
      }
    }

    val directMappedDependencies = mappedDeps.apar.map { case (afs, equivalents) =>
      definitionToInfo(afs) -> equivalents.map(definitionToInfo)
    }

    val transitiveMappedDependencies =
      externalDependencies.multiSourceDependencies.apar.collect {
        case ExternalDependency(afs, equivalents)
            if equivalents.map(definitionToInfo).forall(artifactsDependencies.contains) =>
          definitionToInfo(afs) -> equivalents.map(definitionToInfo)
      }.toMap

    ResolutionResult(
      resolvedArtifactsToDepInfos = artifactsToDepInfosList.map { case (a, ds) => (a, ds.to(Seq).sortBy(_.toString)) },
      messages = errorMessages ++ conflictMessages ++ artifactMessages,
      jniPaths = allJniPaths,
      moduleLoads = allModuleLoads,
      finalDependencies = finalDependencies,
      mappedDependencies = directMappedDependencies ++ transitiveMappedDependencies
    )
  }
}

// wrapper so that hashcode is cached (to improve node cache performance)
@stable final case class Versions(allVersions: Map[Module, String])

@entity object CoursierInterner {
  @node def internedDependency(d: Dependency): Dependency = d
  @entersGraph def interned(d: Dependency): Dependency = internedDependency(d)
}

final case class CoursierArtifact(value: Artifact, publication: Publication) {
  def attributes = publication.attributes
  def authentication = value.authentication
  def changing = value.changing
  def checksumUrls = value.checksumUrls
  def extra = value.extra
  def isOptional = value.optional
  def `type` = publication.`type`
  def url = value.url
}
