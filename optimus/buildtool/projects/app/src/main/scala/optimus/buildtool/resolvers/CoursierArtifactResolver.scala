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

import coursier.Dependency
import coursier.Module
import coursier._
import coursier.core.ArtifactSource
import coursier.core.Authentication
import coursier.core.Configuration
import coursier.core.Extension
import coursier.core.Publication
import coursier.core.Repository
import coursier.core.ResolutionProcess.fetchOne
import coursier.util.Artifact
import coursier.util.EitherT
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.ExternalArtifactType
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.cache.NoOpRemoteAssetStore
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.DependencyCoursierKey
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinition.DefaultConfiguration
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.Exclude
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.ExternalDependency
import optimus.buildtool.config.ExtraLibDefinition
import optimus.buildtool.config.GroupNameConfig
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.MappedDependencyDefinitions
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NamingConventions.UnzipMavenRepoExts
import optimus.buildtool.config.Substitution
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.resolvers.MavenUtils.downloadUrl
import optimus.buildtool.resolvers.MavenUtils.maxSrcDocDownloadSeconds
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.StackUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.core.CoreAPI
import optimus.graph.Node
import optimus.platform._
import optimus.scalacompat.collection._
import optimus.stratosphere.artifactory.Credential
import optimus.stratosphere.utils.Text
import org.xml.sax.SAXParseException

import java.net.URI
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystem
import java.nio.file.FileSystemException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.regex.Pattern
import scala.collection.compat._
import scala.collection.immutable.ListMap
import scala.collection.immutable.Seq
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
  // regex-ignore-line this one is only for codegen
  private[buildtool] val Marker: String = "TODO"
  private val mavenDefaultConfigs =
    Seq(Configuration.compile, Configuration.default, Configuration.defaultCompile, Configuration.runtime)
  private[buildtool] val ObtMavenDefaultConfig = ""

  private def jvmDepMsg(line: Int) = s"$JvmDependenciesFilePathStr:$line -"

  private[buildtool] def realConfigurationStr(rawConfiguration: String): String = {
    val configurationRegex = "^default[(](.+)[)]$".r
    configurationRegex.findFirstMatchIn(rawConfiguration) match {
      case Some(configuration) => configuration.group(1)
      case None                => rawConfiguration
    }
  }
  private def toDepString(in: String): String = if (in.contains(".")) Text.doubleQuote(in) else in

  private[buildtool] def coursierDepToNameString(d: Dependency, withVersion: Boolean = false): String = {
    val nameStr = s"${toDepString(d.module.organization.value)}.${toDepString(d.module.name.value)}"
    if (withVersion) s"$nameStr.${d.version}"
    else nameStr
  }

  private[buildtool] def getVariantsDeps(deps: Seq[DependencyDefinition]): Seq[DependencyDefinition] =
    deps.filter(d => d.variant.exists(!_.configurationOnly))

  private[buildtool] def invalidUnmappedDepMsg(
      line: Int,
      depName: String,
      unmappedResult: UnmappedResult,
      autoMappedResult: Option[AutoMappedResult] = None): String = {

    def toMsg(deps: Set[Dependency]): String = deps.toList
      .sortBy(_.module.name.value)
      .map { d =>
        s"""${d.module.name.value} {
           |  // $Marker: add AFS mapping here in form of 'afs.<meta>.<project>.version = ${d.version}
           |  ${coursierDependencyToRule(Set(d), "maven")}
           |}
           |""".stripMargin
      }
      .mkString("\n")

    def coursierDependencyToRule(deps: Set[Dependency], prefix: String): String =
      deps.toList
        .sortBy(_.module.organization.value)
        .map(d => s"$prefix.${coursierDepToNameString(d)}.version = ${d.version}")
        .mkString("\n")

    def toMappingRules(autoAfsToMavenMap: Map[Dependency, Set[Dependency]]): String =
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

    val leafMapMsg = toMsg(unmappedResult.leafs)
    val autoMappedMsg = autoMappedResult match {
      case Some(auto) if auto.autoAfsToMavenMap.nonEmpty =>
        s"|Found ${auto.afs.size} afs libs could potentially be mapped to same name maven equivalents: ${toMappingRules(auto.autoAfsToMavenMap)}"
      case _ => ""
    }

    s"""|${jvmDepMsg(line)} '$depName' depends on ${unmappedResult.all.size} transitive unmapped dependencies:
        |
        |${toMsg(unmappedResult.all)}
        |Please add mapping rules for ${unmappedResult.leafs.size} leaf dependencies first:
        |
        |$leafMapMsg
    $autoMappedMsg""".stripMargin
  }

  def cleanModuleStr(module: Module): String = s"${module.organization.value}:${module.name.value}"

  def getConfig(config: Configuration, isMaven: Boolean) =
    if (isMaven && mavenDefaultConfigs.contains(config)) ObtMavenDefaultConfig else config.value

  def coursierDependencyToInfo(d: Dependency, isMaven: Boolean): DependencyInfo = {
    val config = getConfig(d.configuration, isMaven)
    DependencyInfo(d.module.organization.value, d.module.name.value, config, d.version, isMaven)
  }

  def definitionToInfo(d: DependencyDefinition): DependencyInfo = {
    DependencyInfo(d.group, d.name, getConfig(Configuration(d.configuration), d.isMaven), d.version, d.isMaven)
  }
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
    globalExcludes: Seq[Exclude] = Nil,
    globalSubstitutions: Seq[Substitution] = Nil,
    fileSystem: FileSystem = FileSystems.getDefault,
    credentials: Seq[Credential] = Nil,
    remoteAssetStore: RemoteAssetStore = NoOpRemoteAssetStore,
    enableMappingValidation: Boolean = true
) extends ExternalDependencyResolver(externalDependencies.definitions) {
  import CoursierArtifactResolver._

  // note that ivyConfiguration exclusions aren't supported natively by Coursier (we handle them ourselves in
  // applyExclusions instead)
  private val (globalConfigSpecificExcludes, globalNonConfigSpecificExcludes) =
    globalExcludes.partition(_.ivyConfiguration.isDefined)

  @node private def globalCoursierExcludes = toCoursierExcludes(globalNonConfigSpecificExcludes, Nil)

  @node private def getVersionLevelExcludesMap(
      deps: Seq[DependencyDefinition]): Map[DependencyCoursierKey, Set[(Organization, ModuleName)]] =
    deps.apar.map { d =>
      // note that JvmDependenciesLoader.readExcludes already checks for this and produces a nicer error - this check
      // is mostly here in case unit tests break the rules
      require(
        d.excludes.forall(_.ivyConfiguration.isEmpty),
        s"ivyConfiguration based excludes are only supported in global excludes (not in ${d.group}.${d.name})")

      DependencyCoursierKey(d.group, d.name, d.configuration, d.version) -> toCoursierExcludes(d.excludes, Nil)
    }.toMap

  @node private def dependencySpecificCoursierExcludes: DependencySpecificCoursierExcludes = {
    val (variants, defaults) = dependencyDefinitions.partition(_.variant.isDefined)
    val variantsVersionLevel = getVersionLevelExcludesMap(variants)
    val defaultsVersionLevel = getVersionLevelExcludesMap(defaults)
    val versionLevelMap = variantsVersionLevel ++ defaultsVersionLevel
    val nameLevelMap = defaultsVersionLevel.map { case (d, excludes) => d.copy(version = "") -> excludes }
    DependencySpecificCoursierExcludes(versionLevelMap, nameLevelMap)
  }

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
  private val afsGroupNameToMavenMap: Map[MappingKey, Seq[DependencyDefinition]] =
    externalDependencies.afsToMavenMap.map { case (afs, maven) =>
      MappingKey(afs.group, afs.name, afs.configuration) -> maven
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
    externalDependencies.multiSourceDependencies.apar
      .flatMap { dep =>
        fingerprintDependencyDefinitions("multi-source-afs", Seq(dep.definition)) ++
          fingerprintDependencyDefinitions(
            "maven-equivalents",
            dep.equivalents.toIndexedSeq) :+ s"enableValidation: $enableMappingValidation"
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

  @node override def fingerprintDependencies(deps: DependencyDefinitions): Seq[String] = {
    // the dependencies which were actually requested need to be fingerprinted in order (i.e. not sorted) because
    // their ordering affects the ordering of the output, but note that only the first variant of any given module matters
    // because subsequent ones are ignored in doResolveDependencies
    val requestedDepsFingerprint = fingerprintDependencyDefinitions("requested", distinctRequestedDeps(deps.all))
    val subsFingerprint = (globalSubstitutions ++ deps.substitutions).map { s =>
      def cfgStr(c: GroupNameConfig) = s"${c.group}.${c.name}${c.config.map(cfg => s".$cfg").getOrElse("")}"
      s"[Substitution]${cfgStr(s.from)} -> ${cfgStr(s.to)}"
    }
    resolvers.allResolvers.flatMap(_.fingerprint) ++ requestedDepsFingerprint ++ defaultDefinitionsFingerprint ++
      globalExcludesFingerprint ++ multiSourceDependenciesFingerprint ++ noVersionMavenDependenciesFingerprint ++
      subsFingerprint
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
  @node private def validateMixModeMavenDeps(
      mixedModeMavenDeps: Seq[DependencyDefinition],
      scopeSubstitutions: Seq[Substitution],
      allVersions: Versions,
      mappedResult: MappedDependencyDefinitions): Seq[CompilationMessage] =
    mixedModeMavenDeps.apar.flatMap { mixedModeMavenDep =>
      val coursierMavenDep = toCoursierDep(mixedModeMavenDep, scopeSubstitutions, allVersions)
      val resolution =
        doResolution(Seq(coursierMavenDep), getVariantsDeps(mixedModeMavenDeps), scopeSubstitutions, allVersions)
      val unmappedResult = getUnmappedResult(resolution)

      if (unmappedResult.all.nonEmpty) {
        val autoMap: Option[AutoMappedResult] = {
          val fromAfs = mappedResult.appliedAfsToMavenMap.apar
            .collect { case (k, v) if v.contains(mixedModeMavenDep) => k }
            .apar
            .map(toCoursierDep(_, scopeSubstitutions, allVersions))
            .to(Seq)
          // no variants for resolved trans result
          val afsMapToMavenTransDeps = doResolution(fromAfs, Nil, scopeSubstitutions, allVersions).dependencies
          generateAutoMappingRules(afsMapToMavenTransDeps, unmappedResult.all)
        }
        val msg = invalidUnmappedDepMsg(mixedModeMavenDep.line, mixedModeMavenDep.key, unmappedResult, autoMap)
        Some(CompilationMessage(None, msg, CompilationMessage.Error))
      } else None
    }.sorted

  @node private def toCoursierDeps(
      defs: Seq[DependencyDefinition],
      scopeSubstitutions: Seq[Substitution],
      allVersions: Versions): Seq[Dependency] =
    defs.apar.map { d => toCoursierDep(d, scopeSubstitutions, allVersions) }

  @node private def toCoursierDep(
      dep: DependencyDefinition,
      scopeSubstitutions: Seq[Substitution],
      allVersions: Versions): Dependency = {
    val module = toModule(dep)
    val coursierDependency = coursier
      .Dependency(
        // tag the Module with these data so we can propagate them into the eventual ClassFileArtifact
        module = module,
        // load predefined version in multipleSourceDeps
        version = allVersions.allVersions.getOrElse(module, dep.version)
      )
      .withConfiguration(Configuration(dep.configuration))
      .withExclusions(globalCoursierExcludes ++ toCoursierExcludes(dep.excludes, scopeSubstitutions))
      .withTransitive(dep.transitive)
    CoursierInterner.internedDependency(dep.classifier match {
      case Some(str) =>
        coursierDependency.withAttributes(Attributes(classifier = Classifier(str)))
      case None => coursierDependency
    })
  }

  @node private def toCoursierExcludes(
      excludes: Iterable[Exclude],
      scopeSubstitutions: Seq[Substitution]): Set[(Organization, ModuleName)] =
    excludes.apar.flatMap(toCoursierExclude(_, scopeSubstitutions)).toSet

  @node private def toCoursierExclude(
      exclude: Exclude,
      scopeSubstitutions: Seq[Substitution]): Set[(Organization, ModuleName)] = {

    def getMappedExcludes(f: MappingKey => Boolean) = afsGroupNameToMavenMap
      .collect { case (k, v) if f(k) => v.map(d => (Organization(d.group), ModuleName(d.name))) }
      .flatten
      .toSet
    def getMappedSubstitutions(f: Substitution => Boolean) = (globalSubstitutions ++ scopeSubstitutions).collect {
      case s if f(s) => (Organization(s.to.group), ModuleName(s.to.name))
    }

    val originalExclude = Set((Organization(exclude.group.getOrElse("*")), ModuleName(exclude.name.getOrElse("*"))))
    val mappedExcludes = exclude match {
      case Exclude(Some(group), Some(name), Some(cfg)) =>
        getMappedExcludes(k => k.group == group && k.name == name && k.configuration == cfg) ++
          getMappedSubstitutions(s =>
            s.from.group == group && s.from.name == name && s.from.config.getOrElse("") == cfg)
      case Exclude(Some(group), Some(name), None) =>
        getMappedExcludes(k => k.group == group && k.name == name) ++
          getMappedSubstitutions(s => s.from.group == group && s.from.name == name)
      case Exclude(Some(group), None, None) => getMappedExcludes(k => k.group == group)
      case Exclude(None, Some(name), None)  => getMappedExcludes(k => k.name == name)
      case _                                => Set.empty
    }
    originalExclude ++ mappedExcludes
  }

  @node def resolveDependencies(deps: DependencyDefinitions, validate: Boolean = true): ResolutionResult = {
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
    val mixModeErrors: Seq[CompilationMessage] = if (enableMappingValidation && validate) {
      val inUsedMixModeDeps =
        externalDependencies.mavenDependencies.mixModeMavenDeps
          .intersect(mavenDeps ++ mappedAfsDepsResult.allDepsAfterMapping.filter(_.isMaven))
          .to(Seq)
      validateMixModeMavenDeps(inUsedMixModeDeps, deps.substitutions, allVersions, mappedAfsDepsResult)
    } else Nil

    val extraPublications: Map[Module, Seq[Publication]] =
      (defaultDefinitions ++ groupPerModule(distinctDepsWithMapping)).collect {
        case (m, d) if d.ivyArtifacts.nonEmpty =>
          (
            m,
            d.ivyArtifacts.map(a =>
              Publication(name = a.name, `type` = Type(a.tpe), ext = Extension(a.ext), classifier = Classifier(""))))
      }

    val dependencies = toCoursierDeps(distinctDepsWithMapping, deps.substitutions, allVersions)

    // we don't allow user specify "AFS.variant.sth" for mapped jvm-deps, so it's safe to use directIds directly
    val obtFileDirectVariants = getVariantsDeps(deps.directIds)
    val resolution = doResolution(dependencies, obtFileDirectVariants, deps.substitutions, allVersions)

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
   * This is a customized Coursier:fetch to make obt support download maven files(.xml/.pom) into remote cache. By
   * default OBT would try resolve maven artifacts in this order:
   *   1. local disk 2. remote cache 3. maven
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
      repos: Seq[Repository],
      variants: Seq[DependencyDefinition]): Either[Seq[String], (ArtifactSource, Project)] = {
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
          val fetchFailed = "Coursier fetch failed"
          log.debug(s"$fetchFailed! ${StackUtils.multiLineStacktrace(e)}")
          DependencyDownloadTracker.addFailedMetadata(cleanModuleStr(module))
          Left(Seq(s"$fetchFailed with exception: $e"))
      }.value
    }
    DependencyDownloadTracker.addFetchDuration(cleanModuleStr(module), durationInNanos)
    fetchedResult.map { case (source, project) =>
      val modifiedProject = project.withDependencies(project.dependencies.apar.flatMop(applyExclusions(_, variants)))
      (source, modifiedProject)
    }
  }

  @node private def applyExclusions(
      confToDep: (Configuration, Dependency),
      variants: Seq[DependencyDefinition]): Option[(Configuration, Dependency)] = {
    val (fromConf, dep) = confToDep
    // we handle configSpecific exclusions ourselves because Coursier doesn't support exclusion by ivy configuration
    if (globalConfigSpecificExcludes.exists(isExcludedBy(dep, _))) None
    else {
      // for local exclusions, Coursier automatically propagates down exclusions from the originally requested
      // dependency, but we also add in any exclusions specified for this dependency
      val exclusions =
        dependencySpecificCoursierExcludes.get(toDependencyCoursierKey(dep), variants)
      val allExclusions = exclusions ++ dep.exclusions
      val updatedDep = if (exclusions.nonEmpty) dep.withExclusions(allExclusions) else dep
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
  @node private def findModuleInRepos(
      module: Module,
      version: String,
      variants: Seq[DependencyDefinition]): Either[Seq[String], (ArtifactSource, Project)] =
    doCoursierFetch(module, version, repos(module), variants)

  @node private def substitutionsMap(
      scopeSubstitutions: Seq[Substitution],
      forceVersions: Versions): Map[GroupNameConfig, Dependency] = {
    def getDepDef(gn: GroupNameConfig): DependencyDefinition = externalDependencies.definitions
      .find { d =>
        gn.config match {
          case Some(cfg) => d.group == gn.group && d.name == gn.name && d.configuration == cfg // cfg level mapping
          case None      => d.group == gn.group && d.name == gn.name // group/name level mapping
        }
      }
      .getOrThrow(
        s"Invalid substitutions detected! ${gn.group}.${gn.name}.${gn.config.getOrElse(DefaultConfiguration)}")

    (globalSubstitutions ++ scopeSubstitutions).apar.map { case Substitution(from, to) =>
      from -> toCoursierDep(getDepDef(to), scopeSubstitutions, forceVersions)
    }.toMap
  }

  @node private def doResolution(
      directDeps: Seq[Dependency],
      variants: Seq[DependencyDefinition],
      scopeSubstitutions: Seq[Substitution],
      forceVersions: Versions): Resolution = {
    val mapTo = substitutionsMap(scopeSubstitutions, forceVersions)
    val res = Resolution(dependencies = directDeps.toSet.toSeq)
      .withForceVersions(forceVersions.allVersions)
      .withMapDependencies(
        Some { fromDep =>
          val nameLevel =
            mapTo.getOrElse(GroupNameConfig(fromDep.module.organization.value, fromDep.module.name.value), fromDep)
          val cfgThenNameLevel = mapTo.getOrElse(
            GroupNameConfig(
              fromDep.module.organization.value,
              fromDep.module.name.value,
              Some(realConfigurationStr(fromDep.configuration.value))),
            nameLevel)
          cfgThenNameLevel
        }
      )

    asyncGet(res.process.run[Node] { modulesVersions =>
      CoreAPI.nodify(modulesVersions.apar.map { case (module, version) =>
        (module, version) -> findModuleInRepos(module, version, variants)
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

  // we won't download target maven jar again after first time check
  @node private def checkMavenJar(isMaven: Boolean, artifact: CoursierArtifact): Boolean =
    if (!isMaven) true
    else
      downloadUrl(new URI(artifact.url).toURL, dependencyCopier, remoteAssetStore)(asNode(f => f.exists))(_ => false)

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
          val containsAgent = false
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
            case Right(artifact)
                if MsIvyRepository.isClassJar(artifact) && checkMavenJar(hasMavenArtifacts, artifact) =>
              Right(
                convertArtifact(
                  source,
                  dep,
                  proj,
                  artifact,
                  containsPlugin = containsPlugin,
                  containsAgent = containsAgent,
                  containsOrUsedByMacros = containsOrUsedByMacros,
                  isMaven = hasMavenArtifacts
                )
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
      containsAgent: Boolean,
      containsOrUsedByMacros: Boolean,
      isMaven: Boolean): ExternalClassFileArtifact = {
    val vid = VersionedExternalArtifactId(
      group = proj.module.organization.value,
      name = proj.module.name.value,
      version = proj.actualVersion,
      artifactName = artifact.url.split("/").last,
      ExternalArtifactType.ClassJar,
      isMaven
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
        containsAgent = containsAgent,
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

    val mavenDependencies = resolution.projectCache
      .collect {
        case ((module, version), (source, proj)) if source.isInstanceOf[MavenRepository] =>
          proj.dependencies.map { case (c, d) => // do not take version/configuration to identify maven library
            DependencyCoursierKey(d.module.organization.value, d.module.name.value, "", "")
          } :+ DependencyCoursierKey(module.organization.value, module.name.value, "", "")
      }
      .flatten
      .toSet

    def isMavenDep(d: Dependency): Boolean =
      d.publication.`type` == coursier.core.Type.pom || mavenDependencies.contains(
        DependencyCoursierKey(d.module.organization.value, d.module.name.value, "", ""))

    val artifactsToDepInfos = mutable.LinkedHashMap.empty[ExternalClassFileArtifact, mutable.HashSet[DependencyInfo]]
    (directArtifacts ++ indirectArtifacts).foreach {
      case (d, Right(art)) =>
        artifactsToDepInfos.getOrElseUpdate(art, mutable.HashSet()) += coursierDependencyToInfo(d, art.isMaven)
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
      nonClassifierDeps
        .to(Seq)
        .apar
        .map { case (d, seqd) =>
          (
            coursierDependencyToInfo(d, isMavenDep(d)),
            seqd.map(sd => coursierDependencyToInfo(sd, isMavenDep(sd))).toIndexedSeq
          )
        // two [Coursier.Dependency] may share same DependencyInfo key, we must use to(Seq) then merge values
        }
        .groupBy(_._1)
        .map { case (k, grouped) => k -> grouped.flatMap(_._2).distinct }
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

// we should apply excludes for evicted dependency(name level), for example:
// say "foo.2.0" with predefined "excludes",
// in Coursier: "foo.1.0" (without excludes) -> evicted to "foo.2.0" (still without excludes) -> unexpected result
final case class DependencySpecificCoursierExcludes(
    versionLevel: Map[DependencyCoursierKey, Set[(Organization, ModuleName)]],
    nameLevel: Map[DependencyCoursierKey, Set[(Organization, ModuleName)]]) {
  def get(input: DependencyCoursierKey, variants: Seq[DependencyDefinition]): Set[(Organization, ModuleName)] = {
    val useVariants = variants
      .find(v => v.group == input.org && v.name == input.name)
      .map(d => DependencyCoursierKey(d.group, d.name, d.configuration, d.version))
    // apply variants excludes setting if be used in module.obt directly
    val key = useVariants.getOrElse(input)
    versionLevel.getOrElse(key, nameLevel.getOrElse(key.copy(version = ""), Set.empty))
  }
}
