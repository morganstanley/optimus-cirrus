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

import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.resolvers.WebDependencyResolver.emptyResultError
import optimus.buildtool.utils.AssetUtils.loadFileAssetToString
import optimus.platform._
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import spray.json.JsValue

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.control.NonFatal
import scala.util.matching.Regex

object WebDependencyResolver {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val MaxRetry = 3
  private val WebYamlFileName = "pnpm-lock.yaml"
  private val WebJsonFileName = "package-lock.json"
  val DevDepKey = "dev"
  val DependenciesKey = "dependencies"
  val VersionKey = "version"
  val PackagesKey = "packages"
  val LocalFileKey = "link:"

  private[resolvers] def retryMsg(retry: Int, exception: String) =
    s"Retrying $retry/$MaxRetry - web dependencies metadata load failed! with exception: $exception"

  private[resolvers] def emptyResultError(scopeId: String): String =
    s"[$scopeId] can't resolve any web dependencies, related $WebYamlFileName and $WebJsonFileName are not found!"

  def searchLockFile(config: ScopeConfiguration, fileName: String): Seq[FileAsset] =
    config.absWebSourceRoots.collect {
      case dir if dir.resolveFile(fileName).exists => dir.resolveFile(fileName)
    }

  @async def resolveWebInfo(
      idConfigs: Map[ScopeId, ScopeConfiguration],
      dependencyDefinitions: Seq[DependencyDefinition]): Map[ScopeId, WebScopeInfo] =
    idConfigs.apar.map { case (id, config) =>
      // all web modules are required to provide web yaml file
      val yamlFiles: Map[FileAsset, String] =
        searchLockFile(config, WebYamlFileName).map(f => f -> loadFileAssetToString(f)).toMap
      val packageLockFiles: Map[FileAsset, String] =
        if (yamlFiles.isEmpty) searchLockFile(config, WebJsonFileName).map(f => f -> loadFileAssetToString(f)).toMap
        else Map.empty
      // optional tooling dependencies, depend on user's config
      val nodeVariant = config.webConfig.flatMap(_.nodeVariant)
      val nodeDependency = dependencyDefinitions
        .find(d => d.group == NpmGroup && d.name == NpmName && d.variant.map(_.name) == nodeVariant)
        .map(_.copy(transitive = false))
      val pnpmVariant = config.webConfig.flatMap(_.pnpmVariant)
      val pnpmDependency = dependencyDefinitions
        .find(d => d.group == PnpmGroup && d.name == PnpmName && d.variant.map(_.name) == pnpmVariant)
        .map(_.copy(transitive = false))
      if (yamlFiles.isEmpty && packageLockFiles.isEmpty && nodeDependency.isEmpty && pnpmDependency.isEmpty)
        throw new IllegalArgumentException(emptyResultError(id.toString))
      id -> WebScopeInfo(yamlFiles, packageLockFiles, nodeDependency, pnpmDependency)
    }

  def safeContentParse[T](content: String, retry: Int = MaxRetry)(f: => T): T = try { f }
  catch {
    case NonFatal(e) =>
      val failedMsg = retryMsg(retry, e.getMessage)
      if (retry > 0) {
        log.warn(failedMsg)
        safeContentParse(content, retry - 1)(f)
      } else {
        log.error(s"unable to parse web dependencies content: \n$content")
        throw new IllegalArgumentException(failedMsg)
      }
  }

  /**
   * remove unused additional info in web dependency's version string
   * @param rawVersion
   *   for example: "26.2.0_additional" or "26.2.0(somelibs@2.6.2)"
   * @return
   *   version string like "26.2.0"
   */
  def getActualVersion(rawVersion: String): String = {
    val result = rawVersion match {
      case withSubDeps if rawVersion.contains("(") && rawVersion.contains(")") =>
        withSubDeps.split("\\(", 2)(0)
      case withIntegrity if rawVersion.contains("_") =>
        withIntegrity.split("_", 2)(0)
      case _ => rawVersion
    }
    if (result.contains("@") || (!result.contains(LocalFileKey) && result.contains("/")))
      throw new IllegalArgumentException(s"$rawVersion is invalid!")
    else result
  }

  def getMetaProject(input: String): (String, String) = {
    val depStr = if (input.last == '/') input.dropRight(1) else input
    val (meta, project) = if (depStr.contains("/")) {
      val strList = depStr.split("/", 2)
      if (depStr.startsWith(MsWebDependencyMeta)) (MsWebDependencyDefaultMeta, strList(1))
      else (strList(0), strList(1))
    } else ("3rd", depStr)
    if (meta.contains("/") || project.contains("/") || project.contains("@"))
      throw new IllegalArgumentException(s"$input is invalid!")
    else (meta, project)
  }

  @node def getWebDependencies(
      directDependencies: Set[DependencyDefinition],
      packageDependencies: Set[PackageDependencyInfo]): Set[WebDependency] = {

    def getParents(target: PackageDependencyInfo): Set[PackageDependencyInfo] = packageDependencies.collect {
      case dep if dep.dependencies.contains(target.copy(dependencies = Set.empty)) => dep
    }
    def isTransitive(target: PackageDependencyInfo): Boolean = !directDependencies.contains(target.toDefinition(false))

    packageDependencies.apar.map { packageDep =>
      WebDependency(
        packageDep.toDefinition(isTransitive(packageDep)),
        getParents(packageDep).map(parent => parent.toDefinition(isTransitive(parent))))
    }
  }

}

object YamlResolver {
  import optimus.buildtool.resolvers.WebDependencyResolver._
  private val SpecifierKey = "specifier"

  @node private def getDirectDependencies(yamlContent: Option[Object]): Set[DependencyDefinition] = {
    def toDependencyDefinition(dependency: String, version: String, isTransitive: Boolean): DependencyDefinition = {
      val (meta, project) = getMetaProject(dependency)
      DependencyDefinition(
        meta,
        project,
        getActualVersion(version),
        LocalDefinition,
        transitive = isTransitive
      )
    }

    yamlContent match {
      case Some(deps) =>
        val loadedDepsMap =
          if (deps.toString.contains(SpecifierKey) && deps.toString.contains(VersionKey)) // for newer dependency format
            deps
              .asInstanceOf[java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]]
              .asScala
              .map { case (depStr, properties) => depStr -> properties.get(VersionKey).toString }
          else
            deps
              .asInstanceOf[java.util.LinkedHashMap[String, String]]
              .asScala

        loadedDepsMap.toMap.apar.map { case (name, version) =>
          toDependencyDefinition(name, version, isTransitive = false)
        }.toSet
      case None =>
        Set.empty
    }
  }

  /**
   * load package dependency string and parse it to PackageDependencyInfo
   * @param depString
   *   for example /@meta/name/1.1.0_sub@abc or @meta/name@1.1.0(subDep)
   * @param definitions
   *   sub definitions such as: "resolution", "dev", "dependencies"
   * @return
   *   PackageDependencyInfo
   */
  @node private def getPackageDependency(depString: String, definitions: Map[String, Object]): PackageDependencyInfo = {
    val depStringWithoutSubDeps =
      if (depString.contains("(")) depString.splitAt(depString.indexOf("("))._1 else depString

    val (meta, project, depVersion) = {

      val (first, second) = depStringWithoutSubDeps.splitAt(depStringWithoutSubDeps.lastIndexOf("/"))
      val withSubRegex = new Regex(".*[^.]_.*") // for example my_.name
      val containSub = withSubRegex.findFirstMatchIn(second).isDefined
      if (second.contains("@") && !containSub) // newer dependency format
        {
          val (name, version) = second.splitAt(second.indexOf("@"))
          val (meta, project) = getMetaProject(first + name)
          (meta, project, version)
        } else {
        val (meta, project) = getMetaProject(first)
        (meta, project, second)
      }
    }
    val release = getActualVersion(depVersion.substring(1))
    val subDependencies: Set[PackageDependencyInfo] = definitions.get(DependenciesKey) match {
      case Some(depList) =>
        depList
          .asInstanceOf[java.util.LinkedHashMap[String, String]]
          .asScala
          .toMap
          .apar
          .map { case (name, version) =>
            val (meta, project) = getMetaProject(name)
            PackageDependencyInfo(meta, project, getActualVersion(version))
          }
          .toSet
      case None => Set.empty
    }
    PackageDependencyInfo(meta, project, release, subDependencies)
  }

  @node def loadYaml(content: String): Set[WebDependency] = {
    val yaml = new Yaml()
    val yamlMap = safeContentParse(content) { yaml.load[java.util.Map[String, Object]](content).asScala.toMap }
    // dependencies section should reflect the version that is installed and used by web cmd
    // we should ignore localDependencies metadata since they are inside of module dir
    val (localDependencies, directDependencies) =
      getDirectDependencies(yamlMap.get(DependenciesKey)).partition(_.version.contains(LocalFileKey))
    // packages section is all resolved web dependency tree info, for metadata we should includes all non-dev deps
    val packages: Map[String, Map[String, Object]] = yamlMap
      .get(PackagesKey) match {
      case Some(packages) =>
        packages
          .asInstanceOf[java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]]
          .asScala
          .map { case (k, v) => k -> v.asScala.toMap }
          .apar
          .collect {
            case (k, v) if !v.contains(DevDepKey) || !v.getOrElse(DevDepKey, "false").asInstanceOf[Boolean] =>
              (k.substring(1), v)
          }
          .toMap
      case None => Map.empty
    }
    val packageDependencies: Set[PackageDependencyInfo] =
      packages.apar.map { case (dep, definitions) =>
        getPackageDependency(dep, definitions)
      }.toSet
    getWebDependencies(directDependencies, packageDependencies)
  }
}

object JsonResolver {
  import optimus.buildtool.resolvers.WebDependencyResolver._
  private val depStrSplitter = "node_modules/"
  private val versionKey = "version"
  private val localFileKey = "file:"

  @node private def jsonInfoToPackageDependencies(
      jsonDeps: Map[(String, String, String), JsValue],
      nameVersionMap: Map[(String, String), String]): Set[PackageDependencyInfo] =
    jsonDeps.apar.map { case ((meta, project, version), properties) =>
      val propertiesMap = properties.asJsObject.fields
      val release = version.replaceAll("\"", "")
      val subDependencies: Set[PackageDependencyInfo] = propertiesMap.get(DependenciesKey) match {
        case Some(subDeps) =>
          val subDepsMap = subDeps.asJsObject.fields.map { case (k, v) => k -> v.toString }
          subDepsMap.keySet
            .map { name =>
              val (meta, project) = getMetaProject(name)
              val release = nameVersionMap.getOrElse((meta, project), subDepsMap(name)).replaceAll("\"", "")
              PackageDependencyInfo(meta, project, release, Set.empty)
            }
        case None => Set.empty
      }
      PackageDependencyInfo(meta, project, release, subDependencies)
    }.toSet

  @node def loadJson(content: String): Set[WebDependency] = {
    import spray.json._
    val packagesContent = safeContentParse(content) {
      content.parseJson.asJsObject.fields(PackagesKey).asJsObject.fields
    }
    val packages: Map[(String, String, String), JsValue] = packagesContent.-("").flatMap { case (name, properties) =>
      val (first, rest) = name.split(depStrSplitter).filter(_.nonEmpty).splitAt(1)
      val (group, firstName) =
        getMetaProject(
          first.headOption.getOrThrow(s"invalid package-lock.json dependency! $name , ${properties.prettyPrint}"))
      val loadedVersion = properties.asJsObject.fields(versionKey).toString
      val loadedNames = rest.map(d => getMetaProject(d)) :+ (group, firstName)
      loadedNames.map { case (meta, project) => (meta, project, loadedVersion) -> properties }
    }
    val directDependencyNames: Set[(String, String)] =
      packagesContent("").asJsObject
        .fields(DependenciesKey)
        .asJsObject
        .fields
        .filter { case (nameStr, version) => !version.toString.contains(localFileKey) }
        .keySet
        .map(getMetaProject)
    val nameVersionMap: Map[(String, String), String] = packages.apar.map {
      case ((meta, project, release), properties) =>
        (meta, project) -> release
    }
    // for release build we should only consider non-dev deps
    val nonDevDependencies: Map[(String, String, String), JsValue] = packages.filter { case (nameStr, properties) =>
      !properties.asJsObject.fields.getOrElse(DevDepKey, "false").toString.toBoolean
    }
    val (directDependencies, transitiveDependencies): (Set[PackageDependencyInfo], Set[PackageDependencyInfo]) = {
      val (direct, trans) = nonDevDependencies.partition { case ((meta, project, release), v) =>
        directDependencyNames.contains(meta, project)
      }
      (jsonInfoToPackageDependencies(direct, nameVersionMap), jsonInfoToPackageDependencies(trans, nameVersionMap))
    }

    getWebDependencies(directDependencies.map(_.toDefinition(false)), directDependencies ++ transitiveDependencies)
  }
}

@entity class WebDependencyResolver(webInfoBeforeBuild: Map[ScopeId, WebScopeInfo]) {
  import optimus.buildtool.resolvers.YamlResolver._
  import optimus.buildtool.resolvers.JsonResolver._

  @node def resolveWebDependencies(idConfigs: Map[ScopeId, ScopeConfiguration]): WebResolution =
    if (idConfigs.nonEmpty) {
      val foundWebInfo: Set[WebScopeInfo] = idConfigs.map { case (id, conf) =>
        webInfoBeforeBuild.getOrElse(id, throw new IllegalArgumentException(emptyResultError(id.toString)))
      }.toSet
      val result = foundWebInfo.apar.map { webInfo =>
        val loadedYamlDeps = webInfo.yamlFiles.apar.flatMap { case (asset, content) => loadYaml(content) }
        val loadedJsonDeps = if (loadedYamlDeps.isEmpty) webInfo.jsonFiles.apar.flatMap { case (asset, content) =>
          loadJson(content)
        }
        else Set.empty
        val nodeDependencies = webInfo.nodeDependency
        val pnpmDependencies = webInfo.pnpmDependency
        (loadedYamlDeps ++ loadedJsonDeps, nodeDependencies ++ pnpmDependencies)
      }
      WebResolution(webDependencies = result.flatMap(_._1), tools = result.flatMap(_._2))
    } else WebResolution(Set.empty, Set.empty)

}

final case class WebScopeInfo(
    yamlFiles: Map[FileAsset, String],
    jsonFiles: Map[FileAsset, String],
    nodeDependency: Option[DependencyDefinition],
    pnpmDependency: Option[DependencyDefinition])

final case class WebDependency(definition: DependencyDefinition, parents: Set[DependencyDefinition])

final case class PackageDependencyInfo(
    meta: String,
    project: String,
    release: String,
    dependencies: Set[PackageDependencyInfo] = Set.empty) {
  def toDefinition(isTransitive: Boolean): DependencyDefinition = {
    DependencyDefinition(
      meta,
      project,
      release,
      LocalDefinition,
      transitive = isTransitive
    )
  }
}

final case class WebResolution(webDependencies: Set[WebDependency], tools: Set[DependencyDefinition])
