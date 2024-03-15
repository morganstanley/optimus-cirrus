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
package optimus.buildtool.builders.postbuilders.metadata

import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.ExternalDependencyResolver
import optimus.buildtool.resolvers.ResolutionResult
import optimus.platform._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Seq

final case class DependenciesReport(dependencies: Set[DependencyReport])

final case class ExternalDependenciesReports(jvm: DependenciesReport, extraLibs: DependenciesReport)

object DependenciesReport {

  implicit val dependenciesReportFormat: RootJsonFormat[DependenciesReport] = jsonFormat1(DependenciesReport.apply)

  @node def apply(
      settings: MetadataSettings,
      configurations: Map[ScopeId, ScopeConfiguration]
  ): ExternalDependenciesReports = {
    val internalDeps = {
      val internalCompileDeps =
        toInternalDependenciesReport(configurations, settings)(Compile, _.internalCompileDependencies)
      val internalRuntimeDeps =
        toInternalDependenciesReport(configurations, settings)(Runtime, _.internalRuntimeDependencies)
      DependencyReport.merge(internalCompileDeps, internalRuntimeDeps)
    }

    val (externalDeps, externalWebDeps) = {
      val externalCompileDeps =
        toExternalDependenciesReport(configurations)(Compile, _.externalCompileDependencies, settings)
      val externalRuntimeDeps =
        toExternalDependenciesReport(configurations)(Runtime, _.externalRuntimeDependencies, settings)
      (
        DependencyReport.merge(externalCompileDeps.jvm.dependencies, externalRuntimeDeps.jvm.dependencies),
        DependencyReport.merge(externalCompileDeps.extraLibs.dependencies, externalRuntimeDeps.extraLibs.dependencies))
    }

    ExternalDependenciesReports(DependenciesReport(internalDeps ++ externalDeps), DependenciesReport(externalWebDeps))
  }

  private def isTest(scopeId: ScopeId): Boolean = scopeId.tpe.toLowerCase().contains("test")

  private def toInternalDependenciesReport(
      configurations: Map[ScopeId, ScopeConfiguration],
      settings: MetadataSettings
  )(
      qualifier: QualifierReport,
      depExtractor: ScopeConfiguration => Seq[ScopeId]
  ): Set[DependencyReport] = {
    val (testConfigurations, nonTestConfigurations) = {
      val (test, nonTest) = configurations.partition { case (id, _) => isTest(id) }
      (test.values, nonTest.values)
    }

    val nonTestDeps = nonTestConfigurations.flatMap(depExtractor).toSet
    val testOnlyDeps = testConfigurations.flatMap(depExtractor).toSet.diff(nonTestDeps)
    nonTestDeps.map(DependencyReport(_, Set(qualifier), settings)) ++ testOnlyDeps.map(
      DependencyReport(_, Set(qualifier, TestOnly), settings))
  }

  @node private def getDepsReports(
      settings: MetadataSettings,
      qualifier: QualifierReport,
      deps: Set[DependencyDefinition],
      testDeps: Set[DependencyDefinition]): Set[DependencyReport] = {

    def extraLibsReports(libs: Set[DependencyDefinition], qualifiers: Set[QualifierReport]): Set[DependencyReport] =
      libs.filter(_.isExtraLib).map(DependencyReport.fromExtraLib(_, qualifiers))

    val nonTestReports = resolveExternalDependencies(deps, settings.dependencyResolver).apar.flatMap {
      case (depDef, resolution) =>
        DependencyReport.fromExternalResolution(depDef, resolution, Set(qualifier), settings)
    } ++ extraLibsReports(deps, Set(qualifier))

    val testQualifier = Set(qualifier, TestOnly)
    val testReports = resolveExternalDependencies(testDeps, settings.dependencyResolver).apar.flatMap {
      case (depDef, resolution) =>
        DependencyReport.fromExternalResolution(depDef, resolution, testQualifier, settings)
    } ++ extraLibsReports(testDeps, testQualifier)

    nonTestReports ++ testReports
  }

  @node private def getWebDepsReports(
      settings: MetadataSettings,
      qualifier: QualifierReport,
      configs: Map[ScopeId, ScopeConfiguration]): Set[DependencyReport] = {
    val resolution = settings.webDependencyResolver.resolveWebDependencies(configs)
    val reports = resolution.webDependencies.apar.map(DependencyReport.fromWebDependency(_, Set(qualifier)))
    val toolingReports =
      resolution.tools.apar.map(DependencyReport.fromWebToolingDefinition(_, Set(Tooling)))
    val webLibsReport = configs.apar.flatMap { case (id, conf) =>
      conf.webConfig.map(_.libs) match {
        case Some(libs) => getNpmLibsReports(id, libs, settings, qualifier)
        case None       => Set[DependencyReport]()
      }
    }

    reports ++ toolingReports ++ webLibsReport
  }

  @node private def getNpmLibsReports(
      id: ScopeId,
      libs: Seq[String],
      settings: MetadataSettings,
      qualifier: QualifierReport): Set[DependencyReport] = libs
    .filter(d => !d.contains(s"$NpmGroup.$NpmName") && !d.contains(s"$PnpmGroup.$PnpmName"))
    .map { str =>
      val splitStr = str.split("\\.")
      val variant = if (splitStr.contains("variant")) Some(splitStr.last) else None
      val afsElectronLibs = settings.dependencyResolver.extraLibsDefinitions
        .find(d => d.group == splitStr(0) && d.name == splitStr(1) && d.variant.map(_.name) == variant)
        .map(_.copy(transitive = false))
        .getOrThrow(s"can't find related extra lib $str for $id")
      DependencyReport.fromWebToolingDefinition(afsElectronLibs, Set(qualifier))
    }
    .toSet

  @node private def getElectronReports(
      settings: MetadataSettings,
      qualifier: QualifierReport,
      configs: Map[ScopeId, ScopeConfiguration]): Set[DependencyReport] = configs.apar.flatMap { case (id, conf) =>
    val electronLibReport: Set[DependencyReport] = conf.electronConfig.map(_.libs) match {
      case Some(libs) => getNpmLibsReports(id, libs, settings, qualifier)
      case None       => Set.empty
    }
    if (conf.electronConfig.flatMap(_.npmBuildCommands).isDefined)
      getWebDepsReports(settings, qualifier, Map(id -> conf)) ++ electronLibReport
    else electronLibReport
  }.toSet

  @node private def toExternalDependenciesReport(configurations: Map[ScopeId, ScopeConfiguration])(
      qualifier: QualifierReport,
      depExtractor: ScopeConfiguration => Seq[DependencyDefinition],
      settings: MetadataSettings
  ): ExternalDependenciesReports = {
    val (testConfs, confs) = configurations.partition { case (id, conf) => isTest(id) }

    def getDeps(cfgs: Map[ScopeId, ScopeConfiguration]): Set[DependencyDefinition] = cfgs.flatMap { case (id, conf) =>
      depExtractor(conf)
    }.toSet

    val (testOnlyDeps, deps) = {
      val (loadTestDeps, loadDeps) = (getDeps(testConfs), getDeps(confs))
      (loadTestDeps.diff(loadDeps), loadDeps)
    }
    val electronConfigs = configurations.filter { case (id, conf) => conf.electronConfig.isDefined }
    val webConfigs = configurations.filter { case (id, conf) => conf.webConfig.isDefined }

    val dependenciesReports = getDepsReports(settings, qualifier, deps, testOnlyDeps)
    val otherExtraLibsReports = getWebDepsReports(settings, qualifier, webConfigs) ++
      getElectronReports(settings, qualifier, electronConfigs)

    ExternalDependenciesReports(DependenciesReport(dependenciesReports), DependenciesReport(otherExtraLibsReports))
  }

  @node private def resolveExternalDependencies(
      deps: Set[DependencyDefinition],
      resolver: ExternalDependencyResolver
  ): Set[(DependencyDefinition, ResolutionResult)] = {
    deps.apar.map { dep =>
      val resolution =
        resolver.resolveDependencies(DependencyDefinitions(directIds = Seq(dep), indirectIds = Seq.empty))
      (dep, resolution)
    }
  }
}
