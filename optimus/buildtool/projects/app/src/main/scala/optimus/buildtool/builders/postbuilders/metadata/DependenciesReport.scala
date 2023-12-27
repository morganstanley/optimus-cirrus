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

final case class DependencyType(maven: Boolean, test: Boolean)

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
      testDeps: Set[DependencyDefinition],
      resolver: ExternalDependencyResolver): Set[DependencyReport] = {
    val nonTestReports = resolveExternalDependencies(deps, resolver).apar.flatMap { case (depDef, resolution) =>
      DependencyReport.fromExternalResolution(depDef, resolution, Set(qualifier), settings)
    }
    val testReports = resolveExternalDependencies(testDeps, resolver).apar.flatMap { case (depDef, resolution) =>
      DependencyReport.fromExternalResolution(depDef, resolution, Set(qualifier, TestOnly), settings)
    }
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
      conf.webConfig.map(_.webLibs) match {
        case Some(libs) => getExtraLibsReports(id, libs, settings, qualifier)
        case None       => Set[DependencyReport]()
      }
    }

    reports ++ toolingReports ++ webLibsReport
  }

  @node private def getExtraLibsReports(
      id: ScopeId,
      libs: Seq[String],
      settings: MetadataSettings,
      qualifier: QualifierReport): Set[DependencyReport] = libs
    .filter(d => !d.contains(s"$NpmGroup.$NpmName") && !d.contains(s"$PnpmGroup.$PnpmName"))
    .map { str =>
      val splitStr = str.split("\\.")
      val variant = if (splitStr.contains("variant")) Some(splitStr.last) else None
      val electronLib = settings.afsDependencyResolver.dependencyDefinitions
        .find(d => d.group == splitStr(0) && d.name == splitStr(1) && d.variant.map(_.name) == variant)
        .map(_.copy(transitive = false))
        .getOrThrow(s"can't find related extra lib $str for $id")
      DependencyReport.fromWebToolingDefinition(electronLib, Set(qualifier))
    }
    .toSet

  @node private def getElectronReports(
      settings: MetadataSettings,
      qualifier: QualifierReport,
      configs: Map[ScopeId, ScopeConfiguration]): Set[DependencyReport] = configs.apar.flatMap { case (id, conf) =>
    val electronLibReport: Set[DependencyReport] = conf.electronConfig.map(_.electronLibs) match {
      case Some(libs) => getExtraLibsReports(id, libs, settings, qualifier)
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
    val groupedConfigurations = configurations.groupBy { case (id, cfg) =>
      DependencyType(maven = cfg.flags.mavenOnly, test = isTest(id))
    }

    def deps(maven: Boolean, test: Boolean): Set[DependencyDefinition] =
      groupedConfigurations
        .get(DependencyType(maven, test))
        .map(_.values.flatMap(depExtractor).toSet)
        .getOrElse(Set.empty)

    val nonTestAfsDeps = deps(maven = false, test = false)
    val testOnlyAfsDeps = deps(maven = false, test = true).diff(nonTestAfsDeps)
    val nonTestMavenDeps = deps(maven = true, test = false)
    val testOnlyMavenDeps = deps(maven = true, test = true).diff(nonTestMavenDeps)
    val electronConfigs = configurations.filter { case (id, conf) => conf.electronConfig.isDefined }
    val webConfigs = configurations.filter { case (id, conf) => conf.webConfig.isDefined }

    val afsReports =
      getDepsReports(settings, qualifier, nonTestAfsDeps, testOnlyAfsDeps, settings.afsDependencyResolver)
    val mavenReports =
      getDepsReports(settings, qualifier, nonTestMavenDeps, testOnlyMavenDeps, settings.mavenDependencyResolver)
    val extraLibsReports = getWebDepsReports(settings, qualifier, webConfigs) ++
      getElectronReports(settings, qualifier, electronConfigs)

    ExternalDependenciesReports(DependenciesReport(afsReports ++ mavenReports), DependenciesReport(extraLibsReports))
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
