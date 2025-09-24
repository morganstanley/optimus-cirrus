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
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.ResolutionResult
import optimus.buildtool.scope.ScopedCompilation
import optimus.platform._

import scala.collection.compat._

final case class DependenciesReport(dependencies: Set[DependencyReport])

final case class ExternalDependenciesReports(jvm: DependenciesReport, extraLibs: DependenciesReport)

object DependenciesReport {

  @node def apply(
      settings: MetadataSettings,
      moduleCompilations: Map[ScopeId, ScopedCompilation]
  ): ExternalDependenciesReports = {
    val internalDeps = {
      val internalCompileDeps =
        toInternalDependenciesReport(moduleCompilations, settings)(Compile, _.internalCompileDependencies)
      val internalRuntimeDeps =
        toInternalDependenciesReport(moduleCompilations, settings)(Runtime, _.internalRuntimeDependencies)
      DependencyReport.merge(internalCompileDeps, internalRuntimeDeps)
    }

    val (externalDeps, externalWebDeps) = {
      val externalCompileDeps =
        toExternalDependenciesReport(moduleCompilations)(Compile, _.externalCompileDependencies, settings)
      val externalRuntimeDeps =
        toExternalDependenciesReport(moduleCompilations)(Runtime, _.externalRuntimeDependencies, settings)

      (
        DependencyReport.merge(externalCompileDeps.jvm.dependencies, externalRuntimeDeps.jvm.dependencies),
        DependencyReport.merge(externalCompileDeps.extraLibs.dependencies, externalRuntimeDeps.extraLibs.dependencies))
    }

    ExternalDependenciesReports(DependenciesReport(internalDeps ++ externalDeps), DependenciesReport(externalWebDeps))
  }

  private def isTest(scopeId: ScopeId): Boolean = scopeId.tpe.toLowerCase().contains("test")

  private def toInternalDependenciesReport(
      moduleCompilations: Map[ScopeId, ScopedCompilation],
      settings: MetadataSettings
  )(
      qualifier: QualifierReport,
      depExtractor: ScopeConfiguration => Seq[ScopeId]
  ): Set[DependencyReport] = {
    val configurations = moduleCompilations.map { case (k, v) => k -> v.config }
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
      compileResolutions: Seq[ResolutionResult],
      testResolutions: Seq[ResolutionResult],
      extraCompileDeps: Set[DependencyDefinition],
      extraTestDeps: Set[DependencyDefinition]): Set[DependencyReport] = {
    // we use resolved direct + transitive all artifacts to generate metadata
    val resolvedCompileArtifacts = compileResolutions.flatMap(_.resolvedArtifacts.map(_.id)).distinct
    val resolvedTestArtifacts =
      testResolutions.flatMap(_.resolvedArtifacts.map(_.id)).diff(resolvedCompileArtifacts).distinct

    def extraLibsReports(libs: Set[DependencyDefinition], qualifiers: Set[QualifierReport]): Set[DependencyReport] =
      libs.map(DependencyReport.fromExtraLib(_, qualifiers))

    val nonTestReports =
      DependencyReport.fromExternalIds(resolvedCompileArtifacts, Set(qualifier), settings) ++
        extraLibsReports(extraCompileDeps, Set(qualifier))
    val testQualifier = Set(qualifier, TestOnly)
    val testReports =
      DependencyReport.fromExternalIds(resolvedTestArtifacts, testQualifier, settings) ++
        extraLibsReports(extraTestDeps, Set(qualifier))

    (nonTestReports ++ testReports).toSet
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
      val afsElectronLibs = settings.extraLibs
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

  @node private def getResolutions(
      compilations: Map[ScopeId, ScopedCompilation],
      qualifier: QualifierReport): Seq[ResolutionResult] =
    compilations.apar
      .flatMap { case (k, v) => v.allResolutions.getOrElse(qualifier, Nil) }
      .to(Seq)
      .filterNot(_.resolvedArtifacts.isEmpty)

  @node private def toExternalDependenciesReport(moduleCompilations: Map[ScopeId, ScopedCompilation])(
      qualifier: QualifierReport,
      depExtractor: ScopeConfiguration => Seq[DependencyDefinition],
      settings: MetadataSettings
  ): ExternalDependenciesReports = {
    val configurations = moduleCompilations.map { case (k, v) => k -> v.config }
    val (testCompilations, compileCompilations) = moduleCompilations.partition { case (id, v) => isTest(id) }
    val testResolutions = getResolutions(testCompilations, qualifier)
    val compileResolutions = getResolutions(compileCompilations, qualifier)

    def getExtraDeps(cfgs: Map[ScopeId, ScopeConfiguration]): Set[DependencyDefinition] = cfgs
      .flatMap { case (id, conf) =>
        depExtractor(conf)
      }
      .toSet
      .filter(_.isExtraLib)
    val (extraTestDeps, extraCompileDeps) = {
      val (testConfs, confs) = configurations.partition { case (id, conf) => isTest(id) }
      val (loadTestDeps, loadDeps) = (getExtraDeps(testConfs), getExtraDeps(confs))
      (loadTestDeps.diff(loadDeps), loadDeps)
    }

    val electronConfigs = configurations.filter { case (id, conf) => conf.electronConfig.isDefined }
    val webConfigs = configurations.filter { case (id, conf) => conf.webConfig.isDefined }

    val dependenciesReports =
      getDepsReports(settings, qualifier, compileResolutions, testResolutions, extraCompileDeps, extraTestDeps)
    val otherExtraLibsReports = getWebDepsReports(settings, qualifier, webConfigs) ++
      getElectronReports(settings, qualifier, electronConfigs)

    ExternalDependenciesReports(DependenciesReport(dependenciesReports), DependenciesReport(otherExtraLibsReports))
  }

}
