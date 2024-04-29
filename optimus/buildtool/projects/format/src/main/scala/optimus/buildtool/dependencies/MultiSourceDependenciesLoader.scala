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
package optimus.buildtool.dependencies

import com.typesafe.config._
import optimus.buildtool.config._
import optimus.buildtool.dependencies.JvmDependenciesLoader.IvyConfigurations
import optimus.buildtool.dependencies.JvmDependenciesLoader.Variants
import optimus.buildtool.dependencies.JvmDependenciesLoader.loadLocalDefinitions
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.DependenciesConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.OrderingUtils
import optimus.buildtool.format.Result
import optimus.buildtool.format.Error
import optimus.buildtool.format.Failure
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.format.Keys.jvmDependencyDefinition
import optimus.buildtool.format.MavenDependenciesConfig
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Success

import scala.collection.immutable.Seq

object MultiSourceDependenciesLoader {
  private[buildtool] val Maven = "maven"
  private[buildtool] val Afs = "afs"
  private[buildtool] val NoAfs = "noAfs"
  private[buildtool] val jvmPathStr = JvmDependenciesConfig.path.pathString
  private[buildtool] val MavenOnlyWithoutNoAfsFlagError =
    "for maven dependency without AFS equivalents, please set 'noAfs = true' flag"

  private[buildtool] def duplicationMsg(d: DependencyDefinition, withFile: String, withLine: Int, suffix: String = "") =
    s"dependency ${d.key} already be defined in $withFile line $withLine ! $suffix"

  def checkDuplicates(
      obtFile: ObtFile,
      multiSourceDeps: MultiSourceDependencies,
      singleSourceDeps: Seq[DependencyDefinition]): Result[MultiSourceDependencies] = {

    def getErrors(
        deps: Seq[(DependencyDefinition, DependencyDefinition)],
        withFile: String,
        surffix: String = ""): Seq[Error] =
      deps.map { case (from, to) =>
        Error(duplicationMsg(from, withFile, to.line, surffix), obtFile, from.line)
      }

    def infoForChecking(d: DependencyDefinition) =
      DependencyDefinition(d.group, d.name, "", LocalDefinition, variant = d.variant)

    def getPredefinedDep(preDefs: Seq[DependencyDefinition], target: DependencyDefinition): DependencyDefinition =
      preDefs.find(predef => predef.name == target.name && predef.group == target.group).get

    val (mavenDeps, afsDeps) = singleSourceDeps.partition(_.isMaven)
    val (afsDepsToCheck, mavenDepsToCheck) = (afsDeps.map(infoForChecking), mavenDeps.map(infoForChecking))

    val duplicationAfsErrors =
      multiSourceDeps.afsDefinedDeps.groupBy(d => infoForChecking(d.definition)).filter(_._2.size > 1).flatMap {
        case (_, deps) =>
          val sorted = deps.sortBy(_.line)
          val from = sorted.head
          val to = sorted.last
          getErrors(Seq((from.definition, to.definition)), jvmPathStr)
      }

    val afsRedefinedErrors = getErrors(
      multiSourceDeps.afsDefinedDeps.collect {
        case d if afsDepsToCheck.contains(infoForChecking(d.definition)) =>
          d.definition -> getPredefinedDep(afsDeps, d.definition)
      },
      DependenciesConfig.path.pathString
    )

    val duplicationMavenErrors =
      multiSourceDeps.mavenDefinedDeps
        .flatMap { d => d.maven.map(m => m -> infoForChecking(m)) }
        .groupBy(_._2)
        .filter(_._2.size > 1)
        .flatMap { case (_, deps) =>
          val sorted = deps.map(_._1).sortBy(_.line)
          val from = sorted.head
          val to = sorted.last
          getErrors(Seq((from, to)), jvmPathStr)
        }
    val mavenRedefinedErrors = getErrors(
      multiSourceDeps.mavenDefinedDeps.flatMap(_.maven).collect {
        case d if mavenDepsToCheck.contains(infoForChecking(d)) => d -> getPredefinedDep(mavenDeps, d)
      },
      MavenDependenciesConfig.path.pathString
    )

    val errors =
      duplicationAfsErrors ++ duplicationMavenErrors ++ afsRedefinedErrors ++ mavenRedefinedErrors

    if (errors.nonEmpty) Failure(errors.toIndexedSeq.distinct)
    else Success(multiSourceDeps)
  }

  def loadJvmDepsBySourceName(
      depConfig: Config,
      kind: Kind,
      obtFile: ObtFile,
      sourceName: String,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[Seq[DependencyDefinition]] = {
    if (depConfig.hasPath(sourceName)) {
      val sourceConf = depConfig.getObject(sourceName).toConfig
      val variantsConf: Option[Config] =
        if (depConfig.hasPath(Variants)) Some(depConfig.getObject(Variants).toConfig) else None
      JvmDependenciesLoader
        .loadLocalDefinitions(
          sourceConf,
          kind,
          obtFile,
          isMaven = sourceName == Maven,
          loadedVariantsConfig = variantsConf,
          loadedResolvers = loadedResolvers,
          scalaMajorVersion = scalaMajorVersion,
          isMultiSourceScalaLib = scalaMajorVersion.isDefined
        )
        .withProblems(deps => OrderingUtils.checkOrderingIn(obtFile, deps))
    } else Result.sequence(Nil)
  }

  def loadJvmDepsIvyConfigurations(
      baseAfsDepOpt: Option[DependencyDefinition],
      depConfig: Config,
      kind: Kind,
      obtFile: ObtFile,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[Map[DependencyDefinition, Seq[DependencyDefinition]]] = {
    baseAfsDepOpt match {
      case Some(baseAfsDep) if depConfig.hasPath(IvyConfigurations) =>
        val ivyConfig = depConfig.getObject(IvyConfigurations).toConfig
        val scalaMajorVer = if (depConfig.optionalBoolean("scala").contains(true)) scalaMajorVersion else None
        Result.traverse(ivyConfig.nested(obtFile)) { case (name, config) =>
          val mavenEquivalentConfig = config.getObject(Maven).toConfig
          val afsVariant = Variant(name, "Additional config to use", configurationOnly = true)
          val afsConfigLib = baseAfsDep.copy(
            configuration = name,
            variant = Some(afsVariant),
            line = mavenEquivalentConfig.origin().lineNumber())
          val mavenConfigEquivalents =
            loadLocalDefinitions(
              mavenEquivalentConfig,
              kind,
              obtFile,
              isMaven = true,
              loadedResolvers,
              scalaMajorVersion = scalaMajorVer,
              isMultiSourceScalaLib = scalaMajorVer.isDefined)
          for {
            mavenLibs <- mavenConfigEquivalents
          } yield afsConfigLib -> mavenLibs
        }

      case _ => Result.sequence(Nil)
    }
  }.map(_.toMap)

  def load(
      dependenciesConfig: Config,
      kind: Kind,
      obtFile: ObtFile,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[MultiSourceDependencies] =
    Result.tryWith(obtFile, dependenciesConfig) {
      val jvmKeyConfigs = dependenciesConfig.nested(obtFile).getOrElse(Nil)
      val multiSourceDeps: Result[Seq[MultiSourceDependencies]] = Result
        .sequence(jvmKeyConfigs.map { case (strName, depConfig) =>
          val scalaMajorVer = if (depConfig.optionalBoolean("scala").contains(true)) scalaMajorVersion else None
          val noAfs = depConfig.optionalBoolean(NoAfs).contains(true)
          if (!noAfs && !depConfig.hasPath(Afs) && depConfig.hasPath(Maven))
            obtFile.failure(depConfig.getValue(Maven), MavenOnlyWithoutNoAfsFlagError)
          else
            for {
              afsDeps <-
                if (noAfs) Success(Nil)
                else loadJvmDepsBySourceName(depConfig, kind, obtFile, Afs, loadedResolvers, scalaMajorVer)
              mavenDeps <- loadJvmDepsBySourceName(depConfig, kind, obtFile, Maven, loadedResolvers, scalaMajorVer)
              ivyConfigsMap <- loadJvmDepsIvyConfigurations(
                afsDeps.find(d => !d.isMaven && d.variant.isEmpty),
                depConfig,
                kind,
                obtFile,
                loadedResolvers,
                scalaMajorVersion)
              multiSourceDeps <- MultiSourceDependency(
                obtFile,
                dependenciesConfig.getValue(strName),
                strName,
                afsDeps,
                mavenDeps,
                ivyConfigsMap,
                depConfig.origin().lineNumber())
                .withProblems(depConfig.checkExtraProperties(JvmDependenciesConfig, jvmDependencyDefinition))
            } yield MultiSourceDependencies(multiSourceDeps)
        }.toIndexedSeq)

      multiSourceDeps
        .map(loaded => loaded.foldLeft(MultiSourceDependencies(Nil))(_ ++ _))
        .withProblems(deps => OrderingUtils.checkOrderingIn(obtFile, deps.loaded))
    }
}
