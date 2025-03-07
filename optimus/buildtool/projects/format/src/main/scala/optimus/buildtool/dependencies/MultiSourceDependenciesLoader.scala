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
import optimus.buildtool.dependencies.JvmDependenciesLoader.loadLocalDefinitions
import optimus.buildtool.dependencies.MultiSourceDependency.MultipleAfsError
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.DependenciesConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.OrderingUtils
import optimus.buildtool.format.OrderingUtils._
import optimus.buildtool.format.Result
import optimus.buildtool.format.Error
import optimus.buildtool.format.Failure
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.format.Keys
import optimus.buildtool.format.Keys.jvmDependencyDefinition
import optimus.buildtool.format.MavenDependenciesConfig
import optimus.buildtool.format.OrderingUtils.NestedDefinitions
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Success

import scala.collection.immutable.Seq
import scala.collection.compat._
import scala.jdk.CollectionConverters._

object MultiSourceDependenciesLoader {
  private[buildtool] val Maven = "maven"
  private[buildtool] val Afs = "afs"
  private[buildtool] val NoAfs = "noAfs"
  private[buildtool] val Scala = "scala"
  private[buildtool] val Extends = "extends"
  private[buildtool] val Runtime = "runtime"
  private[buildtool] val jvmPathStr = JvmDependenciesConfig.path.pathString
  private[buildtool] val MavenOnlyWithoutNoAfsFlagError =
    "for maven dependency without AFS equivalents, please set 'noAfs = true' flag"

  private[buildtool] def duplicationMsg(d: DependencyDefinition, withFile: String, withLine: Int, suffix: String = "") =
    s"dependency ${d.key} already be defined in $withFile line $withLine ! $suffix"

  private def infoForChecking(d: DependencyDefinition) =
    DependencyDefinition(d.group, d.name, "", LocalDefinition, variant = d.variant)

  private def getErrors(
      obtFile: ObtFile,
      deps: Seq[(DependencyDefinition, DependencyDefinition)],
      withFile: String,
      surffix: String = ""): Seq[Error] =
    deps.map { case (from, to) =>
      Error(duplicationMsg(from, withFile, to.line, surffix), obtFile, from.line)
    }

  private def checkDuplicatedJvmNames(obtFile: ObtFile, loaded: Seq[MultiSourceDependency]): Seq[Error] = {
    val depNameToDef = loaded.map { d => s"${d.definition.group}.${d.definition.name}" -> d.definition }.toMap
    loaded.flatMap { l =>
      depNameToDef.get(l.name) match {
        case Some(dup) => getErrors(obtFile, Seq((l.definition, dup)), jvmPathStr)
        case None      => Seq.empty
      }
    }
  }

  private def checkDuplicatedAfslibs(obtFile: ObtFile, afsDefinedDeps: Seq[MultiSourceDependency]): Seq[Error] =
    afsDefinedDeps
      .groupBy(d => infoForChecking(d.definition))
      .filter(_._2.size > 1) // by info for checking
      .flatMap { case (_, deps) =>
        val sorted = deps.sortBy(_.line)
        val from = sorted.head
        val to = sorted.last
        getErrors(obtFile, Seq((from.definition, to.definition)), jvmPathStr)
      }
      .to(Seq)

  private def checkDuplicatedMavenlibs(obtFile: ObtFile, mavenDefinedDeps: Seq[MultiSourceDependency]): Seq[Error] =
    mavenDefinedDeps
      .flatMap { d => d.maven.map(m => m -> infoForChecking(m)) }
      .groupBy(_._2) // by info for checking
      .filter(_._2.size > 1)
      .flatMap { case (_, deps) =>
        val sorted = deps.map(_._1).sortBy(_.line)
        val from = sorted.head
        val to = sorted.last
        if (from == to) Nil // allow ivy config extends
        else getErrors(obtFile, Seq((from, to)), jvmPathStr)
      }
      .to(Seq)

  def checkDuplicates(
      obtFile: ObtFile,
      multiSourceDeps: MultiSourceDependencies,
      singleSourceDeps: Seq[DependencyDefinition]): Result[MultiSourceDependencies] = {

    def getPredefinedDep(preDefs: Seq[DependencyDefinition], target: DependencyDefinition): DependencyDefinition =
      preDefs.find(predef => predef.name == target.name && predef.group == target.group).get

    val (mavenDeps, afsDeps) = singleSourceDeps.partition(_.isMaven)
    val (afsDepsToCheck, mavenDepsToCheck) = (afsDeps.map(infoForChecking), mavenDeps.map(infoForChecking))

    val duplicatedNames = checkDuplicatedJvmNames(obtFile, multiSourceDeps.loaded)

    val duplicatedAfs = checkDuplicatedAfslibs(obtFile, multiSourceDeps.afsDefinedDeps)

    val afsRedefinedErrors = getErrors(
      obtFile,
      multiSourceDeps.afsDefinedDeps.collect {
        case d if afsDepsToCheck.contains(infoForChecking(d.definition)) =>
          d.definition -> getPredefinedDep(afsDeps, d.definition)
      },
      DependenciesConfig.path.pathString
    )

    val duplicatedMavenErrors = checkDuplicatedMavenlibs(obtFile, multiSourceDeps.mavenDefinedDeps)
    val mavenRedefinedErrors = getErrors(
      obtFile,
      multiSourceDeps.mavenDefinedDeps.flatMap(_.maven).collect {
        case d if mavenDepsToCheck.contains(infoForChecking(d)) => d -> getPredefinedDep(mavenDeps, d)
      },
      MavenDependenciesConfig.path.pathString
    )

    val errors = duplicatedNames ++
      duplicatedAfs ++ duplicatedMavenErrors ++ afsRedefinedErrors ++ mavenRedefinedErrors

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
      JvmDependenciesLoader
        .loadLocalDefinitions(
          sourceConf,
          sourceName,
          kind,
          obtFile,
          isMaven = sourceName == Maven,
          loadedResolvers = loadedResolvers,
          scalaMajorVersion = scalaMajorVersion,
          isMultiSourceScalaLib = scalaMajorVersion.isDefined
        )
    } else Result.sequence(Nil)
  }

  private def loadJvmDepsIvyConfigurations(
      baseAfsDepOpt: Option[DependencyDefinition],
      baseMavenLibs: Seq[DependencyDefinition],
      depConfig: Config,
      name: String,
      kind: Kind,
      obtFile: ObtFile,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[(Option[MultiSourceDependency], Seq[MultiSourceDependency])] = {

    def getAfsConfigDep(name: String, baseAfsDep: DependencyDefinition, line: Int): DependencyDefinition =
      baseAfsDep.copy(
        configuration = name,
        variant = Some(Variant(name, "Additional config to use", configurationOnly = true)),
        line = line)

    def loadMavenConfig(
        config: Config,
        name: String,
        baseAfsDep: DependencyDefinition,
        scalaMajorVer: Option[String]): Result[IvyConfigurationMapping] = {
      val afsConfigLib = getAfsConfigDep(name, baseAfsDep, config.origin().lineNumber())
      val mavenConfigEquivalents = if (config.hasPath(Maven)) {
        val mavenConfig = config.getObject(Maven).toConfig
        loadLocalDefinitions(
          mavenConfig,
          Maven,
          kind,
          obtFile,
          isMaven = true,
          loadedResolvers,
          scalaMajorVersion = scalaMajorVer,
          isMultiSourceScalaLib = scalaMajorVer.isDefined)
      } else Result.sequence(Nil)
      val withExtends =
        if (config.hasPath(Extends)) config.getStringList(Extends).asScala.to(Seq)
        else Nil
      for {
        mavenLibs <- mavenConfigEquivalents
      } yield IvyConfigurationMapping(afsConfigLib, mavenLibs, withExtends)
    }

    def loadExtendsConfig(
        config: Config,
        name: String,
        baseAfsDep: DependencyDefinition,
        loadedIvyConfigs: Seq[IvyConfigurationMapping]): Result[(DependencyDefinition, Seq[DependencyDefinition])] = {

      def expandDepsFromCfg(
          cfgName: String,
          checked: Set[String],
          errors: Seq[Error],
          configNameToExtends: Map[String, Seq[String]]): (Seq[String], Seq[Error]) = {
        val (extended, failure) = configNameToExtends.get(cfgName) match {
          case Some(res) => (res, Seq.empty)
          case None =>
            val msg =
              s"ivy config '$cfgName' not exists! at ${JvmDependenciesConfig.path.name}:${config.origin().lineNumber()} $IvyConfigurations '$name'"
            (Nil, Set(obtFile.errorAt(config.getValue(Extends), msg)))
        }
        if (checked.contains(cfgName)) (Seq.empty, errors ++ failure)
        else {
          val result = extended.map(expandDepsFromCfg(_, checked + cfgName, errors ++ failure, configNameToExtends))
          (result.flatMap(_._1) :+ cfgName, result.flatMap(_._2) ++ failure)
        }
      }

      def extendedMavenLibsResult(
          configNameToMavenLibs: Map[String, Seq[DependencyDefinition]],
          configNameToExtends: Map[String, Seq[String]]): Result[Seq[DependencyDefinition]] = {

        if (config.hasPath(Extends)) {
          val extendsList = (config.getStringList(Extends).asScala.to(Seq) :+ name).distinct
          val (allExtended, extendErrors): (Set[String], Set[Error]) = {
            val result = extendsList.map(expandDepsFromCfg(_, Set.empty, Seq.empty, configNameToExtends))
            (result.flatMap(_._1).toSet, result.flatMap(_._2).toSet)
          }
          if (extendErrors.isEmpty) Success(allExtended.flatMap(configNameToMavenLibs).to(Seq))
          else Failure(extendErrors.to(Seq))
        } else Success(configNameToMavenLibs(name))
      }

      val allMapping = loadedIvyConfigs :+ IvyConfigurationMapping(baseAfsDep, baseMavenLibs, Nil)
      val afsConfigLib = getAfsConfigDep(name, baseAfsDep, config.origin().lineNumber())
      val configNameToMavenLibs = allMapping.map { case IvyConfigurationMapping(afs, maven, _) =>
        afs.configuration -> maven
      }.toMap
      val configNameToExtended: Map[String, Seq[String]] = allMapping.map {
        case IvyConfigurationMapping(afs, _, extended) =>
          afs.configuration -> extended.filterNot(_ == afs.configuration) // ignore self extends
      }.toMap
      val extendedMavenLibs = extendedMavenLibsResult(configNameToMavenLibs, configNameToExtended)
      extendedMavenLibs.map(afsConfigLib -> _)
    }

    def validKeysForName(name: String): Keys.KeySet =
      if (name == Runtime) Keys.KeySet(Extends)
      else Keys.ivyConfiguration

    baseAfsDepOpt match {
      case Some(baseAfsDep) if depConfig.hasPath(IvyConfigurations) =>
        val ivyConfig = depConfig.getObject(IvyConfigurations).toConfig
        val scalaMajorVer = if (depConfig.optionalBoolean("scala").contains(true)) scalaMajorVersion else None
        val ivyMultiSourceDependencies = for {
          loadedIvyConfigs <- Result.traverse(ivyConfig.nested(obtFile)) { case (name, config) =>
            loadMavenConfig(config, name, baseAfsDep, scalaMajorVer)
              .withProblems(config.checkExtraProperties(obtFile, validKeysForName(name)))
          }
          extendedConfigs <- Result.traverse(ivyConfig.nested(obtFile)) { case (name, config) =>
            loadExtendsConfig(config, name, baseAfsDep, loadedIvyConfigs)
          }
          res = extendedConfigs.map { case (ivy, equivalents) =>
            MultiSourceDependency(s"$name.${ivy.configuration}", Some(ivy), equivalents, ivy.line, ivyConfig = true)
          }
        } yield res
        ivyMultiSourceDependencies
          .withProblems(OrderingUtils.checkOrderingIn(obtFile, _))
          .map { ivyMultiSourceDependencies =>
            val runtimeOverrideIndex = ivyMultiSourceDependencies.indexWhere(_.afs.exists(_.configuration == Runtime))
            if (runtimeOverrideIndex > 0) {
              val runtimeDefinitionOverride = ivyMultiSourceDependencies(runtimeOverrideIndex)
              val newAfs = runtimeDefinitionOverride.afs.map(_.copy(variant = None))
              val adjustedRuntimeDefinitionOverride = runtimeDefinitionOverride.copy(afs = newAfs)
              val rest = ivyMultiSourceDependencies
                .take(runtimeOverrideIndex) ++ ivyMultiSourceDependencies.drop(runtimeOverrideIndex).tail
              Some(adjustedRuntimeDefinitionOverride) -> rest
            } else None -> ivyMultiSourceDependencies
          }
      case _ => Success(None -> Nil)
    }
  }

  def load(
      dependenciesConfig: Config,
      kind: Kind,
      obtFile: ObtFile,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[MultiSourceDependencies] = {
    Result.tryWith(obtFile, dependenciesConfig) {
      val jvmKeyConfigs = dependenciesConfig.nested(obtFile).getOrElse(Nil)
      val multiSourceDeps = Result
        .sequence(jvmKeyConfigs.map { case (strName, depConfig) =>
          val scalaMajorVer = if (depConfig.optionalBoolean(Scala).contains(true)) scalaMajorVersion else None
          val noAfs = depConfig.optionalBoolean(NoAfs).contains(true)
          if (!noAfs && !depConfig.hasPath(Afs) && depConfig.hasPath(Maven))
            obtFile.failure(depConfig.getValue(Maven), MavenOnlyWithoutNoAfsFlagError)
          else
            for {
              afsDeps <-
                if (noAfs) Success(Nil)
                else loadJvmDepsBySourceName(depConfig, kind, obtFile, Afs, loadedResolvers, scalaMajorVer)
              coreAfsDep <- afsDeps.filter(_.variant.isEmpty) match {
                case Seq(unique) =>
                  Success(Some(unique))
                case _ if afsDeps.size > 1 =>
                  Failure(Seq(obtFile.errorAt(depConfig.getValue(Afs), MultipleAfsError)))
                case _ => Success(None)
              }
              mavenDeps <- loadJvmDepsBySourceName(depConfig, kind, obtFile, Maven, loadedResolvers, scalaMajorVer)
              (maybeOverridenRuntime, ivyMultiSourceDeps) <- loadJvmDepsIvyConfigurations(
                coreAfsDep,
                mavenDeps,
                depConfig,
                strName,
                kind,
                obtFile,
                loadedResolvers,
                scalaMajorVersion)
              multiSourceDeps <- MultiSourceDependency
                .expandDependencyDefinitions(
                  obtFile,
                  dependenciesConfig.getValue(strName),
                  strName,
                  coreAfsDep,
                  afsDeps.filter(_.variant.nonEmpty),
                  mavenDeps,
                  maybeOverridenRuntime,
                  depConfig.origin().lineNumber()
                )
                .withProblems(depConfig.checkExtraProperties(JvmDependenciesConfig, jvmDependencyDefinition))
              allDeps = maybeOverridenRuntime ++ ivyMultiSourceDeps ++ multiSourceDeps
            } yield NestedDefinitions(allDeps, "obt.jvm.loaded", strName, depConfig.origin().lineNumber())
        }.toIndexedSeq)

      multiSourceDeps
        .withProblems(OrderingUtils.checkOrderingIn(obtFile, _))
        .map(nestedDefinitions => MultiSourceDependencies(nestedDefinitions.flatMap(_.loaded)))
    }
  }
}

final case class IvyConfigurationMapping(
    afs: DependencyDefinition,
    mavenLibs: Seq[DependencyDefinition],
    extended: Seq[String])
