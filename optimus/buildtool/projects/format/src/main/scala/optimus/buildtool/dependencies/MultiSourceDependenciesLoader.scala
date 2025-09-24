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
import optimus.buildtool.dependencies.JvmDependenciesLoader.loadSubstitutions
import optimus.buildtool.dependencies.MultiSourceDependency.MultipleAfsError
import optimus.buildtool.format.ConfigUtils.ConfOps
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.OrderingUtils
import optimus.buildtool.format.OrderingUtils._
import optimus.buildtool.format.Result
import optimus.buildtool.format.Error
import optimus.buildtool.format.Failure
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.format.Keys
import optimus.buildtool.format.Keys.jvmDependencyDefinition
import optimus.buildtool.format.OrderingUtils.NestedDefinitions
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Success
import optimus.buildtool.utils.FormatUtils.distinctLast

import scala.collection.compat._
import scala.jdk.CollectionConverters._

object MultiSourceDependenciesLoader {
  private[buildtool] val Maven = "maven"
  private[buildtool] val Afs = "afs"
  private[buildtool] val NoAfs = "noAfs"
  private[buildtool] val Scala = "scala"
  private[buildtool] val Extends = "extends"
  private[buildtool] val Runtime = "runtime"
  private[buildtool] val MavenOnlyWithoutNoAfsFlagError =
    "for maven dependency without AFS equivalents, please set 'noAfs = true' flag"

  private[buildtool] def duplicationMsg(key: String, otherFile: DependencyConfig, otherLine: Int) =
    s"Dependency $key is already defined in ${otherFile.path.pathString}:$otherLine"

  private def loadJvmDepsBySourceName(
      depConfig: Config,
      obtFile: ObtFile,
      sourceName: String,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[Seq[DependencyDefinition]] = {
    if (depConfig.hasPath(sourceName)) {
      val sourceConf = depConfig.getObject(sourceName).toConfig
      DependencyLoader
        .loadLocalDefinitions(
          sourceConf,
          sourceName,
          LocalDefinition,
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
      afsVariants: Seq[DependencyDefinition],
      baseMavenLibs: Seq[DependencyDefinition],
      depConfig: Config,
      name: String,
      obtFile: DependencyConfig,
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
        DependencyLoader.loadLocalDefinitions(
          mavenConfig,
          Maven,
          LocalDefinition,
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
            val msg = s"Ivy config '$cfgName' doesn't exist"
            (Nil, Set(obtFile.errorAt(config.getValue(Extends), msg)))
        }
        if (checked.contains(cfgName)) (Seq.empty, errors ++ failure)
        else {
          val result = extended.map(expandDepsFromCfg(_, checked + cfgName, errors ++ failure, configNameToExtends))
          (cfgName +: result.flatMap(_._1), result.flatMap(_._2) ++ failure)
        }
      }

      def extendedMavenLibsResult(
          configNameToMavenLibs: Map[String, Seq[DependencyDefinition]],
          configNameToExtends: Map[String, Seq[String]]): Result[Seq[DependencyDefinition]] = {

        if (config.hasPath(Extends)) {
          val extendsList = distinctLast(name +: config.getStringList(Extends).asScala.to(Seq))
          val (allExtended, extendErrors): (Seq[String], Set[Error]) = {
            val result = extendsList.map(expandDepsFromCfg(_, Set.empty, Seq.empty, configNameToExtends))
            (distinctLast(result.flatMap(_._1)), result.flatMap(_._2).toSet)
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

    def isVariant(dd: DependencyDefinition) = dd.variant.exists(!_.configurationOnly)

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
          res <- Result
            .sequence(extendedConfigs.map { case (ivy, equivalents) =>
              MultiSourceDependency.expandDependencyDefinitions(
                obtFile,
                depConfig.getValue(IvyConfigurations),
                s"$name.${ivy.configuration}",
                Some(ivy),
                afsVariants.map(v => getAfsConfigDep(ivy.configuration, v, ivy.line)),
                equivalents,
                None,
                ivy.line
              )
            })
            .map(_.flatten)
        } yield res
        ivyMultiSourceDependencies
          .withProblems { ds =>
            // don't check ordering for variants, since "foo.variant" lexically comes after "foo-bar.variant"
            val nonVariants = ds.filter(!_.definitions.all.exists(isVariant))
            OrderingUtils.checkOrderingIn(obtFile, nonVariants)
          }
          .map { ivyMultiSourceDependencies =>
            val runtimeOverrideIndex = ivyMultiSourceDependencies.indexWhere(_.afs.exists(_.configuration == Runtime))
            if (runtimeOverrideIndex >= 0) {
              val runtimeDefinitionOverride = ivyMultiSourceDependencies(runtimeOverrideIndex)
              val adjustedRuntimeDefinitionOverride = runtimeDefinitionOverride.definitions match {
                case Mapped(afs, maven) =>
                  runtimeDefinitionOverride.copy(definitions = Mapped(afs.copy(variant = None), maven))
                case _ =>
                  runtimeDefinitionOverride
              }
              val rest = ivyMultiSourceDependencies.patch(runtimeOverrideIndex, Nil, 1)
              Some(adjustedRuntimeDefinitionOverride) -> rest
            } else None -> ivyMultiSourceDependencies
          }
      case _ => Success(None -> Nil)
    }
  }

  def load(
      config: Config,
      dependenciesKey: String,
      obtFile: DependencyConfig,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[MultiSourceDependencies] = {
    Result.tryWith(obtFile, config) {
      val dependenciesConfig = config.getConfig(dependenciesKey)
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
                else loadJvmDepsBySourceName(depConfig, obtFile, Afs, loadedResolvers, scalaMajorVer)
              coreAfsDep <- afsDeps.filter(_.variant.isEmpty) match {
                case Seq(unique) =>
                  Success(Some(unique))
                case _ if afsDeps.size > 1 =>
                  Failure(Seq(obtFile.errorAt(depConfig.getValue(Afs), MultipleAfsError)))
                case _ => Success(None)
              }
              mavenDeps <- loadJvmDepsBySourceName(depConfig, obtFile, Maven, loadedResolvers, scalaMajorVer)
              (maybeOverridenRuntime, ivyMultiSourceDeps) <- loadJvmDepsIvyConfigurations(
                coreAfsDep,
                afsDeps.filter(_.variant.nonEmpty),
                mavenDeps,
                depConfig,
                strName,
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

      for {
        nestedDefinitions <- multiSourceDeps.withProblems(OrderingUtils.checkOrderingIn(obtFile, _))
        substitutions <- loadSubstitutions(config, obtFile)
      } yield MultiSourceDependencies(nestedDefinitions.flatMap(_.loaded), substitutions)
    }
  }
}

final case class IvyConfigurationMapping(
    afs: DependencyDefinition,
    mavenLibs: Seq[DependencyDefinition],
    extended: Seq[String])
