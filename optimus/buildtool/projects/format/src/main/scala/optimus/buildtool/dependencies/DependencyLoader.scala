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
import optimus.buildtool.config.DependencyDefinition.DefaultConfiguration
import optimus.buildtool.config.NamingConventions.IsAgentKey
import optimus.buildtool.config._
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.Scala
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.Error
import optimus.buildtool.format.Keys
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.OrderingUtils
import optimus.buildtool.format.OrderingUtils._
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result
import optimus.buildtool.format.ResultSeq
import optimus.buildtool.format.SetConfig
import optimus.buildtool.format.Success

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

object DependencyLoader {
  import JvmDependenciesLoader._

  def mavenScalaLibName(name: String, scalaMajorVer: String) = s"${name}_$scalaMajorVer"

  private def afsScalaVersion(version: String, scalaMajorVer: String) = s"$scalaMajorVer-$version"

  def loadLocalDefinitions(
      dependenciesConfig: Config,
      configKey: String,
      kind: Kind,
      obtFile: ObtFile,
      isMaven: Boolean,
      loadedResolvers: ResolverDefinitions,
      isExtraLib: Boolean = false,
      scalaMajorVersion: Option[String] = None,
      isMultiSourceScalaLib: Boolean = false
  ): Result[Seq[DependencyDefinition]] = {

    Result.tryWith(obtFile, dependenciesConfig) {
      val groupsToDependencies = ResultSeq(dependenciesConfig.nested(obtFile)).map {
        case (groupName: String, groupConfig: Config) =>
          val dependencyDefinitions = for {
            (depName, dependencyConfig) <- ResultSeq(groupConfig.nested(obtFile))
            isScalaLib = dependencyConfig.optionalBoolean(Scala).getOrElse(isMultiSourceScalaLib)
            loadResult = loadLibrary(
              scalaMajorVersion
                .filter(_ => isScalaLib && isMaven)
                .map(mavenScalaLibName(depName, _))
                .getOrElse(depName),
              groupName,
              dependencyConfig,
              kind,
              obtFile,
              isMaven,
              loadedResolvers,
              isExtraLib,
              if (isScalaLib) scalaMajorVersion else None
            )
          } yield NestedDefinitions(
            ResultSeq(loadResult),
            s"$configKey.$groupName",
            depName,
            dependencyConfig.origin().lineNumber())

          val orderedDependencyDefinitions = dependencyDefinitions
            .withProblems(OrderingUtils.checkOrderingIn(obtFile, _))
            .flatMap(_.loaded)

          NestedDefinitions(orderedDependencyDefinitions, configKey, groupName, groupConfig.origin().lineNumber())
      }

      groupsToDependencies
        .withProblems(OrderingUtils.checkOrderingIn(obtFile, _))
        .flatMap(_.loaded)
        .value

    }
  }

  private def loadLibrary(
      name: String,
      group: String,
      config: Config,
      kind: Kind,
      obtFile: ObtFile,
      isMaven: Boolean,
      loadedResolvers: ResolverDefinitions,
      isExtraLib: Boolean = false,
      scalaMajorVersion: Option[String] = None): Result[Seq[DependencyDefinition]] =
    Result.tryWith(obtFile, config) {

      def loadArtifacts(config: Config) =
        if (!config.hasPath(Artifacts)) Success(Nil)
        else {
          Result.traverse(config.getObject(Artifacts).toConfig.nested(obtFile)) { case (name, artifactConfig) =>
            Result
              .tryWith(obtFile, artifactConfig) {
                val tpe = artifactConfig.getString(TypeStr)
                val ext = artifactConfig.stringOrDefault(Ext, tpe)
                Success(IvyArtifact(name, tpe, ext))
              }
              .withProblems(artifactConfig.checkExtraProperties(obtFile, Keys.artifactConfig))
          }
        }

      def loadVersion(obtFile: ObtFile, config: Config): Option[String] = {
        val depVersion = // allow no version config for multiSource dependencies
          if (obtFile == JvmDependenciesConfig || obtFile.isInstanceOf[SetConfig])
            config.optionalString(Version)
          else Some(config.getString(Version))
        if (isMaven) depVersion
        else
          depVersion.map { v =>
            scalaMajorVersion match { // special case for AFS lib when scala = true)
              case Some(scalaVer) => afsScalaVersion(v, scalaVer)
              case None           => v
            }
          }
      }

      def loadResolvers(
          obtFile: ObtFile,
          config: Config,
          predefinedResolvers: Option[Seq[String]]): Result[Seq[String]] = Result.tryWith(obtFile, config) {
        val (validNames, invalidNames) = predefinedResolvers match {
          case Some(userDefined) =>
            val (validNames, invalidNames) =
              userDefined.partition(str => loadedResolvers.allResolvers.exists(r => r.name == str))
            (validNames, invalidNames)
          case None =>
            val preDefinedIvyRepos = loadedResolvers.defaultIvyResolvers
            val preDefinedMavenRepos = loadedResolvers.defaultMavenResolvers
            if (isMaven && preDefinedMavenRepos.nonEmpty) (preDefinedMavenRepos.map(_.name), Nil)
            else (preDefinedIvyRepos.map(_.name), Nil)
        }
        Success(validNames).withProblems(invalidNames.map(invalid =>
          Error(s"resolver name $invalid is invalid!", obtFile, config.origin().lineNumber())))
      }

      def loadBaseLibrary(fullConfig: Config) = {
        for {
          artifactExcludes <- loadExcludes(fullConfig, obtFile)
          artifacts <- loadArtifacts(fullConfig)
          version = loadVersion(obtFile, fullConfig).getOrElse("")
          predefinedResolvers = config.optionalStringList(Resolvers)
          resolvers <- loadResolvers(obtFile, config, predefinedResolvers)
        } yield DependencyDefinition(
          group = group,
          name = if (isMaven) fullConfig.stringOrDefault("name", default = name) else name,
          version = version,
          kind = kind,
          configuration =
            fullConfig.stringOrDefault(Configuration, default = if (isMaven) "" else DefaultConfiguration),
          classifier = config.optionalString(Classifier),
          excludes = artifactExcludes,
          variant = None,
          resolvers = resolvers,
          transitive = fullConfig.booleanOrDefault(Transitive, default = true),
          force = fullConfig.booleanOrDefault(Force, default = false),
          line = config.origin().lineNumber(),
          keySuffix = fullConfig.stringOrDefault(KeySuffix, default = ""),
          containsMacros = fullConfig.booleanOrDefault(ContainsMacros, default = false),
          isScalacPlugin = fullConfig.booleanOrDefault(IsScalacPlugin, default = false),
          ivyArtifacts = artifacts,
          isMaven = isMaven,
          isAgent = fullConfig.booleanOrDefault(IsAgentKey, default = false),
          isExtraLib = isExtraLib
        )
      }

      val defaultLib = loadBaseLibrary(config)
        .withProblems(config.checkExtraProperties(obtFile, Keys.dependencyDefinition))

      def loadVariant(variant: Config, name: String): Result[DependencyDefinition] = {
        val reason = if (variant.hasPath(Reason)) variant.getString(Reason) else ""
        val variantReason = Variant(name, reason)

        loadBaseLibrary(variant.withFallback(config))
          .map(_.copy(variant = Some(variantReason), line = variant.origin().lineNumber()))
          .withProblems(variant.checkExtraProperties(obtFile, Keys.variantDefinition))
      }

      val configurationVariants =
        if (!config.hasPath(Configurations)) Success(Nil)
        else {
          Result
            .traverse(config.getStringList(Configurations).asScala.to(Seq)) { config =>
              val variant = Variant(config, "Additional config to use", configurationOnly = true)
              defaultLib.map(_.copy(configuration = config, variant = Some(variant)))
            }
            .withProblems(OrderingUtils.checkOrderingIn(obtFile, _)(OrderingUtils.pathOrdering))
        }

      val variantsConfigs =
        if (!config.hasPath(Variants)) Success(Nil)
        else
          Result
            .traverse(config.getObject(Variants).toConfig.nested(obtFile)) { case (name, config) =>
              loadVariant(config, name)
            }
            .withProblems(OrderingUtils.checkOrderingIn(obtFile, _))

      for {
        dl <- defaultLib
        cv <- configurationVariants
        vc <- variantsConfigs
      } yield Seq(dl) ++ cv ++ vc
    }

  def loadExcludes(config: Config, file: ObtFile, allowIvyConfiguration: Boolean = false): Result[Seq[Exclude]] = {
    file.tryWith {
      if (config.hasPath(Excludes)) {
        val excludeList = config.getList(Excludes).asScala.toList
        val excludeResults: Seq[Result[Exclude]] = excludeList.map {
          case obj: ConfigObject =>
            val excludeConf: Config = obj.toConfig

            val ivyConfiguration = excludeConf.optionalString(IvyConfiguration)
            val (allowedKeys, ivyWarning, ivyConfigurationToUse) =
              if (allowIvyConfiguration) (Keys.excludesWithIvyConfig, Nil, ivyConfiguration)
              else if (ivyConfiguration.isDefined) {
                val warning =
                  file.warningAt(
                    obj,
                    s"'$IvyConfiguration' is only supported for global $Excludes, not local $Excludes")
                (Keys.excludesConfig, Seq(warning), None)
              } else (Keys.excludesConfig, Nil, None)

            val exclude =
              Exclude(
                group = excludeConf.optionalString(Group),
                name = excludeConf.optionalString(Name),
                ivyConfiguration = ivyConfigurationToUse)

            Success(exclude)
              .withProblems(
                excludeConf.checkExtraProperties(file, allowedKeys) ++ ivyWarning ++
                  // we always require at least one of group or name to be present
                  excludeConf.checkEmptyProperties(file, Keys.excludesConfig))

          case other => file.failure(other, s""""$Excludes" element is not an object""")
        }

        Result.sequence(excludeResults)
      } else Success(Nil)
    }
  }

  def loadExtraLibs(
      depsConfig: Config,
      obtFile: ObtFile,
      isMavenConfig: Boolean,
      loadedResolvers: ResolverDefinitions,
      scalaMajorVersion: Option[String]): Result[Seq[DependencyDefinition]] =
    if (depsConfig.hasPath(ExtraLibs)) {
      val extraLibOccurrences = depsConfig.getObject(ExtraLibs).toConfig
      loadLocalDefinitions(
        extraLibOccurrences,
        ExtraLibs,
        ExtraLibDefinition,
        obtFile,
        isMavenConfig,
        loadedResolvers = loadedResolvers,
        isExtraLib = true,
        scalaMajorVersion = scalaMajorVersion
      )
    } else Success(Nil)

}
