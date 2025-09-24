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
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.duplicationMsg
import optimus.buildtool.format.MavenDependenciesConfig
import optimus.buildtool.format.BuildDependenciesConfig
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.DependenciesConfig
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.DependencySetConfig
import optimus.buildtool.format.Error
import optimus.buildtool.format.Failure
import optimus.buildtool.format.Keys
import optimus.buildtool.format.JvmDependenciesConfig
import optimus.buildtool.format.Keys.substitutionsConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result
import optimus.buildtool.format.SetConfig
import optimus.buildtool.format.Success
import optimus.buildtool.format.Warning
import optimus.buildtool.format.WorkspaceStructure

import scala.collection.compat._
import scala.jdk.CollectionConverters._

object JvmDependenciesLoader {
  private[buildtool] val Artifacts = "artifacts"
  private[buildtool] val Configuration = "configuration"
  private[buildtool] val Configurations = "configurations"
  private[buildtool] val IvyConfigurations = "ivyConfigurations"
  private[buildtool] val IvyConfiguration = "ivyConfiguration"
  private[buildtool] val ContainsMacros = "containsMacros"
  private[buildtool] val Classifier = "classifier"
  private[buildtool] val JvmDependenciesKey = "jvm-dependencies"
  private[buildtool] val Dependencies = "dependencies"
  private[buildtool] val Excludes = "excludes"
  private[buildtool] val ExtraLibs = "extraLibs"
  private[buildtool] val Force = "force"
  private[buildtool] val Group = "group"
  private[buildtool] val Groups = "groups"
  private[buildtool] val IsScalacPlugin = "isScalacPlugin"
  private[buildtool] val Version = "version"
  private[buildtool] val Variants = "variants"
  private[buildtool] val Name = "name"
  private[buildtool] val NativeDependencies = "nativeDependencies"
  private[buildtool] object PublicationSettingsKeys {
    val key = "publicationSettings"
    val publicationName = "name"
    val tpe = "tpe"
    val classifier = "classifier"
    val ext = "ext"
  }
  private[buildtool] val Resolvers = "resolvers"
  private[buildtool] val Substitutions = "substitutions"

  private[buildtool] val Ext = "ext"
  private[buildtool] val KeySuffix = "keySuffix"
  private[buildtool] val Reason = "reason"
  private[buildtool] val TypeStr = "type"
  private[buildtool] val Transitive = "transitive"

  def loadSubstitutions(config: Config, file: ObtFile): Result[Seq[Substitution]] = {
    file.tryWith {
      if (config.hasPath(Substitutions)) {
        val subsList = config.getList(Substitutions).asScala.toList
        val subsResults: Seq[Result[Substitution]] = subsList.map {
          case obj: ConfigObject =>
            val subsConf: Config = obj.toConfig
            val substitution = {
              val fromGroup = subsConf.getString(s"fromGroup")
              val fromName = subsConf.getString(s"fromName")
              val fromConfig = subsConf.optionalString(s"fromConfig")
              val toGroup = subsConf.getString(s"toGroup")
              val toName = subsConf.getString(s"toName")
              val toConfig = subsConf.optionalString(s"toConfig")
              Substitution(GroupNameConfig(fromGroup, fromName, fromConfig), GroupNameConfig(toGroup, toName, toConfig))
            }
            Success(substitution)
              .withProblems(
                subsConf.checkExtraProperties(file, substitutionsConfig) ++
                  subsConf.checkEmptyProperties(file, substitutionsConfig))
          case other =>
            file.failure(other, s""""$Substitutions" element is not an object""")
        }.distinct
        Result.sequence(subsResults)
      } else Success(Nil)
    }
  }

  def versionsAsConfig(deps: Iterable[DependencyDefinition]): Config = {
    def versionConfig(dep: DependencyDefinition): ConfigObject =
      ConfigFactory.empty().root().withValue(Version, ConfigValueFactory.fromAnyRef(dep.version))

    def groupConfig(deps: Iterable[DependencyDefinition]): ConfigObject = {
      deps.foldLeft(ConfigFactory.empty().root()) { (root, dep) =>
        root.withValue(dep.name, versionConfig(dep))
      }
    }

    deps
      .groupBy(_.group)
      .foldLeft(ConfigFactory.empty().root()) { case (config, (group, deps)) =>
        config.withValue(group, groupConfig(deps))
      }
      .toConfig
  }

  def load(
      centralConfiguration: ProjectProperties,
      loader: ObtFile.Loader,
      workspace: WorkspaceStructure,
      useMavenLibs: Boolean,
      scalaMajorVersion: Option[String],
      loadedResolvers: ResolverDefinitions
  ): Result[JvmDependencies] = {

    val afsLoader =
      new LegacyAfsDependenciesLoader(centralConfiguration, loadedResolvers, scalaMajorVersion)
    val mavenLoader =
      new LegacyMavenDependenciesLoader(centralConfiguration, loadedResolvers, scalaMajorVersion, useMavenLibs)
    val buildDepsLoader =
      new LegacyBuildDependenciesLoader(centralConfiguration, loadedResolvers, scalaMajorVersion)

    def loadMultiSourceDeps(conf: Config, file: DependencyConfig, key: String): Result[MultiSourceDependencies] =
      if (conf.hasPath(key))
        MultiSourceDependenciesLoader.load(conf, key, file, loadedResolvers, scalaMajorVersion)
      else
        Success(MultiSourceDependencies(Nil, Nil)).withProblems(Seq(Warning(s"OBT file missing: ${file.path}", file)))

    def loadDependencySets(variantSets: Set[VariantSet]): Result[Set[DependencySet]] = {
      val configFiles = workspace.dependencySets.to(Seq)

      def depSet(id: DependencySetId, file: DependencySetConfig, config: Config): Result[DependencySet] = {
        val hasDefaultVariant = config.hasPath(Keys.DefaultVariant)
        val hasJvmDeps = config.hasPath(JvmDependenciesKey)
        if (hasDefaultVariant && hasJvmDeps) {
          val msg = s"Cannot have both ${Keys.DefaultVariant} and $JvmDependenciesKey for the same dependency set"
          Failure(Seq(Error(msg, file, 0)))
        } else if (hasDefaultVariant) {
          val defaultVariant = config.getString(Keys.DefaultVariant)
          val variant = variantSets.find(_.id.name == defaultVariant) match {
            case Some(v) =>
              Success(v)
            case None =>
              Failure(Seq(file.errorAt(config.getValue(Keys.DefaultVariant), s"Variant set $defaultVariant not found")))
          }
          val ds = for {
            v <- variant
            boms <- loadBoms(config, file)
          } yield DependencySet(id, v.dependencies, boms)
          ds.withProblems(config.checkExtraProperties(file, Keys.dependencySetVariant))
        } else {
          val ds = for {
            deps <- loadMultiSourceDeps(config, file, JvmDependenciesKey)
            boms <- loadBoms(config, file)
          } yield DependencySet(id, deps, boms)
          ds.withProblems(config.checkExtraProperties(file, Keys.dependencySet))
        }
      }

      val dependencySets = configFiles.map { case (id, file) =>
        for {
          config <- loader(file)
          dependencySet <- depSet(id, file, config.withFallback(centralConfiguration.config).resolve())
        } yield dependencySet
      }
      Result.sequence(dependencySets).map(_.toSet)
    }

    def loadVariantSets: Result[Set[VariantSet]] = {
      val configFiles = workspace.variantSets.to(Seq)
      val variantSets = configFiles.map { case (id, file) =>
        for {
          config <- loader(file)
            .map(_.withFallback(centralConfiguration.config).resolve())
            .withProblems(_.checkExtraProperties(file, Keys.variantSet))
          deps <- loadMultiSourceDeps(config, file, JvmDependenciesKey)
          boms <- loadBoms(config, file)
        } yield VariantSet(id, deps, boms)
      }
      Result.sequence(variantSets).map(_.toSet)
    }

    def loadBoms(config: Config, file: SetConfig): Result[Seq[DependencyDefinition]] =
      if (config.hasPath(Keys.Boms)) {
        DependencyLoader.loadLocalDefinitions(
          config.getConfig(Keys.Boms),
          Keys.Boms,
          LocalDefinition,
          file,
          isMaven = true,
          loadedResolvers
        )
      } else Success(Nil)

    val jvmDependencies = for {
      afsDeps <- afsLoader.load(loader)
      mavenDeps <- mavenLoader.load(loader)
      buildDeps <- buildDepsLoader.load(loader)
      jvmConfig <- loader(JvmDependenciesConfig)
        .map(_.withFallback(centralConfiguration.config).resolve())
        .withProblems(_.checkExtraProperties(JvmDependenciesConfig, Keys.jvmDependenciesFile))
      multipleSourceDeps <- loadMultiSourceDeps(jvmConfig, JvmDependenciesConfig, Dependencies)
      globalExcludes <- DependencyLoader.loadExcludes(jvmConfig, JvmDependenciesConfig, allowIvyConfiguration = true)
      globalSubstitutions <- loadSubstitutions(jvmConfig, JvmDependenciesConfig)
      extraLibs <- DependencyLoader.loadExtraLibs(
        jvmConfig,
        JvmDependenciesConfig,
        isMavenConfig = false,
        loadedResolvers,
        scalaMajorVersion
      )
      variantSets <- loadVariantSets
      dependencySets <- loadDependencySets(variantSets)
    } yield {
      def toMultiSource(ds: Seq[DependencyDefinition], file: DependencyConfig) =
        if (file != MavenDependenciesConfig) ds.map(d => MultiSourceDependency(None, AfsOnly(d), file, d.line))
        else ds.map(d => MultiSourceDependency(None, Unmapped(d), file, d.line))

      JvmDependenciesImpl(
        MultiSourceDependencies(
          multipleSourceDeps.all ++
            toMultiSource(extraLibs, JvmDependenciesConfig) ++
            toMultiSource(afsDeps.dependencies ++ afsDeps.extraLibs, DependenciesConfig) ++
            toMultiSource(mavenDeps.dependencies ++ mavenDeps.extraLibs, MavenDependenciesConfig) ++
            toMultiSource(buildDeps.dependencies, BuildDependenciesConfig),
          globalSubstitutions
        ),
        dependencySets,
        variantSets,
        afsDeps.nativeDependencies,
        afsDeps.groups,
        globalExcludes ++ afsDeps.excludes ++ mavenDeps.excludes,
        mavenDeps.mavenDefinition,
        scalaMajorVersion
      )
    }

    jvmDependencies.withProblems(ds => checkDuplicates(ds))
  }

  private def checkDuplicates(deps: JvmDependencies): Seq[Error] = {
    val duplicates = deps
      .dependenciesByKey(deps.dependencies, deps.groups, onlyMavenKeys = false)
      .map { case (key, file, line, _) => key -> (file, line) }
      .groupBy(_._1)
      .collect { case (k, vs) if vs.size > 1 => k -> vs.map(_._2) }

    val errors = for {
      (dupeKey, dupes) <- duplicates
      dupe <- dupes
      (dupeFile, dupeLine) = dupe
    } yield {
      // pick an arbitrary other duplicate to reference in error message
      val (otherFile, otherLine) = if (dupe == dupes.head) dupes(1) else dupes.head
      Error(duplicationMsg(dupeKey, otherFile, otherLine), dupeFile, dupeLine)
    }
    errors.to(Seq)
  }

}
