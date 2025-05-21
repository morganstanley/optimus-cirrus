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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._
import OrderingUtils._
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.OrderedElement
import optimus.buildtool.dependencies.DependencySetId
import optimus.buildtool.dependencies.VariantSetId

import scala.collection.compat._
import scala.collection.immutable.Seq

final case class ModuleSetDefinition(
    id: ModuleSetId,
    publicModules: Set[Module],
    privateModules: Set[Module],
    canDependOn: Set[(ModuleSetId, Int)],
    dependencySets: Set[(DependencySetId, Int)],
    variantSets: Set[(VariantSetId, Int)],
    file: ObtFile,
    line: Int
) extends OrderedElement[ModuleSetId]

object ModuleSetDefinition {

  def load(
      loader: ObtFile.ScanningLoader,
      bundles: Seq[Bundle],
      allDependencySets: Set[DependencySetId],
      allVariantSets: Set[VariantSetId]
  ): Result[Set[ModuleSetDefinition]] = {
    val bundleMap = bundles.map(b => b.id -> b).toMap
    val dependencySetMap = allDependencySets.map(id => id.name -> id).toMap
    val variantSetMap = allVariantSets.map(id => id.name -> id).toMap

    val files = loader.files(ModuleSetConfig.Root).map(ModuleSetConfig(_))
    val newModuleSetIdsByFile =
      files.map(f => f -> ModuleSetId(f.path.name.stripSuffix(s".${NamingConventions.ObtExt}"))).toMap
    val newModuleSetIdsByName = newModuleSetIdsByFile.values.map(id => id.name -> id).toMap

    for {
      legacySets <- loadLegacySets(loader, bundleMap, newModuleSetIdsByName, dependencySetMap, variantSetMap)
      allModuleSetMap = newModuleSetIdsByName ++ legacySets.map(d => d.id.name -> d.id)
      newSets <- Result.traverse(files) { f =>
        val id = newModuleSetIdsByFile(f)
        loadModuleSetFile(id, loader, f, bundleMap, allModuleSetMap, dependencySetMap, variantSetMap)
      }
      allSets <- validate(legacySets ++ newSets.toSet)
    } yield allSets
  }

  private def loadLegacySets(
      loader: ObtFile.ScanningLoader,
      bundleMap: Map[MetaBundle, Bundle],
      newModuleSets: Map[String, ModuleSetId],
      allDependencySets: Map[String, DependencySetId],
      allVariantSets: Map[String, VariantSetId]
  ): Result[Set[ModuleSetDefinition]] = ModuleSetsConfig.tryWith {
    def loadSets(config: Config) = {
      val msConfig = config.getConfig(Keys.ModuleSets)
      msConfig
        .nested(ModuleSetsConfig)
        .flatMap { legacyModuleSets =>
          val allModuleSets =
            legacyModuleSets.map { case (name, _) => name -> ModuleSetId(name) }.toMap ++ newModuleSets
          Result
            .traverse(legacyModuleSets) { case (moduleSetName, moduleSetCfg) =>
              val id = ModuleSetId(moduleSetName)
              loadModuleSet(
                id,
                ModuleSetsConfig,
                moduleSetCfg,
                bundleMap,
                allModuleSets,
                allDependencySets,
                allVariantSets
              )
            }
            .withProblems { moduleSetDefinitions =>
              config.checkExtraProperties(ModuleSetsConfig, Keys.moduleSetsDefinition) ++
                OrderingUtils.checkOrderingIn(ModuleSetsConfig, moduleSetDefinitions)
            }
        }
        .map(_.toSet)
    }

    if (loader.exists(ModuleSetsConfig)) {
      for {
        config <- loader(ModuleSetsConfig)
        sets <- loadSets(config)
      } yield sets
    } else Success(Set.empty)
  }

  private def loadModuleSetFile(
      id: ModuleSetId,
      loader: ObtFile.Loader,
      file: ObtFile,
      bundles: Map[MetaBundle, Bundle],
      moduleSets: Map[String, ModuleSetId],
      allDependencySets: Map[String, DependencySetId],
      allVariantSets: Map[String, VariantSetId]
  ): Result[ModuleSetDefinition] = file.tryWith {
    for {
      config <- loader(file)
      ms <- loadModuleSet(
        id,
        file,
        config.getConfig(Keys.ModuleSet),
        bundles,
        moduleSets,
        allDependencySets,
        allVariantSets
      ).withProblems(config.checkExtraProperties(file, Keys.moduleSetFile))
    } yield ms

  }

  private def loadModuleSet(
      id: ModuleSetId,
      file: ObtFile,
      moduleSetCfg: Config,
      bundles: Map[MetaBundle, Bundle],
      moduleSets: Map[String, ModuleSetId],
      allDependencySets: Map[String, DependencySetId],
      allVariantSets: Map[String, VariantSetId]
  ): Result[ModuleSetDefinition] = {
    val moduleSet = for {
      publicModules <- loadModules(file, moduleSetCfg.optionalConfig(Keys.Public), bundles)
      privateModules <- loadModules(file, moduleSetCfg.optionalConfig(Keys.Private), bundles)
      canDependOn <- loadSetsFromConfig(file, moduleSetCfg, Keys.CanDependOn, "module", moduleSets)
      dependencySets <- loadSetsFromConfig(file, moduleSetCfg, Keys.DependencySets, "dependency", allDependencySets)
      variantSets <- loadSetsFromConfig(file, moduleSetCfg, Keys.VariantSets, "variant", allVariantSets)
    } yield {
      ModuleSetDefinition(
        id = id,
        publicModules = publicModules,
        privateModules = privateModules,
        canDependOn = canDependOn,
        dependencySets = dependencySets,
        variantSets = variantSets,
        file = file,
        line = moduleSetCfg.origin.lineNumber
      )
    }
    moduleSet.withProblems(moduleSetCfg.checkExtraProperties(file, Keys.moduleSetDefinition))
  }

  private def loadModules(
      file: ObtFile,
      config: Option[Config],
      bundles: Map[MetaBundle, Bundle]
  ): Result[Set[Module]] = config
    .map { cfg =>
      val modules = for {
        (moduleName, moduleCfg) <- ResultSeq(cfg.nested(file, 3))
        moduleId = ModuleId.parse(moduleName)
        bundle <- ResultSeq.single(getBundle(moduleId.metaBundle, bundles, file, moduleCfg))
        module <- ResultSeq.single(loadModule(moduleName, bundle, file, moduleCfg))
      } yield module

      modules.value.withProblems(mods => OrderingUtils.checkOrderingIn(file, mods)).map(_.toSet)
    }
    .getOrElse(Success(Set.empty))

  private def getBundle(id: MetaBundle, bundles: Map[MetaBundle, Bundle], file: ObtFile, cfg: Config): Result[Bundle] =
    bundles.get(id) match {
      case Some(bundle) => Success(bundle)
      case None =>
        Failure(
          Seq(file.errorAt(cfg.root, s"Bundle '${id.properPath}' not found in ${BundlesConfig.path.pathString}"))
        )
    }

  private def loadModule(name: String, bundle: Bundle, file: ObtFile, conf: Config) =
    Result.tryWith(file, conf) {
      val id = ModuleId.parse(name)
      Success(
        Module(
          id = id,
          path = RelativePath(s"${bundle.modulesRoot}/${id.module}/${id.module}.obt"),
          owner = conf.getString("owner"),
          owningGroup = conf.getString("group"),
          line = conf.root().origin().lineNumber()
        )
      ).withProblems(conf.checkExtraProperties(file, Keys.moduleOwnership))
    }

  private def loadSetsFromConfig[A](
      file: ObtFile,
      cfg: Config,
      key: String,
      descriptor: String,
      allIds: Map[String, A]
  )(implicit ord: NamedOrdering[A]): Result[Set[(A, Int)]] =
    Result.tryWith(file, cfg) {
      Result
        .traverse(cfg.stringConfigListOrEmpty(key)) { case (idStr, idCfg) =>
          allIds
            .get(idStr)
            .map(id => Success((id, idCfg.origin().lineNumber())))
            .getOrElse(Failure(Seq(file.errorAt(idCfg, s"Unknown $descriptor set '$idStr'"))))
        }
        .withProblems(OrderingUtils.checkOrderingInTuples(file, _))
        .map(_.toSet)
    }

  private def validate(moduleSetDefinitions: Set[ModuleSetDefinition]): Result[Set[ModuleSetDefinition]] = {
    val allSets = moduleSetDefinitions.map(ms => (ms.id, ms.file))
    val byId = moduleSetDefinitions.map(ms => ms.id -> ms).toMap

    val circularDeps = {
      def inner(id: ModuleSetId, file: ObtFile, line: Int, path: Seq[ModuleSetId]): Set[Error] = {
        if (path.headOption.contains(id)) {
          // we have a minimum-length cycle
          val cycleStr = (path :+ id).map(_.name).mkString(" -> ")
          Set(Error(s"Circular dependency detected: $cycleStr", file, line))
        } else if (path.contains(id)) {
          // we have a cycle further down the path - stop walking the sets here, but don't report an
          // error (it will be caught as a shorter cycle in the block above)
          Set.empty
        } else {
          for {
            set <- byId.get(id).toSet[ModuleSetDefinition]
            (dep, line) <- byId(id).canDependOn
            error <- inner(dep, set.file, line, path :+ id)
          } yield error
        }
      }
      allSets.flatMap { case (id, file) => inner(id, file, 0, Nil) }
    }

    // if we've got circular deps, it's not safe to pass the module sets onward (or we risk a stack overflow)
    if (circularDeps.isEmpty)
      Success(moduleSetDefinitions)
        .withProblems {
          def transitiveCanDependOn(ms: ModuleSetDefinition): Set[ModuleSetDefinition] = {
            val transitives = for {
              (cdoId, _) <- ms.canDependOn
              cdo <- byId.get(cdoId).toSet[ModuleSetDefinition]
              tcdo <- transitiveCanDependOn(cdo)
            } yield tcdo
            transitives + ms
          }

          // check for cases where this module set specifies a variant set but the matching dependency set
          // isn't in the list of transitive can-depend-ons
          val missingDependencies = for {
            ms <- moduleSetDefinitions
            cdos = transitiveCanDependOn(ms)
            (vs, vsLine) <- ms.variantSets
            if !cdos.exists(cdo => cdo.dependencySets.exists(_._1 == vs.dependencySetId))
          } yield Error(
            s"Module set ${ms.id.name} specifies variant set ${vs.name}, but no module sets it can depend on include the dependency set ${vs.dependencySetId.name}",
            ms.file,
            vsLine
          )

          // check for cases where this module set transitively depends on one which specifies a variant set
          // but an equivalent variant set (ie. one for the same dependency set) doesn't exist in this module set
          val missingVariants = for {
            ms <- moduleSetDefinitions
            cdo <- transitiveCanDependOn(ms)
            (cdoVs, _) <- cdo.variantSets if !ms.variantSets.exists { case (vs, _) =>
              vs.dependencySetId == cdoVs.dependencySetId
            }
          } yield Error(
            s"Module set ${ms.id.name} must specify a variant set for ${cdoVs.dependencySetId.name}, since it depends on module set ${cdo.id.name} which uses variant set ${cdoVs.name}",
            ms.file,
            ms.line
          )

          val multipleVariants = for {
            ms <- moduleSetDefinitions
            (depSet, variantSets) <- ms.variantSets.groupBy(_._1.dependencySetId) if variantSets.size > 1
            (_, vsLine) <- variantSets
          } yield Error(
            s"Duplicate variant sets specified in module set ${ms.id.name} for dependency set ${depSet.name}: ${variantSets
                .map(_._1.name)
                .mkString(", ")}",
            ms.file,
            vsLine
          )

          missingDependencies.to(Seq) ++ missingVariants ++ multipleVariants
        }
    else Failure(circularDeps.to(Seq))
  }

}
