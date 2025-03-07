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
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._
import OrderingUtils._
import optimus.buildtool.config.OrderedElement

import scala.collection.immutable.Seq

final case class ModuleSetDefinition(
    id: ModuleSetId,
    publicModules: Set[Module],
    privateModules: Set[Module],
    canDependOn: Set[(ModuleSetId, Int)],
    line: Int
) extends OrderedElement[ModuleSetId] {
  def moduleSet: ModuleSet = ModuleSet(id, canDependOn.map(_._1))
}

object ModuleSetDefinition {
  def load(loader: ObtFile.Loader, bundles: Seq[Bundle]): Result[Seq[ModuleSetDefinition]] =
    ModuleSetsConfig.tryWith {
      for {
        config <- loader(ModuleSetsConfig)
        resolver <- load(config, bundles)
      } yield resolver
    }

  private def load(config: Config, bundles: Seq[Bundle]): Result[Seq[ModuleSetDefinition]] = {
    val bundleMap = bundles.map(b => b.id -> b).toMap

    config
      .optionalConfig(Keys.ModuleSets)
      .map { cfg =>
        cfg.nested(ModuleSetsConfig).flatMap { moduleSets =>
          Result
            .traverse(moduleSets) { case (moduleSetName, moduleSetCfg) =>
              val moduleSet = for {
                publicModules <- loadModules(moduleSetCfg.optionalConfig(Keys.Public), bundleMap)
                privateModules <- loadModules(moduleSetCfg.optionalConfig(Keys.Private), bundleMap)
                canDependOn <- loadCanDependOn(moduleSetCfg, moduleSets.map(_._1).toSet)
              } yield ModuleSetDefinition(
                id = ModuleSetId(moduleSetName),
                publicModules = publicModules,
                privateModules = privateModules,
                canDependOn = canDependOn,
                line = moduleSetCfg.origin.lineNumber
              )
              moduleSet.withProblems(moduleSetCfg.checkExtraProperties(ModuleSetsConfig, Keys.moduleSetDefinition))
            }
            .withProblems { moduleSetDefinitions =>
              val ordering = OrderingUtils.checkOrderingIn(ModuleSetsConfig, moduleSetDefinitions)

              val allSets = moduleSetDefinitions.map(ms => ms.moduleSet.id).toSet
              val unknownSets = moduleSetDefinitions.flatMap { ms =>
                ms.moduleSet.canDependOn
                  .diff(allSets)
                  .map(id => ModuleSetsConfig.errorAt(cfg.root, s"Unknown dependency $id in ${ms.moduleSet.id}"))
              }

              val circularDeps = {
                val byId = moduleSetDefinitions.map(ms => ms.moduleSet.id -> ms).toMap
                def inner(id: ModuleSetId, line: Int, path: Seq[ModuleSetId]): Set[Error] = {
                  if (path.headOption.contains(id)) {
                    // we have a minimum-length cycle
                    val cycleStr = (path :+ id).map(_.name).mkString(" -> ")
                    Set(Error(s"Circular dependency detected: $cycleStr", ModuleSetsConfig, line))
                  } else if (path.contains(id)) {
                    // we have a cycle further down the path - stop walking the sets here, but don't report an
                    // error (it will be caught as a shorter cycle in the block above)
                    Set.empty
                  } else {
                    byId(id).canDependOn.flatMap { case (dep, line) => inner(dep, line, path :+ id) }
                  }
                }
                allSets.flatMap(inner(_, 0, Nil))
              }

              ordering ++ unknownSets ++ circularDeps
            }
        }
      }
      .getOrElse(Success(Nil))
      .withProblems(config.checkExtraProperties(ModuleSetsConfig, Keys.moduleSetsDefinition))
  }

  private def loadModules(config: Option[Config], bundles: Map[MetaBundle, Bundle]): Result[Set[Module]] = config
    .map { cfg =>
      val modules = for {
        (moduleName, moduleCfg) <- ResultSeq(cfg.nested(ModuleSetsConfig, 3))
        moduleId = ModuleId.parse(moduleName)
        bundle <- ResultSeq.single(getBundle(moduleId.metaBundle, bundles, moduleCfg))
        module <- ResultSeq.single(loadModule(moduleName, bundle, moduleCfg))
      } yield module

      modules.value.withProblems(mods => OrderingUtils.checkOrderingIn(ModuleSetsConfig, mods)).map(_.toSet)
    }
    .getOrElse(Success(Set.empty))

  private def getBundle(id: MetaBundle, bundles: Map[MetaBundle, Bundle], cfg: Config): Result[Bundle] =
    bundles.get(id) match {
      case Some(bundle) => Success(bundle)
      case None =>
        Failure(
          Seq(
            ModuleSetsConfig
              .errorAt(cfg.root, s"Bundle '${id.properPath}' not found in ${BundlesConfig.path.pathString}")
          )
        )
    }

  private def loadModule(name: String, bundle: Bundle, conf: Config) =
    Result.tryWith(ModuleSetsConfig, conf) {
      val id = ModuleId.parse(name)
      Success(
        Module(
          id = id,
          path = RelativePath(s"${bundle.modulesRoot}/${id.module}/${id.module}.obt"),
          owner = conf.getString("owner"),
          owningGroup = conf.getString("group"),
          line = conf.root().origin().lineNumber()
        )
      )
    }

  private def loadCanDependOn(cfg: Config, moduleSets: Set[String]): Result[Set[(ModuleSetId, Int)]] =
    Result.tryWith(ModuleSetsConfig, cfg) {
      Result
        .traverse(cfg.stringConfigListOrEmpty(Keys.CanDependOn)) { case (id, idCfg) =>
          if (moduleSets.contains(id)) Success((ModuleSetId(id), idCfg.origin().lineNumber()))
          else Failure(Seq(ModuleSetsConfig.errorAt(idCfg, s"Unknown module set '$id'")))
        }
        .map(_.toSet)
    }

}
