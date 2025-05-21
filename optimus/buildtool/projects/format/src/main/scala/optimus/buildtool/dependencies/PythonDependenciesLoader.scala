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

import com.typesafe.config.Config
import optimus.buildtool.config.ModuleType
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.Failure
import optimus.buildtool.format.Keys
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.ResultSeq
import optimus.buildtool.format.Success
import optimus.buildtool.format.SuccessSeq
import optimus.buildtool.format.TopLevelConfig

import scala.collection.compat._
import scala.collection.immutable.Seq
object PythonDependenciesLoader {

  private[buildtool] val Dependencies = "dependencies"
  private[buildtool] val PythonPath = "path"
  private[buildtool] val Python = "python"
  private[buildtool] val Reason = "reason"
  private[buildtool] val RuffVersion = "ruff"
  private[buildtool] val ThinPyapp = "thin-pyapp"
  private[buildtool] val Variants = "variants"
  private[buildtool] val Version = "version"

  def lacksDependencies(origin: ObtFile) =
    s"""This file should contain dependencies config, please add 'dependencies {}' section in ${origin.path}"""
  def lacksPythonVersion(origin: ObtFile) =
    s"""This file should contain python definitions, please add 'python {}' section in ${origin.path}"""

  def load(topFile: TopLevelConfig, loader: ObtFile.Loader): Result[PythonDependencies] = {
    if (loader.exists(topFile))
      for {
        conf <- loader(topFile)
        deps <- fromConfig(conf, topFile)
      } yield deps
    else {
      // This file is optional
      Success(PythonDependencies.empty())
    }
  }

  def fromConfig(config: Config, origin: ObtFile): Result[PythonDependencies] = Result.tryWith(origin, config) {
    def loadAllPythonDefinitions(pythonConfig: Config): Result[Seq[PythonDefinition]] = {
      ResultSeq(loadPythonDefinitions(pythonConfig)).value
        .withProblems(pythonConfig.checkExtraProperties(origin, Keys.pythonDefinition))
    }

    def loadPythonDefinitions(config: Config): Result[Seq[PythonDefinition]] = Result.tryWith(origin, config) {
      for {
        pythonVariants <-
          if (!config.hasPath(Variants)) Success(Nil)
          else
            Result.traverse(config.getObject(Variants).toConfig.nested(origin)) { case (variantName, config) =>
              Result.tryWith(origin, config) {
                loadPythonVariant(variantName, config).withProblems(
                  config.checkExtraProperties(origin, Keys.pythonVariant))
              }
            }

        mainPython <- loadPython(config)

      } yield {
        (pythonVariants ++ Seq(mainPython)).flatten
      }
    }

    def loadPython(config: Config): Result[Option[PythonDefinition]] =
      Result.tryWith(origin, config) {
        val version = config.getString(Version)
        val path = config.getString(PythonPath)
        val thinPyapp = config.getString(ThinPyapp)
        val ruffVersion = config.getString(RuffVersion)
        Success(PythonDefinition(version, path, thinPyapp, ruffVersion, None))
      } match {
        case Success(result, problems) => Success(Some(result), problems)
        case Failure(problems)         => Success(None, problems)
      }

    def loadPythonVariant(variantName: String, config: Config): Result[Option[PythonDefinition]] =
      for {
        variant <- loadVariant(variantName, config)
        python <- loadPython(config)
      } yield python.map(py => PythonDefinition(py.version, py.path, py.thinPyapp, py.ruffVersion, Some(variant)))

    def loadVariant(name: String, config: Config): Result[PythonDependencies.Variant] =
      Success(
        PythonDependencies.Variant(
          name,
          config.getString(Reason)
        )
      )

    for {
      dependenciesConfig <-
        if (config.hasPath(Dependencies)) Success(config.getConfig(Dependencies))
        else Failure(lacksDependencies(origin), origin)

      dependencies <- loadAllDependencies(dependenciesConfig, origin)

      pythonDefConfig <-
        if (config.hasPath(Python)) Success(config.getConfig(Python))
        else Failure(lacksPythonVersion(origin), origin)

      pythonDefinitions <- loadAllPythonDefinitions(pythonDefConfig)

    } yield PythonDependencies(pythonDefinitions.toSet, dependencies.toSet)

  }

  def loadAllDependencies(config: Config, origin: ObtFile): Result[Seq[PythonDependency]] = {
    def withPath[A](config: Config, path: String)(f: Config => Result[A]): Result[Option[A]] = {
      if (config.hasPath(path)) {
        val theConfig = config.getConfig(path)
        (f(theConfig)).map(Some(_))
      } else Success(None)
    }

    def loadPypiDef(name: String, depConfig: Config, origin: ObtFile): Result[Option[PythonDependencyDefinition]] =
      Result.tryWith(origin, depConfig) {
        withPath(depConfig, ModuleType.PyPi.label) { pypiConfig =>
          val pypiName = pypiConfig.keySet.head
          val version = pypiConfig.getConfig(pypiName).getString(Version)
          Success(PythonDependencyDefinition(name, pypiName, version, None))
        }
      }

    def loadAfsDef(name: String, depConfig: Config, origin: ObtFile): Result[Option[PythonAfsDependencyDefinition]] =
      Result.tryWith(origin, depConfig) {
        withPath(depConfig, ModuleType.Afs.label) { afsConfig =>
          val res = for {
            (meta, metaConfig) <- ResultSeq(afsConfig.nested(origin))
            (project, projectConfig) <- ResultSeq(metaConfig.nested(origin))
            version = projectConfig.getString(Version)
          } yield PythonAfsDependencyDefinition(name, meta, project, version, None)
          res.value
            .withProblems { deps =>
              if (deps.size != 1)
                Seq(origin.errorAt(afsConfig.root, s"Expected a single dependency but got ${deps.size}"))
              else Nil
            }
            .map(_.head)
        }
      }

    final case class VariantConfig(name: String, reason: String, version: String) {
      def variant: PythonDependencies.Variant = PythonDependencies.Variant(name, reason)
    }
    def loadVariantConf(name: String, config: Config): Result[VariantConfig] =
      Success(
        VariantConfig(
          name,
          config.getString(Reason),
          config.getString(Version)
        )
      )

    def loadVariants(
        depConfig: Config,
        origin: ObtFile,
        pypiDef: Option[PythonDependencyDefinition],
        afsDef: Option[PythonAfsDependencyDefinition]): Result[Seq[PythonDependency]] =
      Result
        .tryWith(origin, depConfig) {
          withPath(depConfig, Variants) { variantsConfig =>
            val deps: ResultSeq[PythonDependency] = for {
              (variantName, variantConfig) <- ResultSeq(variantsConfig.nested(origin))
              (sourceName, sourceConfig) <- ResultSeq(variantConfig.nested(origin))
              variant <- ResultSeq.single(loadVariantConf(variantName, sourceConfig))
              dep <- SuccessSeq(
                if (sourceName == ModuleType.Afs.label)
                  afsDef.map(afs => afs.copy(variant = Some(variant.variant), version = variant.version)).to(Seq)
                else if (sourceName == ModuleType.PyPi.label)
                  pypiDef.map(pypi => pypi.copy(variant = Some(variant.variant), version = variant.version)).to(Seq)
                else Nil
              )
            } yield dep
            deps.value
          }
        }
        .map(_.getOrElse(Nil))

    def loadDependency(label: String, config: Config): Result[Seq[PythonDependency]] = {
      val definitions = for {
        pypi <- loadPypiDef(label, config, origin)
        afs <- loadAfsDef(label, config, origin)
        variants <- loadVariants(config, origin, pypi, afs)

      } yield variants ++ pypi ++ afs

      definitions.withProblems(config.checkExtraProperties(origin, Keys.pythonDependencyLevel))
    }

    val loadedDependencies = for {
      (depName, depConfig) <- ResultSeq(config.nested(origin))
      lib <- ResultSeq(loadDependency(depName, depConfig))

    } yield lib

    loadedDependencies.value
  }
}
