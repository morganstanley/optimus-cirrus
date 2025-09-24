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
import optimus.buildtool.files.Asset
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.DependenciesConfig
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.Keys
import optimus.buildtool.format.OrderingUtils
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result
import optimus.buildtool.format.Success

import java.nio.file.FileSystems
import scala.collection.compat._
import scala.jdk.CollectionConverters._

final case class LegacyAfsDependencies(
    dependencies: Seq[DependencyDefinition],
    excludes: Seq[Exclude],
    nativeDependencies: Map[String, NativeDependencyDefinition],
    groups: Seq[DependencyGroup],
    extraLibs: Seq[DependencyDefinition]
)

class LegacyAfsDependenciesLoader(
    override protected val centralConfiguration: ProjectProperties,
    override protected val loadedResolvers: ResolverDefinitions,
    override protected val scalaMajorVersion: Option[String]
) extends LegacyDependenciesLoader[LegacyAfsDependencies] {
  import JvmDependenciesLoader._

  override protected val file: DependencyConfig = DependenciesConfig
  override protected val isMaven: Boolean = false

  protected def load(config: Config): Result[LegacyAfsDependencies] = {
    val afsDeps = for {
      dependencies <- loadDependencies(config)
      excludes <- DependencyLoader.loadExcludes(config, file, allowIvyConfiguration = true)
      nativeDependencies <- loadNativeDeps(config)
      groups <- loadGroups(config, dependencies)
      extraLibs <- loadExtraLibs(config)
    } yield LegacyAfsDependencies(dependencies, excludes, nativeDependencies, groups, extraLibs)
    afsDeps.withProblems(config.checkExtraProperties(file, Keys.dependenciesFile))
  }

  override protected val empty: LegacyAfsDependencies = LegacyAfsDependencies(Nil, Nil, Map.empty, Nil, Nil)

  private def loadNativeDeps(root: Config): Result[Map[String, NativeDependencyDefinition]] = {
    val nativeDeps = if (root.hasPath(NativeDependencies)) {
      file.tryWith(root.getValue(NativeDependencies)) {
        val config = root.getConfig(NativeDependencies)
        Result
          .traverse(config.keys(file)) { key =>
            val value = config.getValue(key)
            file.tryWith(value) {
              value.valueType match {
                case ConfigValueType.LIST =>
                  Success(
                    key -> NativeDependencyDefinition(
                      line = value.origin.lineNumber,
                      name = key,
                      paths = config.getStringList(key).asScala.to(Seq),
                      extraPaths = Nil
                    ))
                case ConfigValueType.OBJECT =>
                  val depCfg = config.getConfig(key)
                  Success(
                    key -> NativeDependencyDefinition(
                      line = value.origin.lineNumber,
                      name = key,
                      paths = depCfg.stringListOrEmpty("paths"),
                      extraPaths = depCfg.stringListOrEmpty("extraFiles").map(toAsset)
                    )
                  )
                    .withProblems(depCfg.checkExtraProperties(file, Keys.nativeDependencyDefinition))
                case other =>
                  file
                    .failure(value, s"Native dependency definition should be string or object; got '$other'")
              }
            }
          }
          .map(_.toMap)
      }
    } else Success(Map.empty[String, NativeDependencyDefinition])
    nativeDeps.withProblems { ds =>
      OrderingUtils.checkOrderingIn(file, ds.values.to(Seq): Seq[NativeDependencyDefinition])
    }
  }

  private def loadGroups(
      config: Config,
      dependencies: Iterable[DependencyDefinition]
  ): Result[Seq[DependencyGroup]] = {

    def validateGroup(name: String, values: Seq[String])(groupValue: ConfigValue) = {
      val foundDependencies = values.flatMap(dependencyKey => dependencies.find(_.key == dependencyKey))
      val missingDependencies = values.filterNot(dependencyKey => dependencies.exists(_.key == dependencyKey))
      val problems = missingDependencies.map { missingDep =>
        file.errorAt(groupValue, s"Unknown dependency $missingDep")
      }
      Success(DependencyGroup(name, foundDependencies, groupValue.origin.lineNumber), problems)
    }

    file.tryWith {
      if (config.hasPath(Groups)) {
        val groupsConfig = config.getObject(Groups).toConfig
        Result.sequence(
          groupsConfig.entrySet.asScala
            .map { entry =>
              validateGroup(entry.getKey, groupsConfig.getStringList(entry.getKey).asScala.to(Seq))(entry.getValue)
            }
            .to(Seq))
      } else Success(Nil)
    }
  }

  private def toAsset(s: String): Asset = Asset(FileSystems.getDefault, s)

}
