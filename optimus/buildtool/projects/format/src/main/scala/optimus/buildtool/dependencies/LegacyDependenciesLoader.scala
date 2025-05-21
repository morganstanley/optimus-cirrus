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
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result
import optimus.buildtool.format.Success
import optimus.buildtool.format.Warning

import scala.collection.immutable.Seq

trait LegacyDependenciesLoader[A] {

  protected def file: DependencyConfig
  protected def isMaven: Boolean

  protected def centralConfiguration: ProjectProperties
  protected def loadedResolvers: ResolverDefinitions
  protected def scalaMajorVersion: Option[String]

  def load(loader: ObtFile.Loader): Result[A] =
    if (loader.exists(file)) {
      file.tryWith {
        for {
          conf <- loader(file)
          deps <- load(conf.withFallback(centralConfiguration.config).resolve())
        } yield deps
      }
    } else {
      Success(empty).withProblems(Seq(Warning(s"OBT file missing: ${file.path}", file)))
    }

  protected def load(config: Config): Result[A]
  protected def empty: A

  protected def loadDependencies(conf: Config): Result[Seq[DependencyDefinition]] =
    DependencyLoader.loadLocalDefinitions(
      conf.getConfig(JvmDependenciesLoader.Dependencies),
      JvmDependenciesLoader.Dependencies,
      LocalDefinition,
      file,
      isMaven,
      loadedResolvers,
      scalaMajorVersion = scalaMajorVersion
    )

  protected def loadExtraLibs(conf: Config): Result[Seq[DependencyDefinition]] =
    DependencyLoader.loadExtraLibs(
      conf,
      file,
      isMaven,
      loadedResolvers,
      scalaMajorVersion
    )
}
