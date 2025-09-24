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
import optimus.buildtool.format.BuildDependenciesConfig
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.Keys
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result

import scala.collection.compat._

final case class LegacyBuildDependencies(dependencies: Seq[DependencyDefinition])

class LegacyBuildDependenciesLoader(
    override protected val centralConfiguration: ProjectProperties,
    override protected val loadedResolvers: ResolverDefinitions,
    override protected val scalaMajorVersion: Option[String]
) extends LegacyDependenciesLoader[LegacyBuildDependencies] {

  override protected val file: DependencyConfig = BuildDependenciesConfig
  override protected val isMaven: Boolean = false

  protected def load(config: Config): Result[LegacyBuildDependencies] = {
    val afsDeps = for {
      dependencies <- loadDependencies(config)
    } yield LegacyBuildDependencies(dependencies)
    afsDeps.withProblems(config.checkExtraProperties(file, Keys.buildDependenciesFile))
  }

  override protected val empty: LegacyBuildDependencies = LegacyBuildDependencies(Nil)
}
