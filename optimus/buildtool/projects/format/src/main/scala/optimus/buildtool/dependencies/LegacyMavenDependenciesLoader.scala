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
import optimus.buildtool.format.MavenDependenciesConfig
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.Keys
import optimus.buildtool.format.MavenDefinition
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.format.Result

import scala.collection.compat._

final case class LegacyMavenDependencies(
    dependencies: Seq[DependencyDefinition],
    excludes: Seq[Exclude],
    extraLibs: Seq[DependencyDefinition],
    mavenDefinition: MavenDefinition
)

class LegacyMavenDependenciesLoader(
    override protected val centralConfiguration: ProjectProperties,
    override protected val loadedResolvers: ResolverDefinitions,
    override protected val scalaMajorVersion: Option[String],
    useMavenLibs: Boolean
) extends LegacyDependenciesLoader[LegacyMavenDependencies] {

  override protected val file: DependencyConfig = MavenDependenciesConfig
  override protected val isMaven: Boolean = true

  protected def load(config: Config): Result[LegacyMavenDependencies] = {
    val mavenDeps = for {
      dependencies <- loadDependencies(config)
      excludes <- DependencyLoader.loadExcludes(config, file)
      extraLibs <- loadExtraLibs(config)
      mavenDef <- MavenDefinition.loadMavenDefinition(config, isMavenCfg = true, useMavenLibs)
    } yield LegacyMavenDependencies(
      dependencies,
      excludes,
      extraLibs,
      mavenDef
    )
    mavenDeps.withProblems(config.checkExtraProperties(file, Keys.mavenDependenciesFile))
  }

  override protected val empty: LegacyMavenDependencies =
    LegacyMavenDependencies(Nil, Nil, Nil, MavenDefinition.empty)
}
