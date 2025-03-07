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

import optimus.buildtool.config.AfsDependencies
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.MavenDependencies

import scala.collection.immutable.Seq

final case class CentralDependencies(
    jvmDependencies: JvmDependencies,
    jdkDependencies: JdkDependencies,
    pythonDependencies: PythonDependencies
) {
  lazy val externalDependencies: ExternalDependencies = {
    val loadedMultiSourceDeps =
      jvmDependencies.multiSourceDependencies.getOrElse(MultiSourceDependencies(Seq.empty))
    val afsMappedDeps = loadedMultiSourceDeps.multiSourceDeps.map(_.asExternalDependency)
    val unmappedAfsDeps =
      jvmDependencies.dependencies ++ loadedMultiSourceDeps.afsOnlyDeps.map(_.definition)
    val unmappedMavenDeps = jvmDependencies.mavenDependencies
    val mappedMavenDeps = afsMappedDeps.flatMap(_.equivalents).distinct
    val mixModeMavenDeps = mappedMavenDeps ++ loadedMultiSourceDeps.mavenOnlyDeps.map(_.definition)
    // only be used for transitive mapping without forced version
    val noVersionMavenDeps = loadedMultiSourceDeps.noVersionMavenDeps.map(_.definition)

    val allAfs = AfsDependencies(unmappedAfsDeps, afsMappedDeps)
    val allMaven = MavenDependencies(unmappedMavenDeps, mixModeMavenDeps, noVersionMavenDeps)
    ExternalDependencies(allAfs, allMaven)
  }
}
