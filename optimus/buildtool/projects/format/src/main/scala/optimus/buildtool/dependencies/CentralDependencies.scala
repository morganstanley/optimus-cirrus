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
import optimus.buildtool.config.ExternalDependenciesSource
import optimus.buildtool.config.ExternalDependency
import optimus.buildtool.config.MavenDependencies
import optimus.buildtool.config.ModuleSet

import scala.collection.immutable.Seq

final case class CentralDependencies(
    jvmDependencies: JvmDependencies,
    jdkDependencies: JdkDependencies,
    pythonDependencies: PythonDependencies
) extends ExternalDependenciesSource {

  lazy val coreExternalDependencies: ExternalDependencies =
    externalDependencies(Set.empty, Set.empty)

  lazy val allExternalDependencies: ExternalDependencies =
    externalDependencies(jvmDependencies.dependencySets, jvmDependencies.variantSets)

  private val dependencySets = jvmDependencies.dependencySets.map(ds => ds.id -> ds).toMap
  private val variantSets = jvmDependencies.variantSets.map(vs => vs.id -> vs).toMap

  def externalDependencies(ms: ModuleSet): ExternalDependencies =
    externalDependencies(ms.transitiveNonVariantDependencySets.map(dependencySets), ms.variantSets.map(variantSets))

  private def externalDependencies(
      dependencySets: Set[DependencySet],
      variantSets: Set[VariantSet]
  ): ExternalDependencies = {
    val loadedMultiSourceDeps =
      jvmDependencies.dependencies +++
        dependencySets.map(_.dependencies) +++
        variantSets.map(_.dependencies)
    val unmappedAfsDeps = loadedMultiSourceDeps.afsOnly
    val afsMappedDeps = loadedMultiSourceDeps.mapped.map(m => ExternalDependency(m.afs, m.maven))

    val unmappedMavenDeps = loadedMultiSourceDeps.unmapped
    val mixedModeMavenDeps = loadedMultiSourceDeps.mavenOnly ++ loadedMultiSourceDeps.mapped.flatMap(_.maven)

    val boms = dependencySets.flatMap(_.boms) ++ variantSets.flatMap(_.boms)

    val allAfs = AfsDependencies(unmappedAfsDeps, afsMappedDeps)
    val allMaven = MavenDependencies(unmappedMavenDeps, mixedModeMavenDeps, boms.to(Seq))
    ExternalDependencies(allAfs, allMaven)
  }
}
