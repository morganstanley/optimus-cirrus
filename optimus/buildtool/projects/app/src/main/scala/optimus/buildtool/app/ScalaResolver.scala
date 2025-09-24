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
package optimus.buildtool.app
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.ScalaVersion
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.resolvers.CoursierArtifactResolver
import optimus.buildtool.resolvers.DependencyCopier
import optimus.platform.node

object ScalaResolver {
  val group = "org.scala-lang"
  val libraryName = "scala-library"

  val requiredArtifactName = List(
    libraryName,
    "scala-compiler",
    "scala-reflect"
  )
}

class ScalaResolver(
    dependencyResolver: CoursierArtifactResolver,
    dependencyCopier: DependencyCopier,
    scalaVersion: ScalaVersion) {

  private val requiredJars = ScalaResolver.requiredArtifactName.map(artifactName => {
    DependencyDefinition(ScalaResolver.group, artifactName, scalaVersion.value, LocalDefinition)
  })

  @node def fetchScalaArtifacts(): Seq[ExternalClassFileArtifact] = {
    val resolution = dependencyResolver.resolveDependencies(DependencyDefinitions(requiredJars, Nil))
    val resolvedArtifacts = resolution.resolvedClassFileArtifacts
    if (resolution.messages.nonEmpty) throw new Exception(resolution.messages.mkString("\n"))
    resolvedArtifacts.apar.map(dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing)
  }

  @node def scalaVersionConfig: ScalaVersionConfig =
    ScalaVersionConfig(scalaVersion = scalaVersion, scalaJars = fetchScalaArtifacts().map(_.file))

}
