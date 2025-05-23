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
package optimus.buildtool.resolvers

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.ExternalArtifactId
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.files.JarAsset
import optimus.buildtool.utils.Hashing
import optimus.platform._

import scala.collection.immutable.{IndexedSeq, Seq}

@entity trait ExternalDependencyResolverSource {
  @node def resolver(moduleSet: ModuleSet): ExternalDependencyResolver
}

@entity abstract class ExternalDependencyResolver(val loadedDefinitions: Seq[DependencyDefinition]) {
  val (extraLibsDefinitions, dependencyDefinitions) = loadedDefinitions.partition(_.isExtraLib)

  @node def fingerprintDependencies(deps: DependencyDefinitions): Seq[String]
  @node def resolveDependencies(deps: DependencyDefinitions): ResolutionResult
}

@entity object ExternalDependencyResolver {
  // (mainly an @node so that the hashing is cached)
  @node final def makeArtifact(
      id: ExternalArtifactId,
      jar: JarAsset,
      source: Option[JarAsset],
      javadoc: Option[JarAsset],
      containsPlugin: Boolean = false,
      containsAgent: Boolean = false,
      containsOrUsedByMacros: Boolean = false,
      isMaven: Boolean = false
  ): ExternalClassFileArtifact = {
    ExternalClassFileArtifact.create(
      id,
      jar,
      source,
      javadoc,
      // we capture the assumption of immutability here so that it's preserved if we later depcopy (etc.)
      assumedImmutable = Hashing.isAssumedImmutable(jar),
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros,
      isMaven = isMaven
    )
  }
}

/**
 * A ResolutionResult serves as a return type in the "CoursierArtifactResolver.scala":getArtifacts method. This class is
 * intended to wrap the artifacts and dependencies information inside to support related functions such as cache
 * mechanism in `dependency-visualiser`'s graph class. Parameters are solved from Coursier Api "Resolution.scala" and
 * "CoursierArtifactResolver.scala".
 *
 * @param resolvedArtifactsToDepInfos All external jar artifacts and the dependency/dependencies from which they were resolved
 * @param messages Any messages associated with resolution
 * @param jniPaths Paths to external JNI artifacts
 * @param moduleLoads External modules to be loaded
 * @param finalDependencies All resolved dependencies and their children
 * @param mappedDependencies All resolved dependencies mapped from AFS to maven coordinates
 */
final case class ResolutionResult(
    resolvedArtifactsToDepInfos: Seq[(ExternalClassFileArtifact, Seq[DependencyInfo])],
    messages: Seq[CompilationMessage],
    jniPaths: Seq[String],
    moduleLoads: Seq[String],
    finalDependencies: Map[DependencyInfo, Seq[DependencyInfo]],
    mappedDependencies: Map[DependencyInfo, Seq[DependencyInfo]]
) {
  lazy val resolvedArtifacts: IndexedSeq[ExternalClassFileArtifact] = resolvedArtifactsToDepInfos.map(_._1).toVector
  def dependencies: Set[DependencyInfo] = resolvedArtifactsToDepInfos.flatMap(_._2).toSet
}

/**
 * A DependencyInfo serves as a container and used for ResolutionResult. This will save dependencies information in
 * String and finally be used for `dependency-visualiser`'s graph to generate its nodes for "OBTVisualizer.scala"
 */
@embeddable final case class DependencyInfo @node() (
    group: String,
    name: String,
    config: String,
    version: String,
    isMaven: Boolean) {
  def module: String = s"$group:$name"
  def dotModule: String = s"$group.$name"
  def noVersionName: String = s"$module:$config"
  override def toString: String = s"$module:$config.$version"
}
