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
package optimus.buildtool.scope.partial

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.AsyncJmhCompiler
import optimus.buildtool.format.WarningsConfiguration
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.platform._

import scala.collection.immutable.Seq

@entity private[scope] class JmhScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: JavaAndScalaCompilationSources,
    jmhc: AsyncJmhCompiler,
    scala: ScalaScopedCompilation,
    java: JavaScopedCompilation
) extends PartialScopedClassCompilation {
  import scope._

  @node def messages: Seq[Artifact] = compile(AT.JmhMessages, None)(Some(jmhc.messages(id, inputsN)))
  @node def classes: Seq[Artifact] = compile(AT.Jmh, None)(jmhc.classes(id, inputsN))

  @node override protected def upstreamArtifacts: Seq[Artifact] = scope.upstream.classesForOurCompiler
  @node override protected def containsRelevantSources: Boolean = scope.config.jmh

  private val inputsN = asNode(() => inputs)
  @node private def inputs: AsyncJmhCompiler.Inputs = {
    val externalArtifacts =
      upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies.result.resolvedArtifacts)
    val otherArtifacts =
      upstreamArtifacts ++ externalArtifacts
    val jmhJars = {
      import optimus.buildtool.config._
      // using _ as a dummy version to get overridden by central dependency definition
      val jmhDep = DependencyDefinition("ossjava", "jmh", "_", LocalDefinition, configuration = "bytecode")
      val r = externalDependencyResolver.resolveDependencies(DependencyDefinitions(Nil, jmhDep :: Nil))
      assert(r.messages isEmpty, r.messages)
      r.resolvedArtifacts
    }
    AsyncJmhCompiler.Inputs(
      localArtifacts = scala.classes ++ java.classes,
      otherArtifacts = JavaScopedCompilation.updateNetworkPaths(dependencyCopier, otherArtifacts),
      jmhJars = jmhJars,
      outputJar = pathBuilder.outputPathFor(id, sources.compilationInputsHash, AT.Jmh, None, incremental = false).asJar,
      // no current use case for passing through full options/warnings, and it's not clear that it would be correct
      scope.config.javacConfig.copy(options = Nil, warnings = WarningsConfiguration.empty)
    )
  }
}
