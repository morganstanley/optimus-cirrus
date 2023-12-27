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
import optimus.buildtool.compilers.AsyncRunConfCompiler
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.RunconfCompilationSources
import optimus.platform._

import scala.collection.immutable.Seq

// Must be in the short-circuit signature in ScopedCompilation as we need to make sure dependencies are resolved
@entity private[scope] class RunconfAppScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: RunconfCompilationSources,
    runconfc: AsyncRunConfCompiler
) extends PartialScopedCompilation {
  import scope._

  @node def messages: Seq[Artifact] = compile(AT.CompiledRunconfMessages, None)(Some(runconfc.messages(id, inputsN)))
  @node def runConfArtifacts: Seq[Artifact] = compile(AT.CompiledRunconf, None)(runconfc.runConfArtifact(id, inputsN))
  @node def runConfigurations: Seq[RunConf] = runconfc.runConfigurations(id, inputsN)

  @node override protected def upstreamArtifacts: Seq[Artifact] =
    Seq.empty // Unless we want to find the mainClass, we depend on nothing
  @node override protected def containsRelevantSources: Boolean = sources.containsRunconf

  private val inputsN = asNode(() => inputs)
  @node private def inputs: AsyncRunConfCompiler.Inputs = {
    AsyncRunConfCompiler.Inputs(
      workspaceSourceRoot = scope.config.paths.workspaceSourceRoot,
      absScopeConfigDir = scope.config.absScopeConfigDir,
      localArtifacts = sources.compilationSources,
      upstreamInputs = sources.upstreamInputs,
      sourceSubstitutions = sources.allSourceSubstitutions,
      blockedSubstitutions = sources.allBlockedSubstitutions,
      installVersion = sources.installVersion,
      outputJar = pathBuilder
        .outputPathFor(id, sources.compilationInputsHash, AT.CompiledRunconf, None, incremental = false)
        .asJar
    )
  }
}
