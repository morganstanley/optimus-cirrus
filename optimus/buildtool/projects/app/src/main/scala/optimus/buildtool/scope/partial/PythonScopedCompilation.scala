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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.compilers.AsyncPythonCompiler
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.PythonCompilationSources
import optimus.platform._

import scala.collection.immutable.{IndexedSeq, Seq}

@entity class PythonScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: PythonCompilationSources,
    compiler: AsyncPythonCompiler)
    extends PartialScopedCompilation {
  import scope._

  @node override protected def upstreamArtifacts: IndexedSeq[Artifact] = IndexedSeq()
  @node override protected def containsRelevantSources: Boolean =
    scope.config.pythonConfig.isDefined && !sources.isEmpty

  private val compilerInputsN = asNode(() => compilerInputs)

  @node private def compilerInputs: AsyncPythonCompiler.Inputs = {
    val pyConfig = scope.config.pythonConfig.getOrThrow(s"python configuration is missing for scope $scope!")

    AsyncPythonCompiler.Inputs(
      sources.compilationSources,
      pathBuilder
        .outputPathFor(
          id,
          sources.compilationFingerprint.hash,
          ArtifactType.Python,
          discriminator = None
        ),
      pyConfig,
      sources.compilationFingerprint.hash
    )
  }

  @node def artifacts: Seq[Artifact] = compile(ArtifactType.Python, None)(compiler.artifact(id, compilerInputsN))
}
