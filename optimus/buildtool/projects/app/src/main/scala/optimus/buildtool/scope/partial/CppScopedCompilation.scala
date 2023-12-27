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
import optimus.buildtool.compilers.AsyncCppCompiler
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.CppCompilationSources
import optimus.platform._

import scala.collection.immutable.Seq

@entity private[scope] class CppScopedCompilation(
    scope: CompilationScope,
    sources: Seq[CppCompilationSources],
    compiler: AsyncCppCompiler,
    private[scope] val cppFallback: Boolean
) {

  @node def artifacts: Seq[Artifact] = sources.apar.flatMap { osSources =>
    osCompilation(osSources).artifacts
  }

  @node private def osCompilation(osSources: CppCompilationSources): CppOsScopedCompilation =
    CppOsScopedCompilation(scope, osSources, compiler, cppFallback)

}

@entity private[scope] class CppOsScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: CppCompilationSources,
    compiler: AsyncCppCompiler,
    private[scope] val cppFallback: Boolean
) extends PartialScopedCompilation {
  import scope._

  @node override protected def upstreamArtifacts: Seq[Artifact] = upstream.cppForOurOsCompiler(sources.osVersion)
  @node override protected def containsRelevantSources: Boolean = !cppFallback && !sources.isEmpty

  @node def artifacts: Seq[Artifact] =
    compile(ArtifactType.Cpp, Some(sources.osVersion))(compiler.artifact(id, compilerInputsN))

  @node private def compilerInputsN = asNode(() => compilerInputs)

  @node private def compilerInputs =
    AsyncCppCompiler.Inputs(
      sources.compilationSources,
      pathBuilder
        .outputPathFor(
          id,
          sources.compilationInputsHash,
          ArtifactType.Cpp,
          Some(sources.osVersion),
          incremental = false
        )
        .asJar,
      scope.config.cppConfig(sources.osVersion),
      upstreamArtifacts
    )
}
