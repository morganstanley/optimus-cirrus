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
import optimus.buildtool.compilers.AsyncWebCompiler
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.WebCompilationSources
import optimus.platform._

import scala.collection.immutable
import scala.collection.immutable.Seq

@entity class WebResourcePackaging(
    override protected val scope: CompilationScope,
    override protected val sources: WebCompilationSources,
    compiler: AsyncWebCompiler
) extends PartialScopedCompilation {
  import scope._

  @node override protected def upstreamArtifacts: immutable.Seq[Artifact] = Nil
  @node override protected def containsRelevantSources: Boolean = scope.config.webConfig.isDefined && !sources.isEmpty

  @node def artifacts: Seq[Artifact] =
    compile(ArtifactType.Resources, None)(Some(compiler.artifact(id, compilerInputsN)))

  private val compilerInputsN = asNode(() => compilerInputs)

  @node private def compilerInputs: AsyncWebCompiler.Inputs = {
    val webConfig = scope.config.webConfig.getOrThrow(s"Web configuration is missing for scope $scope!")
    val nodeVersion = scope.webNodeDependency.map(_.version).getOrElse {
      throw new IllegalArgumentException(s"No central dependency found for $NpmGroup.$NpmName")
    }
    val pnpmVersion = scope.webPnpmDependency.map(_.version).getOrElse {
      throw new IllegalArgumentException(s"No central dependency found for $PnpmGroup.$PnpmName")
    }
    AsyncWebCompiler.Inputs(
      sources.compilationSources,
      pathBuilder.resourceOutPath(id, sources.compilationInputsHash),
      webConfig,
      nodeVersion,
      pnpmVersion,
      upstreamArtifacts
    )
  }
}
