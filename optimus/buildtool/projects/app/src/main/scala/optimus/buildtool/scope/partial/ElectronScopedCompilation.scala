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
import optimus.buildtool.compilers.AsyncElectronCompiler
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.ElectronCompilationSources
import optimus.platform._

import scala.collection.immutable
import scala.collection.immutable.Seq

@entity private[scope] class ElectronScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: ElectronCompilationSources,
    compiler: AsyncElectronCompiler
) extends PartialScopedCompilation {
  import scope._

  @node override protected def upstreamArtifacts: immutable.Seq[Artifact] = Nil
  @node override protected def containsRelevantSources: Boolean =
    scope.config.electronConfig.isDefined && !sources.isEmpty

  @node def artifacts: Seq[Artifact] =
    compile(ArtifactType.Electron, None)(Some(compiler.artifact(id, compilerInputsN)))

  private val compilerInputsN = asNode(() => compilerInputs)

  @node private def compilerInputs: AsyncElectronCompiler.Inputs = {
    val electronConfig = scope.config.electronConfig.getOrThrow(s"Electron configuration is missing for scope $scope!")
    val nodeVersion = scope.electronNodeDependency.map(_.version).getOrElse {
      throw new IllegalArgumentException(s"No central dependency found for $NpmGroup.$NpmName")
    }
    val pnpmVersion = scope.electronPnpmDependency.map(_.version).getOrElse {
      throw new IllegalArgumentException(s"No central dependency found for $PnpmGroup.$PnpmName")
    }
    AsyncElectronCompiler.Inputs(
      sources.compilationSources,
      pathBuilder.electronOutPath(id, sources.compilationInputsHash),
      electronConfig,
      nodeVersion,
      pnpmVersion,
      upstreamArtifacts
    )
  }
}
