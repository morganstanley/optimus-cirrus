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
import optimus.buildtool.compilers.GenericFilesPackager
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.GenericFilesCompilationSources
import optimus.platform._

import scala.collection.immutable.Seq

@entity private[scope] class GenericFilesScopedCompilation(
    override protected val scope: CompilationScope,
    override protected val sources: GenericFilesCompilationSources,
    genericFilesPackager: GenericFilesPackager
) extends PartialScopedCompilation {
  import scope._

  @node override protected def upstreamArtifacts: Seq[Artifact] = Nil
  @node override protected def containsRelevantSources: Boolean = !sources.isEmpty

  @node def files: Seq[Artifact] =
    compile(ArtifactType.GenericFiles, None)(genericFilesPackager.files(id, genericFilesPackagerInputsN))

  private val genericFilesPackagerInputsN = asNode(() => genericFilesPackagerInputs)

  @node private def genericFilesPackagerInputs = GenericFilesPackager.Inputs(
    pathBuilder
      .outputPathFor(id, sources.compilationInputsHash, ArtifactType.GenericFiles, None, incremental = false)
      .asJar,
    sources.compilationSources
  )

}
