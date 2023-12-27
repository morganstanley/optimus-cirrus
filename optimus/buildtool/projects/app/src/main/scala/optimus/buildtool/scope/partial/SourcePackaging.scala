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
import optimus.buildtool.compilers.JarPackager
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.SourceCompilationSources
import optimus.buildtool.trace.Sources
import optimus.platform._

import scala.collection.immutable.Seq

@entity private[scope] class SourcePackaging(
    override protected val scope: CompilationScope,
    override protected val sources: SourceCompilationSources,
    sourcePackager: JarPackager
) extends PartialScopedCompilation {
  import scope._

  // package sources if:
  // - we're in a sparse workspace and this scope is missing locally
  // - the scope is configured to install sources
  @node override protected def containsRelevantSources: Boolean =
    !sources.isEmpty && (!scope.scopeConfigSource.local(scope.id) || scope.config.flags.installSources)

  @node override protected def upstreamArtifacts: Seq[Artifact] = Seq()

  @node def packagedSources: Seq[Artifact] =
    compile(ArtifactType.Sources, None)(sourcePackager.artifact(id, sourcePackagerInputsN))

  private val sourcePackagerInputsN = asNode(() => sourcePackagerInputs)

  @node private def sourcePackagerInputs =
    JarPackager.Inputs(
      Sources,
      ArtifactType.Sources,
      pathBuilder
        .outputPathFor(id, sources.compilationInputsHash, ArtifactType.Sources, None, incremental = false)
        .asJar,
      sources.compilationSources,
      Map.empty
    )
}
