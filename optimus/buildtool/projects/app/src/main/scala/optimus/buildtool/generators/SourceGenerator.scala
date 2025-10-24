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
package optimus.buildtool.generators

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.sandboxed.SandboxedFiles
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.scope.CompilationScope
import optimus.platform._

import scala.collection.immutable.IndexedSeq

/**
 * A generator to create source files (typically scala or java sources) based on input templates and configuration.
 */
@entity trait SourceGenerator {
  val artifactType: GeneratedSourceArtifactType = ArtifactType.GeneratedSource
  protected def generatorType: String
  def tpe: GeneratorType = GeneratorType(generatorType)

  type Inputs <: SourceGenerator.Inputs

  @node def dependencies(scope: CompilationScope): IndexedSeq[Artifact] = Vector()

  def templateType(configuration: Map[String, String]): PathFilter

  // Returned as a NodeFunction0 to prevent Inputs (which will often contain full sources) being inadvertently
  // cached as an argument to other @nodes. SandboxedTemplates is a NodeFunction0 here for the same reason.
  final def inputs(
      templates: NodeFunction0[SandboxedInputs],
      configuration: Map[String, String],
      scope: CompilationScope
  ): NodeFunction0[Inputs] =
    asNode(() => cachedInputs(templates, configuration, scope))

  @node private def cachedInputs(
      templates: NodeFunction0[SandboxedInputs],
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = _inputs(templates(), configuration, scope)

  /**
   * Create (and fingerprint) the inputs needed for source generation.
   *
   * Note that anything which could affect the output of the generator (file contents, path to external script, etc.)
   * must be captured as part of the fingerprint to ensure that OBT knows to regenerate the sources when it has
   * changed. This method is async so that we don't cache the templates (which may include a large amount of hashed
   * content) as part of a node cache key. The method itself, however, is cached by virtue of being
   * called from [[cachedInputs]].
   */
  @async protected def _inputs(
      templates: SandboxedInputs,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs

  /** True if the generator needs to run at all for a given set of inputs */
  @node def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean = !inputs().files.isEmpty

  @node final def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact] = _generateSource(scopeId, inputs(), writer)

  // Note: This is async so that we don't cache the inputs (which may include a large amount of hashed content) as
  // part of a node cache key
  /**
   * Generate sources for a given set of inputs.
   *
   * No reference to files or configuration that is not captured as part of the fingerprint should be made here,
   * to avoid the possibility of OBT incorrectly returning stale generated sources when something outside the
   * fingerprint changes. This method is async so that we don't cache the inputs (which may include a large amount
   * of hashed content) as part of a node cache key. The method itself, however, is cached by virtue of being
   * called from [[generateSource]].
   */
  @async def _generateSource(
      scopeId: ScopeId,
      inputs: Inputs,
      writer: ArtifactWriter
  ): Option[GeneratedSourceArtifact]

}

object SourceGenerator {
  private[buildtool] val SourcePath = RelativePath("src")

  trait Inputs {
    def files: SandboxedFiles
    def fingerprint: FingerprintArtifact
    def generatorId: String = files.generatorId
  }

  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since _inputs holds the template files and the hash, it's important that it's frozen for the duration of
  // a compilation (so that we're sure what we hashed is what we used for generation)
  cachedInputs.setCustomCache(reallyBigCache)

  // we want to guard against generating sources more than once
  generateSource.setCustomCache(reallyBigCache)
}
