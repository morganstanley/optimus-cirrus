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
import optimus.buildtool.compilers.RegexScanner
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.RegexMessagesCompilationSources
import optimus.platform._

import scala.collection.immutable.Seq

@entity
class RegexMessagesScopedCompilation(
    val scope: CompilationScope,
    val sources: RegexMessagesCompilationSources,
    regexScanner: RegexScanner
) extends PartialScopedCompilation {

  @node override protected def upstreamArtifacts: Seq[Artifact] = Nil
  @node override protected def containsRelevantSources: Boolean =
    sources.nonEmpty && scope.config.regexConfig.exists(_.rules.nonEmpty)

  @node
  def messages: Seq[Artifact] = compile(ArtifactType.RegexMessages, None) {
    Some(regexScanner.messages(scope.id, inputsN))
  }

  private val inputsN = asNode(() => inputs)

  @node private def inputs = {
    val jsonFile = scope.pathBuilder
      .outputPathFor(scope.id, fingerprintHash, ArtifactType.RegexMessages, None, incremental = false)
      .asJson
    RegexScanner.Inputs(sources.compilationSources, rules, jsonFile)
  }

  private val rules = scope.config.regexConfig.map(_.rules).getOrElse(Nil)

}
