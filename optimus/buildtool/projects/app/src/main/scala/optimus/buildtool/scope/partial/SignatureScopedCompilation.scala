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
import optimus.buildtool.compilers.AsyncSignaturesCompiler
import optimus.buildtool.scope.CompilationScope
import optimus.platform._
import scala.collection.immutable.Seq

@entity private[scope] class SignatureScopedCompilation(
    override protected val scope: CompilationScope,
    scalac: AsyncSignaturesCompiler,
    override protected val compilationInputs: ScalaCompilationInputs
) extends ScalaScopedCompilationBase {
  import scope._

  @node override protected def containsRelevantSources: Boolean = !sources.isEmpty

  @node def messages: Seq[Artifact] =
    compile(AT.SignatureMessages, None)(Some(scalac.signatureMessages(id, scalacInputsN)))

  @node def javaAndScalaSignatures: Seq[Artifact] =
    compile(AT.JavaAndScalaSignatures, None)(scalac.signatures(id, scalacInputsN))

  @node def analysis: Seq[Artifact] = compile(AT.SignatureAnalysis, None)(scalac.signatureAnalysis(id, scalacInputsN))

}
