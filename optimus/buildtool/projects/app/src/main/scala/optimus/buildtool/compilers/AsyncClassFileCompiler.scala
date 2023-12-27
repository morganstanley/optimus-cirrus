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
package optimus.buildtool.compilers

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.TaskTrace
import optimus.platform._

/**
 * Allows invocation of compilation asynchronously. Delegates the actual compilation work to a SyncCompiler instance.
 *
 * Don't put any logic in here that is not related to making the asynchronicity work. For example any args setup or file
 * moving/munging logic should live in SyncCompiler or one of its subclasss or callees, not here.
 *
 * Note: All of the @node methods take NodeFunction0[Inputs] arguments rather than Inputs; this is because Inputs
 * contains all the source code of the scope, and would make for a very large key in the node cache.
 */
@entity private[buildtool] trait AsyncClassFileCompiler extends LanguageCompiler {
  @node final def classes(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[InternalClassFileArtifact] =
    output(scopeId, inputs).classes

  @node final def messages(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerMessagesArtifact =
    output(scopeId, inputs).messages

  @node final def analysis(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[AnalysisArtifact] =
    output(scopeId, inputs).analysis

  @node protected def output(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerOutput
}

/**
 * Allows invocation of Scala compilation to retrieve signatures asynchronously (i.e. without blocking the calling
 * thread), while full compilation proceeds in the background thus allowing pipelined compilation (whereby downstream
 * scope can start compiling as soon as signatures are ready).
 *
 * Note: All of the @node methods take NodeFunction0[Inputs] arguments rather than Inputs; this is because Inputs
 * contains all the source code of the scope, and would make for a very large key in the node cache.
 */
@entity private[buildtool] trait AsyncSignaturesCompiler extends AsyncClassFileCompiler {
  @node final def signatures(scopeId: ScopeId, input: NodeFunction0[Inputs]): Option[SignatureArtifact] =
    signatureOutput(scopeId, input).signatures

  @node def signatureMessages(scopeId: ScopeId, input: NodeFunction0[Inputs]): CompilerMessagesArtifact =
    signatureOutput(scopeId, input).messages

  @node final def signatureAnalysis(scopeId: ScopeId, input: NodeFunction0[Inputs]): Option[AnalysisArtifact] =
    signatureOutput(scopeId, input).analysis

  @node protected def signatureOutput(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): SignatureCompilerOutput
}

final case class Task(trace: TaskTrace, artifactType: MessageArtifactType, category: MessageTrace)
