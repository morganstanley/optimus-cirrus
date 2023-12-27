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

import optimus.buildtool.artifacts._
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.ObtTrace
import optimus.graph.CancellationScope
import optimus.platform._

import scala.collection.immutable.Seq
import scala.util.control.NonFatal

@entity private[buildtool] class AsyncJavaCompiler(compilerFactory: SyncCompilerFactory)
    extends AsyncClassFileCompiler {

  @node override def fingerprint: Seq[String] = compilerFactory.fingerprint(Java)

  @node override protected def output(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerOutput = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    if (sourceFiles.isEmpty)
      CompilerOutput.empty(scopeId, ArtifactType.JavaMessages, outputFile(ArtifactType.JavaMessages).asJson)
    else {
      val sourceSizeBytes = sourceFiles.values.map(_.size).sum
      compilerFactory.throttled(sourceSizeBytes)(asNode { () =>
        doCompilation(scopeId, resolvedInputs).watchForDeletion()
      })
    }
  }

  private def doCompilation(scopeId: ScopeId, inputs: Inputs): CompilerOutput = {
    val compiler = compilerFactory.newCompiler(scopeId, Java)

    val cancelScope = EvaluationContext.cancelScope
    val listener = { _: CancellationScope =>
      compiler.cancel()
    }
    cancelScope.addListener(listener)

    val trace = ObtTrace.startTask(scopeId, Java)
    val task = Task(trace, ArtifactType.JavaMessages, Java)
    try {
      // actually run the compiler
      val output = compiler.compile(inputs, None, task)
      trace.end(success = !output.messages.hasErrors, output.messages.errors, output.messages.warnings)
      output
    } catch {
      case NonFatal(e) =>
        trace.publishMessages(Seq(CompilationMessage.error(e)))
        trace.end(success = false)
        throw e
    } finally {
      EvaluationContext.cancelScope.removeListener(listener)
    }
  }
}

object AsyncJavaCompiler {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // This is the node through which scala compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  output.setCustomCache(reallyBigCache)
}
