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

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.LoggingTaskTrace
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Outline
import optimus.buildtool.trace.Queue
import optimus.buildtool.trace.Scala
import optimus.buildtool.trace.Signatures
import optimus.buildtool.trace.TaskTrace
import optimus.buildtool.utils.Utils
import optimus.graph.CancellationScope
import optimus.graph.Node
import optimus.graph.NodePromise
import optimus.graph.NodeTask
import optimus.graph.PropertyNode
import optimus.graph.Scheduler
import optimus.platform._

import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

@entity private[buildtool] final class AsyncScalaCompiler(compilerFactory: SyncCompilerFactory)
    extends AsyncSignaturesCompiler {

  import AsyncScalaCompiler._

  @node override def fingerprint: Seq[String] = compilerFactory.fingerprint(Scala)

  @node override protected def output(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerOutput =
    // this will enqueue the outputNode to run on this thread, which is desirable since someone is waiting for the
    // CompilerOutput right now
    asyncGetAttached(outputAndSignatureNodes(scopeId, inputs)._1)

  @node override protected def signatureOutput(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs]
  ): SignatureCompilerOutput = {

    val (outputNode, sigsNode) = outputAndSignatureNodes(scopeId, inputs)
    if (!outputNode.isStarted) {
      // submitted to run asynchronously so that our caller doesn't get blocked until the full compilation is complete
      // (which is important because the caller may only want to wait for the signaturesNode, not the full classesNode).
      // n.b. we might end up scheduling it twice due to a race but that's ok - the scheduler will ignore the second one
      Scheduler.currentOrDefault.evaluateNodeAsync(outputNode)
    }
    // signatures node is from a NodePromise - it doesn't need to be enqueued since the outputNode will complete it
    asyncGetWithoutEnqueue(sigsNode)

  }

  // noinspection ScalaUnusedSymbol
  // Gets but does not start the output (i.e. real compilation) and signatures (i.e. promise) Nodes. This node
  // is placed in the ReallyBigCache so that multiple requests for the compilation always return the same instance.
  // Note that `cancellationScope` is an argument - one problem with returning `Node[T]` from this method rather
  // than `T` is that we still get cache hits for future builds with a new CancellationScope, even if `Node[T]`
  // ends up completing exceptionally. Passing `cancellationScope` each time is overly pessimistic in that we
  // won't get cache hits between builds even if the `Node`s complete normally, but in general we expect that
  // in that case we'll have had cache hits from the nodes that call this method (`output` and `signatureOutput`),
  // and so this method won't get called at all.
  @node private def outputAndSignatureNodes(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      cancellationScope: CancellationScope = EvaluationContext.cancelScope
  ): (Node[CompilerOutput], Node[SignatureCompilerOutput]) = {
    val queueTask: TaskTrace = ObtTrace.startTask(scopeId, Queue)
    val outVersions = outputVersions(inputs)
    val sigPromise = NodePromise[SignatureCompilerOutput]()
    // get but do not start the outputNode (so that we don't end up running it on this thread if we only wanted to get
    // signatures)
    val outputNode = nodeOf(_output(scopeId, inputs, outVersions, sigPromise, queueTask)).asInstanceOf[OutputNode]
    // usually sigPromise will get completed by the signatures callback, but if compilation fails before that, it needs
    // to get notified by the usual continueWith mechanism
    val completion = new Callback(() => completeSigs(scopeId, outputNode, sigPromise))
    outputNode.continueWithIfEverRuns(completion, EvaluationContext.current)
    (outputNode, sigPromise.node)
  }

  // noinspection ScalaUnusedSymbol - `outputVersions` is passed to ensure we get a cache miss when artifacts
  // generated by this compiler are deleted by rubbish tidying
  @node private def _output(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputVersions: Seq[Int],
      signaturePromise: SignaturePromise,
      queueTask: TaskTrace
  ): CompilerOutput = {
    val resolvedInputs = inputs()
    val config = resolvedInputs.scalacConfig
    val newConfig = config.copy(options = s"-DscopeId=$scopeId" +: config.options)
    val newInputs = resolvedInputs.copy(scalacConfig = newConfig)
    import newInputs._

    if (sourceFiles.isEmpty)
      CompilerOutput.empty(scopeId, ArtifactType.ScalaMessages, outputFile(ArtifactType.ScalaMessages).asJson)
    else {
      val outputNode = EvaluationContext.currentNode.asInstanceOf[OutputNode]
      val sourceSizeBytes = resolvedInputs.sourceFiles.values.map(_.size).sum
      compilerFactory.throttled(sourceSizeBytes)(asNode { () =>
        ObtTrace.throttleIfLowMem(scopeId) {

          // Queue timing should have already been started when the signatures node was originally requested. Note that if
          // classes were requested directly, not via signatures node, then this node is run by the calling thread so queue time
          // is and should be (nsignature) zero.
          queueTask.end(success = true)

          log.debug(
            s"[$scopeId:scala] Stopped signature output queueing (${dbgStr(outputNode, signaturePromise)})"
          )

          doCompilation(scopeId, newInputs, outputNode, signaturePromise)
        }
      })
    }
  }

  private def completeSigs(
      scopeId: ScopeId,
      outputNode: OutputNode,
      signaturePromise: SignaturePromise
  ): Unit = {
    // Usually the outputNode will have manually completed us already around half way through
    // its execution, so usually we are done before this callback fires and we need to do nothing at all.
    // If we're not done when the callback fires because the outputNode failed, fine, we fail too.
    // If we're not done and the outputNode didn't fail, it should only be because (a) there were no sources to compile
    // or (b) it has fatal compilation messages; in case (b) we should propagate them so that people expecting to find
    // signatures will find those messages instead.
    if (!signaturePromise.node.isDone) {
      val more =
        if (outputNode.isDoneWithException) {
          signaturePromise.completeWithChild(Failure(outputNode.exception()), outputNode)
          s"exception=${outputNode.exception()}"
        } else if (outputNode.isDoneWithResult) {
          val scalaOutput: CompilerOutput = outputNode.result
          val sigMessageFile =
            Utils.outputPathForType(scalaOutput.messages.messageFile, ArtifactType.SignatureMessages).asJson

          if (scalaOutput.isEmpty) {
            val sigOutput = SignatureCompilerOutput.empty(scopeId, sigMessageFile)
            signaturePromise.completeWithChild(Success(sigOutput), outputNode)
          } else if (scalaOutput.messages.hasErrors) {
            // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput`, which
            // are the leaf nodes that call this code
            val signatureMessages = CompilerMessagesArtifact.unwatched(
              InternalArtifactId(scalaOutput.messages.id.scopeId, ArtifactType.SignatureMessages, None),
              sigMessageFile,
              scalaOutput.messages.messages,
              Signatures,
              scalaOutput.messages.incremental
            )
            signatureMessages.storeJson()

            val signatureOutput = SignatureCompilerOutput(None, signatureMessages, None)
            Utils.FailureLog.error(s"[$scopeId:scala] Completing signature compilation with errors")
            log.debug(s"[$scopeId:scala] Signature errors: $signatureOutput")
            signaturePromise.completeWithChild(Success(signatureOutput), outputNode)
            s"errors=${scalaOutput.messages.messages.count(_.severity == CompilationMessage.Error)}"
          } else {
            log.debug(
              s"[$scopeId:scala] Unexpected callback for signature output node: $scalaOutput. Will await signature callback (if this isn't received, compilation will stall)."
            )
            "unexpected callback"
          }
        } else {
          "awaiting completion"
        }
      log.debug(
        s"[$scopeId:scala] signaturesNode.onChildCompleted callback (${dbgStr(outputNode, signaturePromise)}): $more"
      )
    }
  }

  // Rather than tracking deletions via invalidation, we instead track deletions by explicitly passing the
  // version of the output artifacts (which is incremented whenever an artifact is deleted). This
  // ensures that _output is rerun if the signature artifacts are deleted, and
  // also allows us to create untracked artifacts within the compiler code itself. However, it does mean
  // we need to ensure any new artifact types created and returned by output or signatureOutput need to be
  // included in outputTypes here.
  @node private def outputVersions(inputs: NodeFunction0[Inputs]): Seq[Int] = {
    import optimus.buildtool.artifacts.{ArtifactType => AT}

    val resolvedInputs = inputs()
    val outputTypes =
      Seq(
        AT.JavaAndScalaSignatures,
        AT.SignatureMessages,
        AT.SignatureAnalysis,
        AT.Scala,
        AT.ScalaMessages,
        AT.ScalaAnalysis)
    outputTypes.apar.map(t => PathedArtifact.version(resolvedInputs.outputFile(t)))
  }

  private def doCompilation(
      scopeId: ScopeId,
      inputs: Inputs,
      outputNode: OutputNode,
      signaturePromise: SignaturePromise
  ): CompilerOutput = {

    val compiler = compilerFactory.newCompiler(scopeId, Scala)

    var scalacTrace: TaskTrace = null
    var signaturesTrace: TaskTrace = null

    val cancelScope = EvaluationContext.cancelScope
    val listener = { _: CancellationScope =>
      compiler.cancel()
    }
    cancelScope.addListener(listener)

    val signatureType = if (inputs.outlineTypesOnly) Outline else Signatures
    try {
      val signatureOutputConsumer: Option[Try[SignatureCompilerOutput] => Unit] =
        if (inputs.signatureOutPath.isDefined) {
          // we record the scalac time until signatures are ready as Signatures/Outline
          signaturesTrace = ObtTrace.startTask(scopeId, signatureType)

          // This callback will be triggered when SignatureArtifacts are available. If the compilation fails before
          // they are available we'll get completed by the generateClasses node instead (probably with error
          // CompilationMessages or a throwable of some kind) - see ClassNodeToSignatureNode
          Some { result: Try[SignatureCompilerOutput] =>
            // once signatures are available, we record the remaining scalac time as Scalac
            val endTime = patch.MilliInstant.now()
            if (signaturesTrace ne null) {
              result.failed.foreach(t => signaturesTrace.publishMessages(Seq(CompilationMessage.error(t))))
              signaturesTrace.end(result.isSuccess, time = endTime)
              signaturesTrace = null

              if (!inputs.outlineTypesOnly) scalacTrace = ObtTrace.startTask(scopeId, Scala, endTime)
              val id = scopeId
              log.debug(s"[$id:scala] signatureOutputConsumer callback (${dbgStr(outputNode, signaturePromise)})")

              result match {
                case Success(signatureOutput) =>
                  log.info(s"[$id:scala] Completing signature compilation")
                  log.debug(s"[$id:scala] Signature output: $signatureOutput")
                case Failure(_) =>
                // do nothing
              }
              signaturePromise.completeWithChild(result, outputNode)
            } else {
              log.warn(
                "Signatures callback was called twice (perhaps Zinc decided to retry compilation due to errors?). " +
                  "The first jar of signatures has already been sent downstream and this jar will be ignored")
            }
          }
        } else {
          // if pipelined signatures are disabled, we record the entire scalac time as Scalac
          scalacTrace = ObtTrace.startTask(scopeId, Scala)
          signaturePromise.completeWithChild(
            Success(SignatureCompilerOutput.empty(scopeId, inputs.outputFile(ArtifactType.SignatureMessages).asJson)),
            outputNode
          )
          None
        }

      // (note that there's no concurrency here, so no worries about the trace getting nulled out during this method)
      def activeTask: Task =
        if (signaturesTrace ne null) Task(signaturesTrace, ArtifactType.SignatureMessages, signatureType)
        else if (scalacTrace ne null) Task(scalacTrace, ArtifactType.ScalaMessages, Scala)
        else Task(LoggingTaskTrace, ArtifactType.ScalaMessages, Scala)

      // actually run the compiler
      val output = compiler.compile(inputs, signatureOutputConsumer, activeTask)
      if (signaturesTrace ne null) {
        signaturesTrace.end(success = !output.messages.hasErrors, output.messages.errors, output.messages.warnings)
        signaturesTrace = null
      }
      if (scalacTrace ne null) {
        scalacTrace.end(success = !output.messages.hasErrors, output.messages.errors, output.messages.warnings)
        scalacTrace = null
      }
      output
    } catch {
      case NonFatal(e) =>
        if (scalacTrace ne null) scalacTrace.publishMessages(Seq(CompilationMessage.error(e)))
        if (signaturesTrace ne null) signaturesTrace.publishMessages(Seq(CompilationMessage.error(e)))
        throw e
    } finally {
      EvaluationContext.cancelScope.removeListener(listener)
      // if we got here without nulling out the trace, it must have failed
      if (scalacTrace ne null) scalacTrace.end(success = false)
      if (signaturesTrace ne null) signaturesTrace.end(success = false)
    }
  }

}

object AsyncScalaCompiler {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // This is the node through which scala compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  outputAndSignatureNodes.setCustomCache(reallyBigCache)

  type OutputNode = PropertyNode[CompilerOutput]
  type SignaturePromise = NodePromise[SignatureCompilerOutput]

  private final class Callback(f: () => Unit) extends Node[Unit] {
    initAsRunning(ScenarioStack.constant)
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = f()
  }

  private def dbgStr(node: NodeTask): String = s"$node@${System.identityHashCode(node)}"

  private def dbgStr(outputNode: NodeTask, sigPromise: SignaturePromise): String =
    s"${dbgStr(outputNode)} -> ${dbgStr(sigPromise.node)}"
}
