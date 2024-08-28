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
package optimus.graph
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.NodeResult
import optimus.platform.annotations.deprecating
import optimus.platform.EvaluationQueue

import java.util.Objects
import scala.util.control.NonFatal

/**
 * ********************************************************************************
 *
 * TODO (OPTIMUS-0000): review the style of these nodes, whether they play nicely with the rest of tracing, graphing, tracking code
 *
 * ********************************************************************************
 */

/**
 * Exceptions extending this trait are not intercepted by nodeResult or asyncResult
 */
private[optimus] trait InvisibleToNodeResult

/**
 * Doesn't run on its own, rather takes the entire child node as the result
 */
private[optimus] abstract class NodeResultNodeImpl[T, W <: NodeResult[T]](
    val childNode: Node[T],
    usingScope: CancellationScope)
    extends CompletableNodeM[W] {

  private[this] var pcs: CancellationScope = _
  private[this] var cs: CancellationScope = _

  override def executionInfo: NodeTaskInfo = NodeTaskInfo.NodeResult
  override def run(ec: OGSchedulerContext): Unit = {
    if (usingScope eq null) {
      pcs = scenarioStack.cancelScope
      cs = pcs.childScope()
    } else
      cs = usingScope
    childNode.attach(scenarioStack.withCancellationScope(cs))
    if (NodeTrace.profileSSUsage.getValue) Profiler.t_sstack_usage(childNode.scenarioStack(), childNode, hit = true)
    ec.enqueue(childNode)
    childNode.continueWith(this, ec)
  }

  protected def resultFromNode(node: Node[T]): W

  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    // since the NodeResultNodeImpl is the computation of a "asyncResult { X }" block it needs to take dependency on any
    // tracking info from the computation of X, but not any Exception or warnings (since these are treated
    // as part of our value rather than thrown up). This means that the node which ran the asyncResult { X } block will
    // correctly take dependency on { X } but not the Exception or warnings.
    combineInfo(child, eq)

    val e = child.exception()

    if (Objects.nonNull(e) && (e.isInstanceOf[InvisibleToNodeResult] || !NonFatal.apply(e))) {
      completeWithException(child.exception(), eq)
    } else {
      _xinfo = _xinfo.withoutWarningsOrException
      // The *result* of a NodeResultNodeImpl is a NodeResult which is a *value* just like a Try. Using that value should
      // *not* cause any propagation of dependency tracking information, because Optimus only tracks dependency based on
      // node calls, not based on tracking the results of those node calls. For example, in "@node def f = asyncResult { X }",
      // f depends on the dependencies X, but in "f(asyncResult { X })", f only depends on the value of X, not on the
      // dependencies of X.
      completeWithResult(resultFromNode(childNode), eq)
    }
  }
}
private[optimus] class NodeResultNode[T](cn: Node[T], cs: CancellationScope)
    extends NodeResultNodeImpl[T, NodeResult[T]](cn, cs) {

  def this(cn: Node[T]) = this(cn, null)
  override protected def resultFromNode(node: Node[T]): NodeResult[T] = new NodeResult(node)
}

private[optimus] final class AsyncUsingNode[R <: AutoCloseable, B](resourceProducer: () => R, consumer: R => Node[B])
    extends CompletableNode[B] {

  override def executionInfo(): NodeTaskInfo = NodeTaskInfo.AsyncUsing

  private var childNode: Node[B] = _
  private var resource: R = _

  private def safeClose(): Unit = synchronized {
    try { if (resource != null) resource.close() }
    finally { resource = null.asInstanceOf[R] }
  }

  private def newClosingAwaiter: NodeAwaiter = {

    /**
     * The awaiter is not in the cancellation scope for this node, and so it will always get a callback on
     * cancellations. There are plans to change this behaviour eventually, but there is a test that will fail in
     * that case.
     *
     * There is a potential race if there are currently executing users of the resource and we get the callback before
     * they have finished executing. With the current state of cancellation, that isn't a problem because we won't get a
     * callback until all of our children have been cancelled. If this invariant changes, this code will have to change
     * too.
     *
     * Also, this is an onChildCompleted callback, we'll get a graph panic if the resource close() call throws an
     * exception, but only if we are cancelled. The non-cancelled path through run() will catch the exception correctly.
     * We could attempt to capture that exception by transforming this node to a tryComplete one, but it doesn't seem
     * worth it.
     */
    (_: EvaluationQueue, _: NodeTask) => safeClose()

  }

  private var state = 0
  override def run(ec: OGSchedulerContext): Unit = {
    state match {
      case 0 =>
        // create the resource, now we have to make sure we'll close it
        resource = resourceProducer()
        if (!tryAddToWaiterList(newClosingAwaiter)) {
          safeClose()
          throw new GraphInInvalidState("this node is both running and completed, which makes no sense")
        }

        childNode = consumer(resource)
        childNode.attach(scenarioStack())
        ec.enqueue(this, childNode)
        state = 1
        childNode.continueWith(this, ec)

      case 1 =>
        val cn = childNode
        childNode = null
        state = 2

        try { safeClose() }
        catch {
          case t: Throwable =>
            // makes sure we get the info even if close() fails
            combineInfo(cn, ec)
            if (cn.isDoneWithException) t.addSuppressed(cn.exception())
            throw t
        }

        completeFromNode(cn, ec) // <- usual combineInfo happens in there

      case wrong => throw new GraphInInvalidState(s"Incorrect asyncUsing node state (${wrong})")
    }
  }

}

object UnsafeInternal extends Node[Object] {
  @deprecating("Warning: this method is for internal use only and may be removed at any time!")
  def clone[R](node: Node[R]): Node[R] = node.cloneTask().asInstanceOf[Node[R]]
}
