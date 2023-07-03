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
package optimus.platform.relational.asm

import java.util.function.Function
import optimus.graph.CompletableNode
import optimus.graph.FlatMapNode
import optimus.graph.Node
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.platform.EvaluationQueue

object Continuation {
  def andThen[T, R](node: Node[T], f: Function[T, Node[R]]): Node[R] = {
    new FlatMapNode(node, (n: T) => f(n), false).enqueue
  }

  def whenAll[T](nodes: Array[Node[_]], f: Function[Array[Any], Node[T]]): Node[T] =
    andThen(new WhenAll(nodes).enqueue, f)

  def wrap[T](f: () => Node[T]): Node[T] = {
    new WrapperNode[T](f)
  }

  private class WrapperNode[T](f: () => Node[T]) extends CompletableNode[T] {
    override def run(ec: OGSchedulerContext): Unit = {
      f().continueWith(this, ec)
    }

    override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
      completeFromNode(node.asInstanceOf[Node[T]], eq)
    }
  }

  // the node in nodes are already running, we do not need to enqueue them
  //
  // note that we could reuse SequenceNode, but we do not, for performance reasons.
  private class WhenAll(nodes: Array[Node[_]]) extends CompletableNode[Array[Any]] {
    private var current = 0
    private val output = Array.ofDim[Any](nodes.length)

    override def run(ec: OGSchedulerContext): Unit = {
      nodes(current).continueWith(this, ec)
    }

    // Note that this calls continueWith in sequence on each node. This ensures that exceptions are raised not in the
    // order they happen in (which is not RT) but in the order of the nodes array. Doing it this way has a potential
    // performance cost (but only when an exception is thrown), as it means we cannot complete due to an exception in
    // nodes(i) until all nodes(0:i) are completed.
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfo(child, eq)
      if (child.isDoneWithException) {
        completeWithException(child.exception(), eq)
      } else {
        output(current) = child.resultObject
        current += 1
        if (current < nodes.length) {
          // This has the potential to blow up the stack if nodes is large-ish, because we are calling ourselves here by
          // recursion when child is completed already. If this is ever a problem, we should fix it by trampolining to
          // re-enqueue ourself.
          nodes(current).continueWith(this, eq)
        } else completeWithResult(output, eq)
      }
    }
  }
}
