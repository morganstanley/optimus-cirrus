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

import optimus.platform.EvaluationQueue

/**
 * A node scheduler plugin which replaces calls to the node with calls to a different node.
 *
 * A is the "foo$node" trait of the node "foo" you are plugging in to
 * (created with @async(exposeArgTypes = true)/@node(exposeArgTypes = true)), and B is the
 * return type of that node (and consequently also the return type of the replacement node)
 */
abstract class NodeRedirectingPlugin[A, B] extends SchedulerPlugin {
  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val replacement = replacementNode(n.asInstanceOf[A])

    n.setWaitingOn(replacement) // Keep causality walk happy
    replacement.continueWith(
      (eq: EvaluationQueue, node: NodeTask) => {
        n.setWaitingOn(null) // Clear causality
        n.asInstanceOf[CompletableNode[B]].completeFromNode(replacement, eq)
      },
      ec)

    true
  }

  /**
   * override to return an enqueued node which will replace the node this plugin is plugged in to. you can get that by
   * calling queuedNodeOf(bar(n.myArg1, n.myArg2, ...)).
   */
  protected def replacementNode(n: A): Node[B]
}
