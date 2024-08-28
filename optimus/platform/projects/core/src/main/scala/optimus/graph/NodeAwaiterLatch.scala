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
 * Like a NodeAwaiter with a built in CountDownLatch, but one class instead of three (CDL has an internal AQS)
 */
final class NodeAwaiterLatch extends NodeAwaiter {
  @volatile private var isDone: Boolean = false

  /**
   * unblocks await
   */
  override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = synchronized {
    isDone = true
    notifyAll()
  }

  /**
   * blocks until onChildCompleted is called
   */
  def await(): Unit = if (!isDone) synchronized {
    while (!isDone) wait()
  }
}
