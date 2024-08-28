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

import optimus.platform.ScenarioStack

import scala.util.Try

/**
 * A scheduler with slightly lower priority (and by default fewer threads). This is used for running InBackground steps
 * and other work that we don't want to clog up the main scheduler
 */
private[optimus] object AuxiliaryScheduler {
  val name = "auxiliary-scheduler"

  val scheduler = new OGScheduler(name, Settings.threadsAuxIdeal, Settings.threadPriorityAux)

  // this is a daemon thread
  def runOnAuxScheduler[T](node: Node[T], callback: Try[T] => Unit, cbSS: ScenarioStack = null): Unit =
    scheduler.evaluateNodeAsync(node, maySAS = true, trackCompletion = true, callback, cbSS = cbSS)
  def runOnAuxScheduler[T](node: Node[T]): Unit = scheduler.evaluateNodeAsync(node)
}

/**
 * A scheduler with an even lower thread priority than the AuxiliaryScheduler. This is used for running
 * InBackground.lowerPriority steps
 */
private[optimus] object LowerPriorityScheduler {
  val name = "lower-priority-scheduler"
  val scheduler = new OGScheduler(name, Settings.lowerPriorityThreadsIdeal, Settings.lowerThreadPriority)
}
