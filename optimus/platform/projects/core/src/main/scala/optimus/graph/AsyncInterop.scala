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
import optimus.platform.annotations.nodeSync

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * Convenience methods to interoperate with other asynchronous frameworks
 */
object AsyncInterop {

  /**
   * Await a future without blocking an optimus thread.
   */
  @nodeSync
  def future[A](future: Future[A], timeoutMillis: Option[Int] = None)(implicit ec: ExecutionContext): A =
    future$queued[A](future, timeoutMillis)(ec).get$
  def future$queued[A](future: Future[A], timeoutMillis: Option[Int] = None)(implicit
      ec: ExecutionContext): NodeFuture[A] = {
    val promise = NodePromise.uncancellable[A](NodeTaskInfo.Future, timeoutMillis)
    // completes immediately if the future is already complete.
    future.onComplete(promise.complete)(ec)
    promise.await$queued
  }

}
