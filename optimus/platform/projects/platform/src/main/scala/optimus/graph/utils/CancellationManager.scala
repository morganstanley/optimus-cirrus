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
package optimus.graph.utils

import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.CancellationScope
import optimus.platform._

import scala.collection.mutable

object CancellationManager {
  private val log = getLogger(this)
}

/**
 * Manages and tracks CancellationScopes. You can run one or more concurrent requests through mgr.cancellable {
 * ...code... }, and cancel their execution from another place using mgr.cancelAll().
 *
 * This is useful for supporting Cancel buttons in UI apps which can cancel all tasks currently running, etc.
 *
 * Note that cancelAll() only cancels tasks currently running. Tasks scheduled after that will run normally unless
 * cancelAll() is called again.
 *
 * Note also that you need to set -Doptimus.cancellation=true for this to work properly.
 */
class CancellationManager {
  import CancellationManager.log

  private val cancellationScopes = mutable.Set.empty[CancellationScope]

  /**
   * Runs the supplied node function with support for cancellation under control of this manager
   */
  @async def cancellable[T](f: NodeFunction0[T]): Option[T] = {
    val scope = CancellationScope.newScope()
    cancellationScopes.synchronized {
      cancellationScopes.add(scope)
    }

    val result = asyncResult {
      val result = withCancellation(scope) {
        f()
      }

      if (result.isEmpty) {
        log.info("Request was cancelled")
      }

      result
    }

    // the if ( ) condition will always be true, but we need to introduce a data dependency on result otherwise
    // the removal code will run immediately after the above block is scheduled, before it has completed
    if (result.hasValue || result.hasException || result.isCancelled) {
      cancellationScopes.synchronized {
        cancellationScopes.remove(scope)
      }
    }

    result.value
  }

  /**
   * cancels all node functions currently running under this manager
   */
  def cancelAll(): Unit = {
    val toCancel = cancellationScopes.synchronized {
      cancellationScopes.toIndexedSeq
    }

    log.info(s"User requested cancellation; cancelling ${toCancel.size} in progress task(s)...")
    toCancel.foreach(_.cancel())
  }
}
