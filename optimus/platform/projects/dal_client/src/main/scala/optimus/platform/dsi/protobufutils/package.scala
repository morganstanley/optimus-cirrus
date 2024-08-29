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
package optimus.platform.dsi

import msjava.protobufutils.server.BackendException
import optimus.platform.dsi.bitemporal.DALRetryableActionException

package object protobufutils {

  private[this] val log = msjava.slf4jutils.scalalog.getLogger("protobufutils")

  private[optimus] def logPrefix(requestUuid: String, seqId: Int) = s"request ${requestUuid}, seq_id = ${seqId}:"

  private[optimus] def completeRequestDueToShutdown(completable: Completable, readOnly: Boolean) = {
    if (readOnly) {
      // protoclient=None, because -
      // 1. this shutdown is invoked in response to the protoclient shutdown procedure
      // so it would be redundant
      // 2. we would need to change this api to accept a protoclient as well, meaning all callers need to have protoclient
      // reference. One of the caller, BatchQueueShutdown is used in broker code as well, which will have to pass None.
      completable.completeWithException(new DALRetryableActionException("Connection shutdown", None))
    } else {
      completable.completeWithException(new BackendException("Connection shutdown"))
    }
  }

  private[optimus] def stopThread(thread: Thread): Unit = {
    val name = thread.getName()
    log.info("Stopping {} thread", name)
    thread.interrupt()
    while (thread.isAlive) {
      try {
        thread.join()
      } catch {
        case ie: InterruptedException =>
          log.info("Still waiting for {} thread to stop", name)
      }
    }
    log.info("{} thread stopped", name)
  }

}
