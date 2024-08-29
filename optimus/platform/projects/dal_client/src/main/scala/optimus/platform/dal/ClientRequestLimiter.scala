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
package optimus.platform.dal

import java.util.concurrent.Semaphore
import msjava.slf4jutils.scalalog.getLogger

object ClientRequestLimiter {
  lazy val unlimited = new ClientRequestLimiter(Int.MaxValue)
  private val log = getLogger(this)
  private lazy val errorMsg =
    s"Consider raising the command limits by overriding JVM arg ${RuntimeProperties.DsiReadCmdLimit}"
}

class ClientRequestLimiter(lockCount: Int) {
  import ClientRequestLimiter._
  private[this] val semaphore = if (lockCount >= 0 && lockCount != Int.MaxValue) {
    log.info(s"Creating request limiter with limits: ${lockCount}")
    Some(new Semaphore(lockCount))
  } else None

  protected def notifyBlock: Unit = {}
  protected def notifyRelease: Unit = {}

  def take(count: Int, reqId: String = "Unknown"): Unit = semaphore foreach { s =>
    require(count <= lockCount, s"requested permits (${count}) are more than total permits (${lockCount}). ${errorMsg}")
    log.debug(s"Taking ${count} locks for $reqId")
    if (!s.tryAcquire(count)) {
      log.warn(s"${reqId} might get blocked for ${count} permits. Available permits: ${availablePermits()}")
      notifyBlock
      s.acquire(count)
      notifyRelease
    }
  }

  def release(count: Int, reqId: String = "Unknown"): Unit = semaphore foreach { s =>
    log.debug(s"releasing ${count} locks for ${reqId}")
    s.release(count)
    val permits = s.availablePermits()
    require(permits <= lockCount, s"released permits (${permits} are more than lockCount: ${lockCount})")
  }

  def availablePermits(): Int = semaphore.map { _.availablePermits() } getOrElse lockCount
}
