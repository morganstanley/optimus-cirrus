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

package optimus.breadcrumbs

import java.io.IOException

import msjava.slf4jutils.scalalog.Logger
import org.apache.http.client.ClientProtocolException

trait RetryWaiter {
  def waitFor(timeoutInMs: Long): Unit
}

object RetrySleeper extends RetryWaiter {
  def waitFor(timeoutInMs: Long) = Thread.sleep(timeoutInMs)
}

object RetryWithExponentialBackoff {
  private[breadcrumbs] val MaxAttempts: Int = Integer.getInteger("optimus.breadcrumbs.metrics.retry.totalAttempts", 8)
  private[breadcrumbs] val InitialBackoffTime: Long =
    java.lang.Long.getLong("optimus.breadcrumbs.metrics.retry.initialBackoffTimeMs", 2000L)
  private[breadcrumbs] val MaxBackoffTime: Long =
    java.lang.Long.getLong("optimus.breadcrumbs.metrics.retry.maxBackoffTimeMs", 300000L)
}

trait RetryWithExponentialBackoff {
  import RetryWithExponentialBackoff._
  protected def log: Logger

  def isExpectedException(e: Throwable): Boolean = {
    e match {
      case e @ (_: IOException | _: ClientProtocolException) => true
      case _                                                 => false
    }
  }

  protected def withRetry[T](
      op: => T,
      maxAttempts: Int = MaxAttempts,
      description: String = "",
      waiter: RetryWaiter = RetrySleeper): T = withRetry(maxAttempts, description, waiter)(op)

  protected def withRetry[T](op: => T)(retryOp: => Unit): T = withRetryCore(MaxAttempts, "", RetrySleeper)(op, retryOp)

  protected def withRetry[T](maxAttempts: Int, description: String)(op: => T): T =
    withRetryCore(maxAttempts, description, RetrySleeper)(op, ())

  protected def withRetry[T](maxAttempts: Int, description: String, waiter: RetryWaiter)(op: => T): T =
    withRetryCore(maxAttempts, description, waiter)(op, ())

  private def withRetryCore[T](maxAttempts: Int, description: String, waiter: RetryWaiter)(
      op: => T,
      retryOp: => Unit): T = {
    var attemptCounter = maxAttempts
    var backoffTime = InitialBackoffTime
    var result: T = null.asInstanceOf[T]

    @volatile var done = false

    while (!done) {
      try {
        attemptCounter -= 1
        result = op
        done = true
      } catch {
        case e if isExpectedException(e) =>
          log.warn(s"$description -- an exception occurred:", e)
          if (attemptCounter <= 0) {
            done = true
            log.error(
              s"$description -- attempt ${maxAttempts - attemptCounter} of $maxAttempts failed, retries exhausted, last exception was: ",
              e)
            throw e
          } else {
            log.info(
              s"$description -- attempt ${maxAttempts - attemptCounter} of $maxAttempts failed, retrying in ${backoffTime} ms")
            waiter.waitFor(backoffTime)
            backoffTime = math.min(backoffTime << 1, MaxBackoffTime)
            retryOp
          }
      }
    }
    result
  }
}
