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
package optimus.buildtool.cache.silverking

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class StatusLogger(store: SilverKingStore, clusterStr: String) {

  private case class LoggedWarning(msg: String, failedTypes: Set[OperationType], retryTime: Option[Instant]) {
    def matches(newMsg: String, newFailedType: OperationType, newRetryTime: Option[Instant]): Boolean =
      msg == newMsg && failedTypes.contains(newFailedType) && retryTime == newRetryTime
  }

  private val lastWarning = new AtomicReference[Option[LoggedWarning]](None)
  private[silverking] def failed(
      alwaysLog: Boolean,
      opType: OperationType,
      msg: String,
      retryTime: Option[Instant] = None
  ): Unit = {
    val last = lastWarning.getAndUpdate {
      case Some(LoggedWarning(_, lastFailTypes, _)) =>
        Some(LoggedWarning(msg, lastFailTypes + opType, retryTime))
      case None =>
        Some(LoggedWarning(msg, Set(opType), retryTime))
    }
    if (!last.exists(_.matches(msg, opType, retryTime)) || alwaysLog) {
      store.warningTrace(s"$clusterStr ${opType.str}: $msg")
      retryTime.foreach { t =>
        store.warningTrace(s"Cache will be retried at $t (or restart OBT server to retry immediately)")
      }
    }
  }

  private[silverking] def succeeded(
      opType: OperationType
  ): Unit = {
    val last = lastWarning.getAndUpdate {
      case Some(LoggedWarning(lastMsg, lastFailTypes, lastRetryTime)) =>
        val newFailedTypes = lastFailTypes - opType
        if (newFailedTypes.nonEmpty) Some(LoggedWarning(lastMsg, newFailedTypes, lastRetryTime))
        else None
      case None =>
        None
    }
    val newlySucceeded = last.toSet.flatMap((w: LoggedWarning) => w.failedTypes).contains(opType)
    if (newlySucceeded) {
      store.infoTrace(s"$clusterStr ${opType.str} re-enabled")
    }
  }
}
