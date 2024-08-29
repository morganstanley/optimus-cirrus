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
package optimus.platform.dsi.protobufutils
import optimus.graph.DiagnosticSettings

import java.util
import scala.annotation.tailrec

final class SharedRequestLimiter(private val countLimit: Int, private val weightLimit: Long) {
  require(countLimit > 0)
  require(weightLimit > 0)

  private val queue = new util.ArrayDeque[Task]()
  private var totalCount: Int = 0
  private var totalWeight: Long = 0

  def push(request: ClientRequest): ClientRequest = {
    val weight: Long = request.commandProtos.iterator.map(_.getSerializedSize).sum
    val task = new Task(weight)
    queue.synchronized {
      queue.add(task)
      trigger()
    }
    task.synchronized {
      while (!task.triggered) task.wait()
    }
    request.withCompletable(InterceptedCompletable(request.completable, _ => task.complete()))
  }

  @tailrec
  private def trigger(): Unit = {
    require(Thread.holdsLock(queue))

    Option(queue.peek()) match {
      case Some(task) =>
        if (
          totalCount <= countLimit - 1 &&
          (totalWeight == 0 ||
            (task.weight <= weightLimit && totalWeight <= weightLimit - task.weight))
        ) {
          totalCount += 1
          totalWeight += task.weight
          queue.poll()

          task.synchronized {
            task.triggered = true
            task.notifyAll()
          }

          trigger()
        }
      case _ =>
    }
  }

  private class Task(val weight: Long) {
    var triggered: Boolean = false
    def complete(): Unit = queue.synchronized {
      totalCount -= 1
      totalWeight -= weight

      trigger()
    }
  }
}
object SharedRequestLimiter {
  private val writeLimitEnabled =
    DiagnosticSettings.getBoolProperty("optimus.platform.dsi.request.write.limitEnabled", true)
  private val writeCountLimit =
    DiagnosticSettings.getIntProperty("optimus.platform.dsi.request.write.countLimit", 48)
  private val writeWeightLimit =
    DiagnosticSettings.getLongProperty(
      "optimus.platform.dsi.request.write.weightLimit",
      Runtime.getRuntime.maxMemory() / 8)
  val write: Option[SharedRequestLimiter] =
    if (writeLimitEnabled) Some(new SharedRequestLimiter(writeCountLimit, writeWeightLimit)) else None
}
