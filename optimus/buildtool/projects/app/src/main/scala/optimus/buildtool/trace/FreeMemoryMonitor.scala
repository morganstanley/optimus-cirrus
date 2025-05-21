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
package optimus.buildtool.trace

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.utils.Utils

import java.util.Timer
import java.util.TimerTask

object FreeMemoryMonitor {
  private val log = getLogger(this)

  def fromProperties(): FreeMemoryMonitor = {
    val crit = sys.props.get("optimus.buildtool.memory.criticalMb").map(_.toInt).getOrElse(1000)
    val warn = sys.props.get("optimus.buildtool.memory.warningMb").map(_.toInt).getOrElse(5000)
    val checkFreq = sys.props.get("optimus.buildtool.memory.checkFrequencyMillis").map(_.toLong).getOrElse(60000L)
    val maxCrit = sys.props.get("optimus.buildtool.memory.maxCritical").map(_.toInt).getOrElse(3)
    val warnRepeat = sys.props.get("optimus.buildtool.memory.warningRepeat").map(_.toInt).getOrElse(5)
    new FreeMemoryMonitor(crit, warn, checkFreq, maxCrit, warnRepeat)
  }
}

class FreeMemoryMonitor(
    criticalThresholdMb: Int,
    warningThresholdMb: Int,
    checkFrequencyMillis: Long,
    maxCriticalCount: Int,
    warningRepeatCount: Long
) extends DefaultObtTraceListener {
  import MemoryThrottle._
  import FreeMemoryMonitor.log

  private val timer = new Timer(true)
  @volatile private var timerTask: Option[TimerTask] = None
  @volatile private var criticalCount = 0
  @volatile private var warningCount = 0

  private val warningSuffix =
    "This can cause very poor build performance due to disk swapping - please free up more system RAM."

  // Free RAM on linux isn't a reliable indicator of available RAM, so only warn on Windows
  override def startBuild(): Unit = if (Utils.isWindows) {
    val freeRam = getFreeRam.freeMb
    val freeHeap = getFreeHeap.freeMb
    val sufficientFreeRam = freeRam > freeHeap
    if (!sufficientFreeRam) {
      val msg =
        s"Less free system RAM (${Utils.mbToString(freeRam)}) than remaining heap (${Utils.mbToString(freeHeap)}). $warningSuffix"
      ObtTrace.warn(msg)
      log.warn(msg)
    }

    // need to capture current ObtTrace here, since it won't be visible from the timer thread
    val obtTrace = ObtTrace.current
    val task = new TimerTask() {
      override def run(): Unit = checkFreeMemory(obtTrace)
    }
    timerTask = Some(task)
    timer.scheduleAtFixedRate(task, checkFrequencyMillis, checkFrequencyMillis)
  }

  private def checkFreeMemory(obtTrace: ObtTraceListener): Unit = {
    val freeRam = getFreeRam.freeMb
    if (freeRam < criticalThresholdMb) {
      criticalCount += 1
      if (criticalCount < maxCriticalCount) {
        val coreMsg =
          s"Free RAM (${Utils.mbToString(freeRam)}) is below the critical threshold (${Utils.mbToString(criticalThresholdMb)})."
        // Don't need to log this via obtTrace, since logged errors always get sent to intellij
        log.error(s"$coreMsg OBT will exit soon if this continues...")
      } else {
        val coreMsg =
          s"Free RAM (${Utils.mbToString(freeRam)}) is still below the critical threshold (${Utils.mbToString(criticalThresholdMb)})."
        // Don't need to log this via obtTrace, since logged errors always get sent to intellij
        log.error(s"$coreMsg OBT will now exit.")
        // regex-ignore-line: thread-blocking
        Thread.sleep(1000) // Allow a little time for the message to be logged before exiting
        sys.exit(1)
      }

    } else {
      criticalCount = 0
      if (freeRam < warningThresholdMb) {
        if (warningCount == 0) {
          warningCount += 1
          val msg =
            s"Free RAM (${Utils.mbToString(freeRam)}) has dropped below the warning threshold (${Utils.mbToString(
                warningThresholdMb)}). $warningSuffix"
          obtTrace.warn(msg)
          log.warn(msg)
        } else if (warningCount < warningRepeatCount) {
          warningCount += 1
        } else {
          val msg = s"Free RAM (${Utils.mbToString(freeRam)}) is still below the warning threshold (${Utils.mbToString(
              warningThresholdMb)}). $warningSuffix"
          obtTrace.warn(msg)
          log.warn(msg)
          // prevent logging the "has dropped below" message next time round
          warningCount = 1
        }
      } else {
        // freeRam above both critical and warning thresholds
        warningCount = 0
      }
    }
  }

  override def endBuild(success: Boolean): Boolean = {
    timerTask.foreach(_.cancel())
    criticalCount = 0
    warningCount = 0
    true
  }
}
