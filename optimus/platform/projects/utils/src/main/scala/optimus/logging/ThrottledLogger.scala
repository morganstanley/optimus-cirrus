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
package optimus.logging

import org.slf4j.Logger

/*
  Track messages/sec over exponentially averaged interval.
 */
class ThrottledWarnOrDebug(log: Logger, targetIntervalSec: Double, averagingTimeSec: Double) {
  require(
    targetIntervalSec > 0.0 && averagingTimeSec > targetIntervalSec,
    "Require averaging interval > target interval > 0")
  val Nmax = averagingTimeSec / targetIntervalSec
  val nu = 0.001 / averagingTimeSec // decay coefficient in 1/ms
  var Ntotal: Double = 0.0 // average number of warnings in interval
  var Nprinted: Double = 0.0 // average number of actually printed warnings in interval
  private var tLastPrintCheck = 0.0
  private var tLastWarning = 0.0
  private var skipped = 0

  final def fail(msg: => String): Unit = this.synchronized {
    val t = System.currentTimeMillis()
    Ntotal = 1.0 + Ntotal * Math.exp(-(t - tLastWarning) * nu)
    val r = -1.0 / (averagingTimeSec * Math.log(1.0 - 1 / Ntotal))
    tLastWarning = t.toDouble
    if (log.isDebugEnabled) {
      log.debug(f"$msg; warning rate=$r%.1f/sec")
    } else {
      Nprinted = Nprinted * Math.exp(-(t - tLastPrintCheck) * nu)
      tLastPrintCheck = t.toDouble
      if (Nprinted > Nmax) {
        skipped += 1
      } else {
        var s = msg
        if (skipped > 0) {
          s += f"; skipped $skipped warnings"
          skipped = 0
        }
        Nprinted += 1.0
        s += f"; warning rate $r%.1f/sec"
        log.warn(s)
      }
    }
  }

  final def succeed(msg: => String): Unit = this.synchronized {
    if (log.isDebugEnabled)
      log.debug(msg)
    else if (skipped > 0) {
      if (Math.exp(-(System.currentTimeMillis() - tLastPrintCheck) * nu) < Nmax) {
        log.info(s"$msg with $skipped skipped warnings")
        skipped = 0
        // Don't reset tLast, because we still want to throttle output even if success and failure rapidly interleave;
        // however, don't count success messages in measured rate.
      }
    }
  }
}

object ThrottlingDemoApp extends App {
  import org.slf4j.LoggerFactory
  import ch.qos.logback.classic.Level
  val log = LoggerFactory.getLogger(this.getClass)
  log.asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.INFO)
  val w = new ThrottledWarnOrDebug(log, 1.0, 5.0)
  for (i <- 0 to 5000) {
    Thread.sleep(1)
    w.fail("Whoops")
  }
  w.succeed("Phew")
  for (i <- 0 to 1000) {
    Thread.sleep(1)
    w.fail("ohno")
  }

}
