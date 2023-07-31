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
package optimus.stratosphere.utils

import optimus.stratosphere.logger.Logger

import java.util.concurrent.TimeUnit
import scala.collection.immutable.Seq

object IntervalPrinter {
  private[utils] def millis(interval: Long): String = {
    if (interval < 1000) {
      s"${interval}ms"
    } else {
      val ms = interval % 1000
      s"${seconds(TimeUnit.MILLISECONDS.toSeconds(interval))}${if (ms > 0) s"${ms}ms" else ""}"
    }
  }

  def seconds(totalSeconds: Long): String = {
    val sec = totalSeconds % 60
    val min = totalSeconds / 60

    if (min > 0) s"${min}m${sec}s"
    else s"${sec}s"
  }

  def from(startTimeInMillis: Long): String = millis(System.currentTimeMillis() - startTimeInMillis)

  def timeThis[T](label: String, logger: Logger)(fct: => T): T = {
    val startTimeMs = System.currentTimeMillis()
    try {
      logger.debug(s"[$label] Running...")
      fct
    } finally {
      logger.debug(f"[$label] Operation lasted ${from(startTimeMs)}.")
    }
  }

  def timeThis[T](cmdLine: Seq[String], logger: Logger)(fct: => T): T =
    timeThis(cmdLine.mkString(" "), logger)(fct)
}
