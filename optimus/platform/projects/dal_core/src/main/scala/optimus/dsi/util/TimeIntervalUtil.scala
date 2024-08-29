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
package optimus.dsi.util

import java.time.Duration

import optimus.platform.TimeInterval

object TimeIntervalUtil {

  /**
   * Divides input time interval into smaller chunks..
   *
   * Note that time interval taken by get-The-Gap (GTG) is exclusive from both sides, whereas recorder is inclusive on
   * left and exclusive on right. Hence, minusNanos and plusNanos.
   *
   * To give an example suppose input time interval is 00:10:00 to 00:15:00, with chunk duration of 1 min. So, input
   * interval would be divided as following: (2015-02-02T00:09:59.999999Z, 2015-02-02T00:11Z) - from GTG perspective its
   * like [10, 11) (2015-02-02T00:10:59.999999Z, 2015-02-02T00:12Z) - from GTG perspective its like [11, 12) ... so
   * forth up to [14, 15).
   */
  def divideInterval(
      inputInterval: TimeInterval,
      chunkDuration: Duration,
      inclusive: Boolean = false): Seq[TimeInterval] = {

    val count = (inputInterval.to
      .minusSeconds(inputInterval.from.getEpochSecond)
      .getEpochSecond / chunkDuration.getSeconds.toFloat).ceil.longValue
    if (count > 0) {

      0L.to(count - 1).map { c =>
        val st =
          if (inclusive)
            inputInterval.from.plusSeconds(chunkDuration.getSeconds * c)
          else
            inputInterval.from.plusSeconds(chunkDuration.getSeconds * c).minusNanos(1000)

        val et =
          if (inclusive)
            st.plusSeconds(chunkDuration.getSeconds)
          else
            st.plusSeconds(chunkDuration.getSeconds).plusNanos(1000)

        if (et isAfter inputInterval.to)
          TimeInterval(st, inputInterval.to)
        else
          TimeInterval(st, et)
      }
    } else {
      Seq(inputInterval)
    }
  }

}
