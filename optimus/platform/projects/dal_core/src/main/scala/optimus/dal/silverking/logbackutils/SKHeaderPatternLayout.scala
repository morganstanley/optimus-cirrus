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
package optimus.dal.silverking.logbackutils

import ch.qos.logback.classic.PatternLayout
import java.time.Clock
import java.time.ZonedDateTime
import optimus.utils.datetime.ZoneIds

class SKHeaderPatternLayout extends PatternLayout {
  val LINE_SEPARATOR = System.getProperty("line.separator")
  val clock = Clock.system(ZoneIds.UTC)

  private def getLogbackUtilsNewLogFileHeader(): String = {
    // Using ZonedDateTime.toOffsetDateTime.toString instead of directly calling ZonedDateTime.toString to omit zone info like '[UTC]'
    val dateString = ZonedDateTime.now(clock).toOffsetDateTime().toString()
    val buffer = new StringBuilder()
    buffer.append("# Logbackutils log file created on ")
    buffer.append(dateString)
    buffer.append(LINE_SEPARATOR)
    buffer.toString()
  }

  override def getFileHeader(): String = {
    getLogbackUtilsNewLogFileHeader()
  }
}
