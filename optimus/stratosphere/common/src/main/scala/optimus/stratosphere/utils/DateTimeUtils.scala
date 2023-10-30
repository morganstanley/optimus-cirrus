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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ofPattern

object DateTimeUtils {

  def formatDateTime(pattern: String, dateTime: ZonedDateTime): String =
    dateTime.format(ofPattern(pattern))

  object Patterns {
    val fileDateTimeUpToMillisSuffix = "yyyy.MM.dd-HH.mm.ss.SSS"
  }

  def nextDateTimeSuffix(): String = formatDateTime(Patterns.fileDateTimeUpToMillisSuffix, ZonedDateTime.now())

}
