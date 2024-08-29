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
package optimus.platform.dsi.bitemporal

import optimus.utils.datetime.ZoneIds
import java.time.LocalDateTime
import optimus.platform.annotations.parallelizable
import optimus.utils.datetime.DateTimeStorable
import java.time.Instant
import optimus.platform.TimeInterval
import java.time.{ZonedDateTime, ZoneId}

@parallelizable
object DateTimeSerialization {
  private def fromLocalDateTime(dt: LocalDateTime): Long = {
    DateTimeStorable.storeDateTime(dt)
  }

  private def toLocalDateTime(l: Long): LocalDateTime = {
    DateTimeStorable.restoreDateTime(l)
  }

  def fromInstant(dt: Instant): Long = {
    if (dt == TimeInterval.Infinity)
      Long.MaxValue
    else if (dt == TimeInterval.NegInfinity)
      Long.MinValue
    else
      DateTimeStorable.storeDateTime(ZonedDateTime.ofInstant(dt, ZoneIds.UTC).toLocalDateTime)
  }

  def toInstant(l: Long): Instant = {
    if (l == Long.MaxValue)
      TimeInterval.Infinity
    else if (l == Long.MinValue)
      TimeInterval.NegInfinity
    else
      ZonedDateTime.of(DateTimeStorable.restoreDateTime(l), ZoneIds.UTC).toInstant
  }
}
