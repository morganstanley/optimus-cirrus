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
package optimus.dsi.utils

import java.time.DayOfWeek
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoField
import java.time.temporal.TemporalAdjusters

import optimus.config.RuntimeEnvironmentEnum
import optimus.platform.dal.config.DALEnvs
import optimus.platform.dal.config.DalEnv

object WeekendWindow {
  private val weekendWindowPattern = """(FRI|SAT|SUN)(\d\d)-(FRI|SAT|SUN)(\d\d)""".r
  private val weekendWindowPatternWithMinutes = """(FRI|SAT|SUN)(\d\d):(\d\d)-(FRI|SAT|SUN)(\d\d):(\d\d)""".r
  val zone = ZoneId.of("America/New_York")
  val DefaultProdWindow = "SAT09-SUN11"
  val DefaultNonProdWindow = "FRI19:30-SUN19:30"
  val WeekendWindowProperty = "optimus.dal.weekend.window"

  def byEnv(env: DalEnv): WeekendWindow = {
    DALEnvs.envInfoRuntime(DalEnv(env.mode)) match {
      case RuntimeEnvironmentEnum.prod => apply(System.getProperty(WeekendWindowProperty, DefaultProdWindow))
      case _                           => apply(System.getProperty(WeekendWindowProperty, DefaultNonProdWindow))
    }
  }

  def apply(weekendWindow: String): WeekendWindow = {
    weekendWindow.toUpperCase match {
      case weekendWindowPattern(day1, t1, day2, t2) =>
        val d1 = parseDay(day1)
        val d2 = parseDay(day2)
        val h1 = parseHour(t1)
        val h2 = parseHour(t2)
        require(d1 < d2 || (d1 == d2 && h1 <= h2), s"From should no later than to: $weekendWindow")
        WeekendWindow(d1, h1, 0, d2, h2, 0)
      case weekendWindowPatternWithMinutes(day1, t1, min1, day2, t2, min2) =>
        val d1 = parseDay(day1)
        val d2 = parseDay(day2)
        val h1 = parseHour(t1)
        val h2 = parseHour(t2)
        val m1 = parseMinute(min1)
        val m2 = parseMinute(min2)
        require(
          d1 < d2 || (d1 == d2 && h1 < h2) || (d1 == d2 && h1 == h2 && m1 < m2),
          s"From should no later than to: $weekendWindow")
        WeekendWindow(d1, h1, m1, d2, h2, m2)
      case _ => throw new IllegalArgumentException(s"Invalid format: $weekendWindow")
    }
  }

  private def parseMinute(m: String): Int = {
    val minute = m.toInt
    require(minute >= 0 && minute < 60, "minute should be 0 to 59")
    minute
  }

  private def parseHour(h: String): Int = {
    val hour = h.toInt
    require(hour >= 0 && hour < 24, "hour should be 0 to 23")
    hour
  }

  private def parseDay(day1: String) = {
    day1 match {
      case "SAT" => DayOfWeek.SATURDAY.getValue
      case "SUN" => DayOfWeek.SUNDAY.getValue
      case "FRI" => DayOfWeek.FRIDAY.getValue
    }
  }

}

final case class WeekendWindow(
    startDay: Int,
    startHour: Int,
    startMinute: Int,
    endDay: Int,
    endHour: Int,
    endMinute: Int) {
  override def toString(): String = {
    s"${dayToString(startDay)}${hourToString(startHour)}:${hourToString(startMinute)}-${dayToString(
        endDay)}${hourToString(endHour)}:${hourToString(endMinute)}"
  }

  private def hourToString(h: Int): String = {
    h.toString match {
      case x if x.length == 1 => s"0$x"
      case x                  => x
    }
  }

  private def dayToString(d: Int): String = {
    DayOfWeek.of(d).toString.substring(0, 3)
  }

  def now: LocalDateTime = LocalDateTime.now(WeekendWindow.zone)

  def isInWindow(): Boolean = {
    isInWindow(now)
  }

  def isInWindow(t: LocalDateTime): Boolean = {
    val d = t.getDayOfWeek.getValue
    val h = t.getHour
    val m = t.getMinute
    (d > startDay || (d == startDay && h > startHour) || (d == startDay && h == startHour && m >= startMinute)) &&
    (d < endDay || (d == endDay && h < endHour) || (d == endDay && h == endHour && m < endMinute))
  }

  def currentOrNextWindow(from: LocalDateTime): LocalDateTimeWindow = {
    if (isInWindow(from)) {
      val end = from
        .`with`(TemporalAdjusters.next(DayOfWeek.of(endDay)))
        .`with`(ChronoField.HOUR_OF_DAY, endHour)
        .`with`(ChronoField.MINUTE_OF_HOUR, endMinute)
      LocalDateTimeWindow(from, end)
    } else {
      LocalDateTimeWindow(
        from
          .`with`(TemporalAdjusters.next(DayOfWeek.of(startDay)))
          .`with`(ChronoField.HOUR_OF_DAY, startHour)
          .`with`(ChronoField.MINUTE_OF_HOUR, startMinute),
        from
          .`with`(TemporalAdjusters.next(DayOfWeek.of(endDay)))
          .`with`(ChronoField.HOUR_OF_DAY, endHour)
          .`with`(ChronoField.MINUTE_OF_HOUR, endMinute)
      )
    }
  }

  /**
   * If from is in current window, and the left time in current window is no less than minLength then Get time interval
   * left in current window Else return time interval of next full window.
   */
  def currentOrNextWindow(from: LocalDateTime, minLength: Duration): LocalDateTimeWindow = {
    val first = currentOrNextWindow(from)
    val duration = Duration.between(first.from, first.to)
    if (duration.compareTo(minLength) < 0) {
      currentOrNextWindow(first.to.plusSeconds(60))
    } else {
      first
    }
  }
}

final case class LocalDateTimeWindow(from: LocalDateTime, to: LocalDateTime)
