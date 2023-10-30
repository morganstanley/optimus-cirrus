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
package optimus.utils.app

import optimus.utils.app.Args4jOptionHandlers._
import optimus.utils.datetime.ZonedDateTimeOps
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter
import patch.MicroZonedDateTime

import java.time.Duration
import java.time._

/*
 * Args4j handlers for java.time classes.
 */

/**
 * Parses periods starting with 'P' (the standard) and without 'P' (ie. P1M1DT1M represents 1 month, 1 day, 1 minute)
 */
trait PeriodParsing {
  protected def parseAsPeriod(arg: String): Period = {
    // Only support Date fields as java.time.Period won't support time fields. For such arguments
    // DurationFromPeriodOptionHandler should be used.
    if (arg.contains("T"))
      throw new IllegalArgumentException("Period string needs to follow the java.time.Period format")
    (arg.toUpperCase: Seq[Char]) match {
      case Seq('P', _) => Period.parse(arg)
      case _           => Period.parse("P" + arg)
    }
  }
}

trait DurationParsing {
  protected def parseAsDuration(arg: String): Duration = Duration.parse(arg)
}

final class PeriodOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Period])
    extends OneArgumentOptionHandler[Period](parser, option, setter)
    with PeriodParsing {
  override def parse(arg: String): Period = parseAsPeriod(arg)
}

final class PeriodOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Period]])
    extends OptionOptionHandler[Period](parser, option, setter)
    with PeriodParsing {
  override def convert(arg: String): Period = parseAsPeriod(arg)
}

final class DurationFromPeriodOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Duration])
    extends OneArgumentOptionHandler[Duration](parser, option, setter)
    with DurationParsing {
  override def parse(arg: String): Duration = parseAsDuration(arg)
}

final class DurationFromPeriodOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[Duration]])
    extends OptionOptionHandler[Duration](parser, option, setter)
    with DurationParsing {
  override def convert(arg: String): Duration = parseAsDuration(arg)
}

final class InstantHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Instant])
    extends OneArgumentOptionHandler[Instant](parser, option, setter) {
  override def parse(arg: String): Instant = stringToInstant(arg)
}

final class LocalTimeOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[LocalTime])
    extends OneArgumentOptionHandler[LocalTime](parser, option, setter) {
  override def parse(arg: String): LocalTime = LocalTime.parse(arg)
}

final class ZoneIdOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[ZoneId])
    extends OneArgumentOptionHandler[ZoneId](parser, option, setter) {
  override def parse(arg: String): ZoneId = ZoneId.of(arg, ZoneId.SHORT_IDS)
}

final class DurationOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Duration])
    extends OneArgumentOptionHandler[Duration](parser, option, setter) {
  override def parse(arg: String): Duration = Duration.parse(arg)
}

final class ZonedDateTimeOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[ZonedDateTime])
    extends OneArgumentOptionHandler[ZonedDateTime](parser, option, setter) {
  override def parse(arg: String): ZonedDateTime = ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(arg)
}

final class LocalDateTimeOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[LocalDateTime])
    extends OneArgumentOptionHandler[LocalDateTime](parser, option, setter) {
  override def parse(arg: String): LocalDateTime = LocalDateTime.parse(arg)
}

final class LocalDateOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[LocalDate])
    extends OneArgumentOptionHandler[LocalDate](parser, option, setter) {
  override def parse(arg: String): LocalDate = LocalDate.parse(arg)
}

final class ZonedDateTimeOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[ZonedDateTime]])
    extends OptionOptionHandler[ZonedDateTime](parser, option, setter) {
  override def convert(arg: String): ZonedDateTime =
    if (arg == "now") MicroZonedDateTime.now
    else ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(arg)
}

final class LocalDateTimeOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[LocalDateTime]])
    extends OptionOptionHandler[LocalDateTime](parser, option, setter) {
  override def convert(arg: String): LocalDateTime = LocalDateTime.parse(arg)
}

final class LocalDateOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[LocalDate]])
    extends OptionOptionHandler[LocalDate](parser, option, setter) {
  override def convert(arg: String): LocalDate = LocalDate.parse(arg)
}

final class LocalDateOrTodayOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[LocalDate]])
    extends OptionOptionHandler[LocalDate](parser, option, setter) {
  override def convert(arg: String): LocalDate =
    if (arg == "today") LocalDate.now() else LocalDate.parse(arg)
}

final class LocalTimeOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[LocalTime]])
    extends OptionOptionHandler[LocalTime](parser, option, setter) {
  override def convert(arg: String): LocalTime = LocalTime.parse(arg)
}

final class LocalDateRangeOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[(LocalDate, LocalDate)]])
    extends OptionOptionHandler[(LocalDate, LocalDate)](parser, option, setter) {
  override def convert(s: String): (LocalDate, LocalDate) = s.split(";") match {
    case Array(head, last) => (LocalDate.parse(head), LocalDate.parse(last))
    case arr =>
      throw new IllegalArgumentException(
        s"Two dates separated by ; are needed to create a range. Found ${arr.size} args")
  }
}

final class LocalDateWithDaysAndOffsetOptionOptionHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[Option[(LocalDate, Either[Int, (Int, Int)])]])
    extends OptionOptionHandler[(LocalDate, Either[Int, (Int, Int)])](parser, option, setter) {
  override def convert(s: String): (LocalDate, Either[Int, (Int, Int)]) = s.split(";") match {
    case Array(date, days)             => (LocalDate.parse(date), Left(days.toInt))
    case Array(date, days, daysOffset) => (LocalDate.parse(date), Right(days.toInt, daysOffset.toInt))
    case _ => throw new IllegalArgumentException(s"Require 'date;days' or 'date;days;offset' . Got $s instead")
  }
}

final class DelimitedZDTOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[ZonedDateTime]])
    extends CommaDelimitedArgumentOptionHandler[ZonedDateTime](parser, option, setter) {
  def convert(s: String): ZonedDateTime =
    if (s == "now") MicroZonedDateTime.now else ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(s)
}
