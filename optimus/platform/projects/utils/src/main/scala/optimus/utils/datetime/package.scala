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
package optimus.utils

import java.math.BigInteger
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Month
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.chrono.ChronoLocalDate
import java.time.chrono.ChronoLocalDateTime
import java.time.chrono.ChronoZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.JulianFields
import java.util.concurrent.TimeUnit

import scala.math.Ordering.Implicits._

package object datetime {

  object ZoneIds {

    // java.time version of ZoneId does not define a constant for UTC and
    // the of("UTC") form seems to be doing a bit of parsing so we define
    // a constant here. The java.time version will call ZoneId.of("UTC")
    // when we make that switch.
    val UTC: ZoneId = ZoneId.of("UTC")
  }

  implicit class RichLocalDate(val d: LocalDate) extends AnyVal {
    def toModifiedJulianDays(): Long = {
      d.getLong(JulianFields.MODIFIED_JULIAN_DAY)
    }
  }

  //
  // This class is intended to provide convenience methods
  // over java.time classes as we migrate away from JSR310
  // (See OPTIMUS-21023)
  // While we intend to convert the calling code to use the
  // java.time style APIs, in some cases, these API's are very
  // verbose and clumsy and the calling code gets noticably worse
  // The functions below are to address this
  // The implementations will call the new java.util style APIs
  // we added to our copy of the JSR310 classes, allowing us to
  // test that the implementations are correct and that there will
  // be no behavioural change if anyone uses those API's directly.
  // When the above JIRA is completed, the imports above will be
  // changed to java.time.

  object LocalDateOps {
    //
    // There is no static method to create a LocalDate from
    // a long representing modified julian day. Some instance
    // needs to be created and a new copy made using the with
    // method.
    // The main use of these calls is from tight loops going
    // through date ranges. so we are storing one and will keep
    // re-using it to avoid object churn.
    private val someDay = LocalDate.ofEpochDay(0);

    def ofModifiedJulianDay(d: Long) = {
      // Implementing using the java.time compatible
      // API's added:
      someDay.`with`(JulianFields.MODIFIED_JULIAN_DAY, d)
    }

    def toModifiedJulianDay(d: LocalDate) = {
      d.getLong(JulianFields.MODIFIED_JULIAN_DAY)
    }

    // Following methods are to replace the JSR310
    // Period.daysBetween(from, to).getDay pattern
    // The pattern is similarly unsafe regarding Long->Int
    // conversion in the original so replacing those usages
    // with the below functions does not make things worse.
    // The method names make it a bit more explicit about
    // this conversion happening while retaining brevity.
    def intDaysBetween(startDate: LocalDate, endDate: LocalDate): Int = {
      startDate.until(endDate, ChronoUnit.DAYS).toInt
    }

    def intMonthsBetween(startDate: LocalDate, endDate: LocalDate): Int = {
      // Since we did not implement LocalDate.until for anything other
      // than ChronoUnit.DAYS, we'll just use the functions on Period
      // When we move to java.time, we can replace with:
      startDate.until(endDate, ChronoUnit.MONTHS).toInt
    }

    def intYearsBetween(startDate: LocalDate, endDate: LocalDate): Int = {
      // Since we did not implement LocalDate.until for anything other
      // than ChronoUnit.DAYS, we'll just use the functions on Period
      // When we move to java.time, we can replace with:
      startDate.until(endDate, ChronoUnit.YEARS).toInt
    }
  }

  object InstantOps {
    def ofEpochNano(epochNano: BigInteger): Instant = {
      val nanosPerSec = BigInteger.valueOf(1000000000)
      val divRem = epochNano.divideAndRemainder(nanosPerSec)
      if (divRem(0).bitLength > 63) {
        throw new ArithmeticException(s"Exceeds capacity of Instant: $epochNano")
      }
      Instant.ofEpochSecond(divRem(0).longValue(), divRem(1).intValue())
    }
  }

  implicit class RichInstant(val instant: Instant) extends AnyVal {
    def toEpochSecondBigDecimal: BigDecimal = {
      BigDecimal.valueOf(instant.getEpochSecond).add(BigDecimal.valueOf(instant.getNano, 9))
    }

    def toEpochNanoBigInteger: BigInteger = {
      BigInteger
        .valueOf(instant.getEpochSecond)
        .multiply(BigInteger.valueOf(1000000000))
        .add(BigInteger.valueOf(instant.getNano))
    }
  }

  implicit class RichDuration(val duration: Duration) extends AnyVal {

    def toHourMinuteSecond: (Long, Long, Long) = {
      (duration.getSeconds / 3600, (duration.getSeconds % 3600) / 60, (duration.getSeconds % 3600) % 60)
    }

    def prettyString: String = {
      if (duration < Duration.ofSeconds(1)) {
        s"${duration.getNano / 1000000} millis"
      } else {
        val (h, m, s) = toHourMinuteSecond
        Seq(
          (h, "hours"),
          (m, "minutes"),
          (s, "seconds")
        ).filter(r => r._1 != 0)
          .map { case (i, str) =>
            s"$i $str"
          }
          .mkString(" ")
      }
    }

    def toBigDecimal: BigDecimal = {
      BigDecimal.valueOf(duration.getSeconds).add(BigDecimal.valueOf(duration.getNano, 9))
    }
  }

  implicit class RichChronoUnit(val unit: ChronoUnit) extends AnyVal {
    def toTimeUnit: TimeUnit = {
      unit match {
        case ChronoUnit.NANOS   => TimeUnit.NANOSECONDS
        case ChronoUnit.MICROS  => TimeUnit.MICROSECONDS
        case ChronoUnit.MILLIS  => TimeUnit.MILLISECONDS
        case ChronoUnit.SECONDS => TimeUnit.SECONDS
        case ChronoUnit.MINUTES => TimeUnit.MINUTES
        case ChronoUnit.HOURS   => TimeUnit.HOURS
        case ChronoUnit.DAYS    => TimeUnit.DAYS
        case _                  => throw new IllegalArgumentException(s"Can't convert $unit to TimeUnit")
      }
    }

    def convert(sourceDuration: Long, sourceUnit: ChronoUnit): Long = {
      toTimeUnit.convert(sourceDuration, sourceUnit.toTimeUnit)
    }
  }

  implicit class RichMonth(val month: Month) extends AnyVal {

    def getQuarterOfYear = {
      val ordinal = month.getValue - 1
      if (ordinal < 3) 1 else if (ordinal < 6) 2 else if (ordinal < 9) 3 else 4
    }
    def getMonthOfQuarter = {
      val ordinal = month.getValue - 1
      (ordinal % 3) + 1
    }
  }

  object JSR310Conversions {

    /* implicit class ConvertibleInstant(val instant: Instant) extends AnyVal {
      def toEpochNano = toJSR310.toEpochNano
      def toJSR310: javax.time.Instant = javax.time.Instant.ofEpochSecond(instant.getEpochSecond, instant.getNano)
    }

    implicit class ConvertibleJSRInstant(val instant: javax.time.Instant) extends AnyVal {
      def toJavaTime: Instant = Instant.ofEpochSecond(instant.getEpochSecond, instant.getNanoOfSecond)
    }

    implicit class ConvertibleZonedDateTime(val zdt: ZonedDateTime) extends AnyVal {
      def toJSR310: javax.time.calendar.ZonedDateTime =
        javax.time.calendar.ZonedDateTime.of(
          zdt.getYear,
          zdt.getMonthValue,
          zdt.getDayOfMonth,
          zdt.getHour,
          zdt.getMinute,
          zdt.getSecond,
          zdt.getNano,
          javax.time.calendar.ZoneId.of(zdt.getZone.getId, ZoneId.SHORT_IDS)
        )
    }

    implicit class ConvertibleJSRZoneId(val zoneId: javax.time.calendar.ZoneId) extends AnyVal {
      def toJavaTime = ZoneId.of(zoneId.getID, ZoneId.SHORT_IDS)
    }

    implicit class ConvertibleJSRLocalDate(val ld: javax.time.calendar.LocalDate) extends AnyVal {
      def toJavaTime: LocalDate = LocalDate.of(ld.getYear, ld.getMonthOfYear.getValue, ld.getDayOfMonth)
    }

    implicit class ConvertibleJSRZonedDateTime(val zdt: javax.time.calendar.ZonedDateTime) extends AnyVal {
      def toJavaTime: ZonedDateTime =
        ZonedDateTime.of(
          zdt.getYear,
          zdt.getMonthOfYear.getValue,
          zdt.getDayOfMonth,
          zdt.getHourOfDay,
          zdt.getMinuteOfHour,
          zdt.getSecondOfMinute,
          zdt.getNanoOfSecond,
          ZoneId.of(zdt.getZone.getID, ZoneId.SHORT_IDS)
        )
    } */

  }

  class ComparableLocalDate(val thiz: LocalDate) extends AnyVal with Comparable[LocalDate] {
    override def compareTo(that: LocalDate): Int = thiz.compareTo(that)
  }

  class ComparableLocalDateTime(val thiz: LocalDateTime) extends AnyVal with Comparable[LocalDateTime] {
    override def compareTo(that: LocalDateTime): Int = thiz.compareTo(that)
  }

  class ComparableZonedDateTime(val thiz: ZonedDateTime) extends AnyVal with Comparable[ZonedDateTime] {
    override def compareTo(that: ZonedDateTime): Int = thiz.compareTo(that)
  }

  trait OrderingImplicits {

    implicit def toComparableLocalDate(thiz: LocalDate): ComparableLocalDate = new ComparableLocalDate(thiz)
    implicit def toComparableLocalDateTime(thiz: LocalDateTime): ComparableLocalDateTime = new ComparableLocalDateTime(
      thiz)
    implicit def toComparableZonedDateTime(thiz: ZonedDateTime): ComparableZonedDateTime = new ComparableZonedDateTime(
      thiz)

    //
    // Ordering[T] for some java.time classes that don't implement Comparable of their-own types, not provided by scala.
    // The complication here is that for example LocalDate does not implement Comparable[localDate] but rather
    // extends ChronoLocalDate which implements Comparable[ChronoLocalDate]
    // types not covered here all extend Comparable of themselves which would be covered by the default ordering
    // for Comparable.
    //
    implicit def localDateOrdering[T <: Comparable[ChronoLocalDate]]: Ordering[T] = new Ordering[T] {
      override def compare(l: T, r: T): Int = l.compareTo(r.asInstanceOf[ChronoLocalDate])
    }
    implicit def localDateTimeOrdering[T <: Comparable[ChronoLocalDateTime[_]]]: Ordering[T] = new Ordering[T] {
      override def compare(l: T, r: T): Int = l.compareTo(r.asInstanceOf[ChronoLocalDateTime[_]])
    }
    implicit def zonedDateTimeOrdering[T <: Comparable[ChronoZonedDateTime[_]]]: Ordering[T] = new Ordering[T] {
      override def compare(l: T, r: T): Int = l.compareTo(r.asInstanceOf[ChronoZonedDateTime[_]])
    }
  }

  object OrderingImplicits extends OrderingImplicits
}
