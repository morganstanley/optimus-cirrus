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
package optimus.utils.datetime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.JulianFields;

/**
 * DateTimeStorable provides static utility functions to convert {@link LocalDate} and {@link
 * LocalDateTime} to binary format.
 *
 * <p>The binary format for LocalDate is the Modified Julian Day Number, stored in the last 24 bits
 * (3 bytes) of an int. This gives a date range of 1858/11/17 to 47793/05/02.
 *
 * <p>The binary format for LocalDateTime stores the date part in the first 24 bits (3 bytes) and
 * the time offset in the day in the last 40 bits of a long. The time part is stored in microsecond
 * (1E-06) precision. This ordering of date and time allows for direct comparison and sorting of the
 * binary values. Microsecond precision needs 37 bits, so 3 bits are unused.
 */
public class DateTimeStorable {
  private static int maxMJD = (int) Math.pow(2, 24) - 1; // 16777215
  private static long maxMicros = 86399999999L; // 24*60*60*1000*1000 - 1

  /**
   * Convert a {@link LocalDate} to a binary representation.
   *
   * @param date
   * @return binary
   * @throws IllegalArgumentException
   */
  public static int storeDate(LocalDate date) throws IllegalArgumentException {
    if (date == null) {
      throw new IllegalArgumentException(String.format("Need LocalDateTime, not null"));
    }
    long mjd = date.getLong(JulianFields.MODIFIED_JULIAN_DAY);
    if (mjd > maxMJD || mjd < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot store %d as julian days, out of range.", mjd));
    }
    return (int) mjd;
  }

  /**
   * Convert binary representation to a {@link LocalDate}.
   *
   * @param binary
   * @return {@link LocalDate}
   * @throws IllegalArgumentException
   */
  public static LocalDate restoreDate(int binary) throws IllegalArgumentException {
    int mjd = binary;
    if (mjd > maxMJD || mjd < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot restore %d as julian days, out of range.", mjd));
    }
    return LocalDate.ofEpochDay(0).with(JulianFields.MODIFIED_JULIAN_DAY, mjd);
  }

  /**
   * Convert a {@link LocalDateTime} to a binary representation.
   *
   * @param dateTime
   * @return binary
   * @throws IllegalArgumentException
   */
  public static long storeDateTime(LocalDateTime dateTime) throws IllegalArgumentException {
    if (dateTime == null) {
      throw new IllegalArgumentException(String.format("Need LocalDateTime, not null"));
    }
    long mjd = storeDate(dateTime.toLocalDate());
    long timeOffset = dateTime.getHour(); // hours
    timeOffset = timeOffset * 60 + dateTime.getMinute(); // minutes
    timeOffset = timeOffset * 60 + dateTime.getSecond(); // seconds
    timeOffset = timeOffset * (long) 1E06 + dateTime.getNano() / 1000; // microseconds
    return (mjd << 40) + timeOffset;
  }

  /**
   * Convert a binary representation to a {@link LocalDateTime}.
   *
   * @param binary
   * @return {@link LocalDateTime}
   * @throws IllegalArgumentException
   */
  public static LocalDateTime restoreDateTime(long binary) throws IllegalArgumentException {
    // 24 bits will always fit in valid range, no check needed
    int mjd = (int) (binary >>> 40);
    // Only use lower 40 bits, shifting left then right, perhaps use mask instead
    long micros = (binary << 24) >>> 24;

    if (micros > maxMicros || micros < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot restore %d micros, out of range. Max = %d", micros, maxMicros));
    }
    LocalDate julian = LocalDate.ofEpochDay(0).with(JulianFields.MODIFIED_JULIAN_DAY, mjd);
    LocalDateTime dateTime = julian.atStartOfDay().plusNanos(micros * 1000);
    return dateTime;
  }
}
