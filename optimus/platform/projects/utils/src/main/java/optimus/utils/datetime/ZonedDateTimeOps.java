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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

public final class ZonedDateTimeOps {

  // Disallow instantiation
  private ZonedDateTimeOps() {}

  // JSR310 accepted short ids for timezones (see java.time.ZoneId.SHORT_IDS), but java.time does
  // not.
  //
  // Additionally in Java 8 there is a difference in parsing behaviour with datetime strings that
  // contain
  // offset and timezone, made a bit worse by JSR310 insisting on a timezone being specified.
  // JSR310 looks at the zone and then applies an adjustment based on the offset provided:
  //
  // scala>
  // javax.time.calendar.ZonedDateTime.parse("2016-11-10T23:59:59.210-04:00[US/Eastern]").toInstant
  // res0: javax.time.Instant = 2016-11-11T03:59:59.210Z
  //                                        ^^
  // scala> java.time.ZonedDateTime.parse("2016-11-10T23:59:59.210-04:00[US/Eastern]").toInstant
  // res1: java.time.Instant = 2016-11-11T04:59:59.210Z
  //                                       ^^
  //
  // The java.time offset bug was fixed in Java 9
  // (see https://stackoverflow.com/questions/56255020/zoneddatetime-change-behavior-jdk-8-11),
  // but the short id issue remains, and because ZonedDateTime.parse is being used in places where
  // the strings come
  // externally, we will need to preserve the JSR310 behaviour.
  public static ZonedDateTime parseTreatingOffsetAndZoneIdLikeJSR310(CharSequence chars) {
    try {
      return ZonedDateTime.parse(chars);
    } catch (DateTimeParseException e) {
      // See if it failed because of use of short ids in the string. To be compatible with
      // javax.time
      // we have to support them by substituting them in the string to be parsed.
      String fixed = expandShortId(chars.toString());
      if (fixed != null) {
        return ZonedDateTime.parse(fixed);
      } else {
        // Not the format we aim to deal with here so throw original exception. JSR310 would have
        // done the same.
        throw e;
      }
    }
  }

  private static String expandShortId(String str) {
    int startIndex = str.indexOf('[');
    int endIndex = str.indexOf(']');
    if (startIndex == -1 || endIndex == -1) {
      return null;
    }
    String zoneId = str.substring(startIndex + 1, endIndex);
    // ZoneId.SHORT_IDS has the mapping to be used for supported short ids.
    // This treatment becomes consistent with ZoneId.of(str, ZoneID.SHORT_IDS)
    String compatibleId = ZoneId.SHORT_IDS.getOrDefault(zoneId, null);
    if (compatibleId != null) {
      return str.replace(zoneId, compatibleId);
    } else {
      return null;
    }
  }
}
