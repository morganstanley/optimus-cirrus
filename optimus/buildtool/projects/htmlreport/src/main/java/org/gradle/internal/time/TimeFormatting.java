/*
 * Copyright 2017 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
 *
 * Modifications were made to that code for compatibility with Optimus Build Tool and its report file layout.
 * For those changes only, where additions and modifications are indicated with 'ms' in comments:
 *
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

package org.gradle.internal.time;

import java.math.BigDecimal;

public class TimeFormatting {

  private static final int MILLIS_PER_SECOND = 1000;
  private static final int MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
  private static final int MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
  private static final int MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

  private TimeFormatting() {}

  public static String formatDurationVeryTerse(long duration) {
    if (duration == 0) {
      return "0s";
    }

    StringBuilder result = new StringBuilder();

    long days = duration / MILLIS_PER_DAY;
    duration = duration % MILLIS_PER_DAY;
    if (days > 0) {
      result.append(days);
      result.append("d");
    }
    long hours = duration / MILLIS_PER_HOUR;
    duration = duration % MILLIS_PER_HOUR;
    if (hours > 0 || result.length() > 0) {
      result.append(hours);
      result.append("h");
    }
    long minutes = duration / MILLIS_PER_MINUTE;
    duration = duration % MILLIS_PER_MINUTE;
    if (minutes > 0 || result.length() > 0) {
      result.append(minutes);
      result.append("m");
    }
    int secondsScale = result.length() > 0 ? 2 : 3;
    result.append(
        BigDecimal.valueOf(duration)
            .divide(BigDecimal.valueOf(MILLIS_PER_SECOND))
            .setScale(secondsScale, BigDecimal.ROUND_HALF_UP));
    result.append("s");
    return result.toString();
  }
}
