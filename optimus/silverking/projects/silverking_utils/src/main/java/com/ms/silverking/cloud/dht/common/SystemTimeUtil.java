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
package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;
import com.ms.silverking.util.SafeTimer;

public class SystemTimeUtil {
  private static final int timerDrivenTimeSourceResolutionMS = 5;
  private static final String timeSourceTimerName = "TimeSourceTimer";
  private static final long nanoOriginTimeInMillis = 946684800000L;

  /**
   * Time source that returns time from Java system time calls. Absolute nanos times are based on
   * elapsed nanoseconds since midnight January 1, 2000.
   */
  public static final SystemTimeSource skSystemTimeSource =
      SystemTimeSource.createWithMillisOrigin(nanoOriginTimeInMillis);

  /** Time driven time source for obtaining granular time with extremely low-overhead */
  public static final TimerDrivenTimeSource timerDrivenTimeSource =
      new TimerDrivenTimeSource(
          new SafeTimer(timeSourceTimerName, true), timerDrivenTimeSourceResolutionMS);

  public static long systemTimeNanosToEpochMillis(long nanos) {
    return (nanos / 1_000_000) + nanoOriginTimeInMillis;
  }
}
