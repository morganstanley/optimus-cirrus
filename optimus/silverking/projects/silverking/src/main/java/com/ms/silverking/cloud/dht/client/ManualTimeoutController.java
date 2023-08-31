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
package com.ms.silverking.cloud.dht.client;

import java.util.Arrays;

import com.ms.silverking.text.ObjectDefParser2;

/** OpTimeoutController implementation that supports manual specification of timeouts */
public class ManualTimeoutController implements OpTimeoutController {
  private final int[] attemptTimeoutsMillis;
  private final int[] attemptExclusionChangeTimeoutsMillis;
  private final int maxRelativeTimeoutMillis;

  static final int defaultMaxAttempts = 5;
  static final int defaultAttemptTimeoutMillis = 2 * 60 * 1000;
  static final int defaultAttemptExclusionChangeTimeoutMillis = 2 * 1000;

  static final ManualTimeoutController template =
      ManualTimeoutController.createWithFixedTimeouts(
          defaultMaxAttempts,
          defaultAttemptTimeoutMillis,
          defaultAttemptExclusionChangeTimeoutMillis);

  static {
    ObjectDefParser2.addParser(template);
  }

  /**
   * Construct a ManualTimeoutController instance with fully specified timeouts
   *
   * @param maxRelativeTimeoutMillis relative timeout in milliseconds
   */
  public ManualTimeoutController(
      int[] attemptTimeoutsMillis,
      int[] attemptExclusionChangeTimeoutsMillis,
      int maxRelativeTimeoutMillis) {
    Util.checkAttempts(attemptTimeoutsMillis.length);
    if (attemptTimeoutsMillis.length != attemptExclusionChangeTimeoutsMillis.length) {
      throw new RuntimeException(
          "attemptTimeoutsMillis.length != attemptExclusionChangeTimeoutsMillis.length");
    }
    this.attemptTimeoutsMillis = attemptTimeoutsMillis;
    this.attemptExclusionChangeTimeoutsMillis = attemptExclusionChangeTimeoutsMillis;
    this.maxRelativeTimeoutMillis = maxRelativeTimeoutMillis;
  }

  public static ManualTimeoutController createWithFixedTimeouts(
      int maxAttempts, int attemptTimeoutMillis, int attemptExclusionChangeTimeoutMillis) {
    int[] attemptTimeoutsMillis;
    int[] attemptExclusionChangeTimeoutsMillis;

    if (maxAttempts <= 0) {
      throw new RuntimeException("maxAttempts <= 0");
    }
    if (attemptTimeoutMillis < attemptExclusionChangeTimeoutMillis) {
      throw new RuntimeException("attemptTimeoutMillis < attemptExclusionChangeTimeoutMillis");
    }
    attemptTimeoutsMillis = new int[maxAttempts];
    attemptExclusionChangeTimeoutsMillis = new int[maxAttempts];
    Arrays.fill(attemptTimeoutsMillis, attemptTimeoutMillis);
    Arrays.fill(attemptExclusionChangeTimeoutsMillis, attemptExclusionChangeTimeoutMillis);
    return new ManualTimeoutController(
        attemptTimeoutsMillis,
        attemptExclusionChangeTimeoutsMillis,
        maxAttempts * attemptTimeoutMillis);
  }

  @Override
  public int getMaxAttempts(AsyncOperation op) {
    return attemptTimeoutsMillis.length;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int curAttemptIndex) {
    return attemptTimeoutsMillis[curAttemptIndex];
  }

  @Override
  public long getRelativeExclusionChangeRetryMillisForAttempt(
      AsyncOperation op, int curAttemptIndex) {
    return attemptExclusionChangeTimeoutsMillis[curAttemptIndex];
  }

  @Override
  public int getMaxRelativeTimeoutMillis(AsyncOperation op) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(attemptTimeoutsMillis)
        ^ Arrays.hashCode(attemptExclusionChangeTimeoutsMillis)
        ^ maxRelativeTimeoutMillis;
  }

  @Override
  public boolean equals(Object o) {
    ManualTimeoutController other;

    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    other = (ManualTimeoutController) o;
    return maxRelativeTimeoutMillis == other.maxRelativeTimeoutMillis
        && Arrays.equals(attemptTimeoutsMillis, ((ManualTimeoutController) o).attemptTimeoutsMillis)
        && Arrays.equals(
            attemptExclusionChangeTimeoutsMillis, attemptExclusionChangeTimeoutsMillis);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * Parse a definition
   *
   * @param def object definition
   * @return a parsed instance
   */
  public static ManualTimeoutController parse(String def) {
    return ObjectDefParser2.parse(ManualTimeoutController.class, def);
  }
}
