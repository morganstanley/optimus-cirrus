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
package com.ms.silverking.net.async;

import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * A simple NewConnectionTimeoutController that ignores destination addresses and accepts
 * maxAttempts, attemptRelativeTimeoutMillis, and maxRelativeTimeoutMillis
 */
public class SimpleNewConnectionTimeoutController implements NewConnectionTimeoutController {
  private final int maxAttempts;
  private final int attemptRelativeTimeoutMillis;
  private final int maxRelativeTimeoutMillis;

  private static final int defaultMaxAttempts = 4;
  private static final int defaultAttemptRelativeTimeoutMillis = 2 * 60 * 1000;
  private static final int defaultMaxRelativeTimeoutMillis = 8 * 60 * 1000;

  private static final SimpleNewConnectionTimeoutController template =
      new SimpleNewConnectionTimeoutController(
          defaultMaxAttempts, defaultAttemptRelativeTimeoutMillis, defaultMaxRelativeTimeoutMillis);

  static {
    ObjectDefParser2.addParser(template);
  }

  public SimpleNewConnectionTimeoutController(
      int maxAttempts, int attemptRelativeTimeoutMillis, int maxRelativeTimeoutMillis) {
    this.maxAttempts = maxAttempts;
    this.attemptRelativeTimeoutMillis = attemptRelativeTimeoutMillis;
    this.maxRelativeTimeoutMillis = maxRelativeTimeoutMillis;
  }

  /**
   * Create a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   * maxAttempts.
   *
   * @return a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   *     maxAttempts
   */
  public SimpleNewConnectionTimeoutController maxAttempts(int maxAttempts) {
    return new SimpleNewConnectionTimeoutController(
        maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   * attemptRelativeTimeoutMillis.
   *
   * @return a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   *     attemptRelativeTimeoutMillis
   */
  public SimpleNewConnectionTimeoutController attemptRelativeTimeoutMillis(
      int attemptRelativeTimeoutMillis) {
    return new SimpleNewConnectionTimeoutController(
        maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis.
   *
   * @return a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis
   */
  public SimpleNewConnectionTimeoutController maxRelativeTimeoutMillis(
      int maxRelativeTimeoutMillis) {
    return new SimpleNewConnectionTimeoutController(
        maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis);
  }

  @Override
  public int getMaxAttempts(AddrAndPort addrAndPort) {
    return maxAttempts;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex) {
    return attemptRelativeTimeoutMillis;
  }

  @Override
  public int getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int hashCode() {
    return maxAttempts ^ attemptRelativeTimeoutMillis ^ maxRelativeTimeoutMillis;
  }

  @Override
  public boolean equals(Object other) {
    SimpleNewConnectionTimeoutController o;

    o = (SimpleNewConnectionTimeoutController) other;
    return maxAttempts == o.maxAttempts
        && attemptRelativeTimeoutMillis == o.attemptRelativeTimeoutMillis
        && maxRelativeTimeoutMillis == o.maxRelativeTimeoutMillis;
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
  public static SimpleNewConnectionTimeoutController parse(String def) {
    return ObjectDefParser2.parse(SimpleNewConnectionTimeoutController.class, def);
  }
}
