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

import com.ms.silverking.text.ObjectDefParser2;

/**
 * OpTimeoutController implementation that supports a specified maximum number of attempts as well
 * as a single relative timeout for the operation.
 */
public class SimpleTimeoutController implements OpTimeoutController {
  private final int maxAttempts;
  private final int maxRelativeTimeoutMillis;

  static final int defaultMaxAttempts = 5;
  static final int defaultMaxRelativeTimeoutMillis = 2 * 60 * 1000;

  static final SimpleTimeoutController template = new SimpleTimeoutController();

  static {
    ObjectDefParser2.addParser(template);
  }

  /**
   * Construct a SingleTimeout instance with the specified number of attempts and the specified
   * relative timeout.
   *
   * @param maxAttempts maximum number of attempts
   * @param maxRelativeTimeoutMillis relative timeout in milliseconds
   */
  public SimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis) {
    Util.checkAttempts(maxAttempts);
    this.maxAttempts = maxAttempts;
    this.maxRelativeTimeoutMillis = maxRelativeTimeoutMillis;
  }

  private SimpleTimeoutController() {
    this(defaultMaxAttempts, defaultMaxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleTimeoutController like this instance, but with a new maxAttempts.
   *
   * @param maxAttempts maximum number of attempts
   * @return a SimpleTimeoutController like this instance, but with a new maxAttempts
   */
  public SimpleTimeoutController maxAttempts(int maxAttempts) {
    return new SimpleTimeoutController(maxAttempts, maxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis.
   *
   * @param maxRelativeTimeoutMillis maximum relative timeout for the given operation
   * @return a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis
   */
  public SimpleTimeoutController maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis) {
    return new SimpleTimeoutController(maxAttempts, maxRelativeTimeoutMillis);
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  @Override
  public int getMaxAttempts(AsyncOperation op) {
    return maxAttempts;
  }

  public int getMaxRelativeTimeoutMillis() {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public long getRelativeExclusionChangeRetryMillisForAttempt(
      AsyncOperation op, int curAttemptIndex) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int getMaxRelativeTimeoutMillis(AsyncOperation op) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int hashCode() {
    return maxAttempts ^ maxRelativeTimeoutMillis;
  }

  @Override
  public boolean equals(Object o) {
    SimpleTimeoutController other;

    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    other = (SimpleTimeoutController) o;
    return maxAttempts == other.maxAttempts
        && maxRelativeTimeoutMillis == other.maxRelativeTimeoutMillis;
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
  public static SimpleTimeoutController parse(String def) {
    return ObjectDefParser2.parse(SimpleTimeoutController.class, def);
  }
}
