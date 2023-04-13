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

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Parent class of all OpTimeoutControllers
 * for WaitFor operations. For these operations, the only parameters that
 * may be specified are the internal retry intervals. All other
 * parameters are either implicitly or explicitly specified in
 * the WaitOptions for the operation.
 */
@OmitGeneration
public class WaitForTimeoutController implements OpTimeoutController {
  private final int internalRetryIntervalSeconds;
  private final int internalExclusionChangeRetryIntervalSeconds;

  // FIXME - internal retries should be specified in ms

  static final int defaultInternalRetryIntervalSeconds = 20;
  static final int defaultExclusionChangeInternalRetryIntervalSeconds = 2;

  static final WaitForTimeoutController template = new WaitForTimeoutController();

  static {
    ObjectDefParser2.addParser(template);
  }

  public WaitForTimeoutController(int internalRetryIntervalSeconds, int internalExclusionChangeRetryIntervalSeconds) {
    this.internalRetryIntervalSeconds = internalRetryIntervalSeconds;
    this.internalExclusionChangeRetryIntervalSeconds = internalExclusionChangeRetryIntervalSeconds;
  }

  public WaitForTimeoutController(int internalRetryIntervalSeconds) {
    this(internalRetryIntervalSeconds, defaultExclusionChangeInternalRetryIntervalSeconds);
  }

  public WaitForTimeoutController() {
    this(defaultInternalRetryIntervalSeconds, defaultExclusionChangeInternalRetryIntervalSeconds);
  }

  @Override
  public int getMaxAttempts(AsyncOperation op) {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex) {
    return internalRetryIntervalSeconds * 1000;
  }

  @Override
  public long getRelativeExclusionChangeRetryMillisForAttempt(AsyncOperation op, int curAttemptIndex) {
    return internalExclusionChangeRetryIntervalSeconds * 1000;
  }

  @Override
  public final int getMaxRelativeTimeoutMillis(AsyncOperation op) {
    AsyncRetrieval asyncRetrieval;
    WaitOptions waitOptions;

    asyncRetrieval = (AsyncRetrieval) op;
    waitOptions = (WaitOptions) asyncRetrieval.getRetrievalOptions();
    if (waitOptions.getTimeoutSeconds() == Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) TimeUnit.MILLISECONDS.convert(waitOptions.getTimeoutSeconds(), TimeUnit.SECONDS);
    }
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(internalRetryIntervalSeconds) ^ Integer.hashCode(
        internalExclusionChangeRetryIntervalSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    WaitForTimeoutController other;
    other = (WaitForTimeoutController) o;
    return internalRetryIntervalSeconds == other.internalRetryIntervalSeconds && this.internalExclusionChangeRetryIntervalSeconds == other.internalExclusionChangeRetryIntervalSeconds;
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
  public static WaitForTimeoutController parse(String def) {
    return ObjectDefParser2.parse(WaitForTimeoutController.class, def);
  }
}
