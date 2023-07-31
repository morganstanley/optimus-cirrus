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

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * An OpTimeoutController that computes timeouts based on operation size.
 * Specifically, each attempt timeout is computed as:
 * constantTimeMillis + itemTimeMillis * opSizeInKeys. For non-keyed
 * operations, the nonKeyedOpMaxRelTimeout_ms is used.
 */
@OmitGeneration
public class OpSizeBasedTimeoutController implements OpTimeoutController {
  private final int maxAttempts;
  private final int constantTime_ms;
  private final int itemTime_ms;
  //private final int   keyedOpMaxRelTimeout_ms; // Waiting for C++ client change to include this, for no using
  // nonKeyed as a workaround
  private final int nonKeyedOpMaxRelTimeout_ms;
  private final int exclusionChangeRetryInterval_ms;

  private static final int minTransferRate_bps = 275 * 1000 * 1000;
  private static final int minTransferRate_Bps = minTransferRate_bps / 8;
  static final int defaultItemTime_ms = Math.toIntExact(
      (1000L * DHTConstants.defaultFragmentationThreshold) / minTransferRate_Bps);

  // For testing
  //private static final int    defaultConstantTime_ms = 10 * 1000;
  //private static final int    defaultConstantTime_ms = 20 * 1000;

  // For production
  static final int defaultConstantTime_ms = 5 * 60 * 1000;
  static final int defaultExclusionChangeRetryIntervalMS = 5 * 1000;

  static final int defaultMaxAttempts = 4;

  static final OpSizeBasedTimeoutController template = new OpSizeBasedTimeoutController();

  //    static final int    defaultKeyedOpMaxRelTimeout_ms    = 25 * 60 * 1000;    // this is actually unused right
  //    now, in the code, we only have keyed-ops that are available for creation
  static final int defaultNonKeyedOpMaxRelTimeout_ms = 25 * 60 * 1000;

  static {
    try {
      ObjectDefParser2.addParser(template);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * Temporarily removing keyedOpMaxRelTimeout_ms. Add back in when we have time to
   * push this into C++.
   */

  /**
   * Construct a fully-specified OpSizeBasedTimeoutController
   *
   * @param maxAttempts                     maximum number of attempts
   * @param constantTimeMillis              constant time in milliseconds
   * @param itemTimeMillis                  per-item time in milliseconds
   * @param nonKeyedOpMaxRelTimeoutMillis   maximum relative timeout in milliseconds
   * @param exclusionChangeRetryInterval_ms TODO (OPTIMUS-0000): describe
   */
  public OpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis,
      /*int keyedOpMaxRelTimeoutMillis,*/ int nonKeyedOpMaxRelTimeoutMillis, int exclusionChangeRetryInterval_ms) {
    Util.checkAttempts(maxAttempts);
    this.maxAttempts = maxAttempts;
    this.constantTime_ms = constantTimeMillis;
    this.itemTime_ms = itemTimeMillis;
    //this.keyedOpMaxRelTimeout_ms = keyedOpMaxRelTimeoutMillis;
    this.nonKeyedOpMaxRelTimeout_ms = nonKeyedOpMaxRelTimeoutMillis;
    this.exclusionChangeRetryInterval_ms = exclusionChangeRetryInterval_ms;
  }

  /**
   * Construct an OpSizeBasedTimeoutController using default parameters
   */
  public OpSizeBasedTimeoutController() {
    this(defaultMaxAttempts, defaultConstantTime_ms, defaultItemTime_ms, /*defaultKeyedOpMaxRelTimeout_ms, */
        defaultNonKeyedOpMaxRelTimeout_ms, defaultExclusionChangeRetryIntervalMS);
  }

  @Override
  public int getMaxAttempts(AsyncOperation op) {
    return maxAttempts;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex) {
    if (op instanceof AsyncKeyedOperation) {
      return computeTimeout(((AsyncKeyedOperation<?>) op).getNumKeys());
    } else {
      return nonKeyedOpMaxRelTimeout_ms;
    }
  }

  @Override
  public long getRelativeExclusionChangeRetryMillisForAttempt(AsyncOperation op, int curAttemptIndex) {
    return exclusionChangeRetryInterval_ms;
  }

  @Override
  public int getMaxRelativeTimeoutMillis(AsyncOperation op) {
    if (op instanceof AsyncKeyedOperation) {
      return getRelativeTimeoutMillisForAttempt(op, maxAttempts) * maxAttempts;
    } else {
      return nonKeyedOpMaxRelTimeout_ms;
    }
  }

  int computeTimeout(int numItems) {
    //return Math.min(constantTime_ms + numItems * itemTime_ms, keyedOpMaxRelTimeout_ms);
    return Math.min(constantTime_ms + numItems * itemTime_ms, nonKeyedOpMaxRelTimeout_ms);
  }

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified maxAttempts.
   *
   * @param maxAttempts maxAttempts for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  public OpSizeBasedTimeoutController maxAttempts(int maxAttempts) {
    return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/
        nonKeyedOpMaxRelTimeout_ms, exclusionChangeRetryInterval_ms);
  }

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified constantTimeMillis.
   *
   * @param constantTimeMillis constantTimeMillis for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  public OpSizeBasedTimeoutController constantTimeMillis(int constantTimeMillis) {
    return new OpSizeBasedTimeoutController(maxAttempts, constantTimeMillis, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/
        nonKeyedOpMaxRelTimeout_ms, exclusionChangeRetryInterval_ms);
  }

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified itemTimeMillis.
   *
   * @param itemTimeMillis itemTimeMillis for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  public OpSizeBasedTimeoutController itemTimeMillis(int itemTimeMillis) {
    return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTimeMillis, /*keyedOpMaxRelTimeout_ms,*/
        nonKeyedOpMaxRelTimeout_ms, exclusionChangeRetryInterval_ms);
  }

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified maxRelTimeoutMillis.
   * @param maxRelTimeoutMillis maxRelTimeoutMillis for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  //public OpSizeBasedTimeoutController keyedOpMaxRelTimeoutMillis(int keyedOpMaxRelTimeout_ms) {
  //return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/
  // nonKeyedOpMaxRelTimeout_ms);
  //}

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified maxRelTimeoutMillis.
   * @param maxRelTimeoutMillis maxRelTimeoutMillis for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  //    public OpSizeBasedTimeoutController nonKeyedOpMaxRelTimeoutMillis(int nonKeyedOpMaxRelTimeout_ms) {
  //        return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms,
  //        /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
  //    }

  /**
   * Create a new OpSizeBasedTimeoutController exactly like this instance, but
   * with the specified maxRelTimeoutMillis.
   *
   * @param maxRelTimeoutMillis maxRelTimeoutMillis for the new instance
   * @return the specified OpSizeBasedTimeoutController
   */
  public OpSizeBasedTimeoutController maxRelTimeoutMillis(int maxRelTimeoutMillis) {
    return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, maxRelTimeoutMillis,
        exclusionChangeRetryInterval_ms);
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(maxAttempts) ^ Integer.hashCode(constantTime_ms) ^ Integer.hashCode(itemTime_ms) ^
        /*Integer.hashCode(keyedOpMaxRelTimeout_ms) ^*/ Integer.hashCode(nonKeyedOpMaxRelTimeout_ms) ^ Integer.hashCode(
        exclusionChangeRetryInterval_ms);
  }

  @Override
  public boolean equals(Object o) {
    OpSizeBasedTimeoutController other;

    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    other = (OpSizeBasedTimeoutController) o;
    return maxAttempts == other.maxAttempts && constantTime_ms == other.constantTime_ms && itemTime_ms == other.itemTime_ms
        //&& keyedOpMaxRelTimeout_ms == o.keyedOpMaxRelTimeout_ms
        && nonKeyedOpMaxRelTimeout_ms == other.nonKeyedOpMaxRelTimeout_ms && exclusionChangeRetryInterval_ms == other.exclusionChangeRetryInterval_ms;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * Parse a definition
   *
   * @param def object definition
   * @return a parsed OpSizeBasedTimeoutController instance
   */
  public static OpSizeBasedTimeoutController parse(String def) {
    return ObjectDefParser2.parse(OpSizeBasedTimeoutController.class, def);
  }
}
