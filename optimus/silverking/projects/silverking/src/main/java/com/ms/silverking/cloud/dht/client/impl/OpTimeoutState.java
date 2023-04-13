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
package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;

/**
 * Encapsulates state related to attempts, retries, and timeouts.
 */
class OpTimeoutState {
  private final AsyncOperation op;
  private final OpTimeoutController timeoutController;
  private final long startTimeMillis;
  private long attemptStartTimeMillis;
  private int curAttemptIndex;

  OpTimeoutState(AsyncOperation op, OpTimeoutController timeoutController, long startTimeMillis) {
    this.op = op;
    this.timeoutController = timeoutController;
    this.startTimeMillis = startTimeMillis;
    // initialize attempt 0
    attemptStartTimeMillis = startTimeMillis;
  }

  public int getCurRelTimeoutMillis() {
    return timeoutController.getRelativeTimeoutMillisForAttempt(op, curAttemptIndex);
  }

  boolean opHasTimedOut(long curTimeMillis) {
        /*
        System.out.printf("%d > %d || \n%d cur\n%d start\n%d to\n", 
                curAttemptIndex, timeoutController.getMaxAttempts(op), 
                curTimeMillis, startTimeMillis, startTimeMillis + timeoutController.getMaxRelativeTimeoutMillis(op));
        System.out.printf("%s || %s\n", 
                curAttempt>  timeoutController.getMaxAttempts(op), 
                curTimeMillis > startTimeMillis + timeoutController.getMaxRelativeTimeoutMillis(op));
        System.out.printf("curAttemptIndex %d timeoutController.getMaxAttempts(op) %d\tmaxRelativeTimeoutMillis %d\n", 
                curAttemptIndex, timeoutController.getMaxAttempts(op), timeoutController.getMaxRelativeTimeoutMillis
                (op));
        */
    return curTimeMillis > startTimeMillis + timeoutController.getMaxRelativeTimeoutMillis(op);
  }

  boolean attemptHasTimedOut(long curTimeMillis) {
    return curTimeMillis > attemptStartTimeMillis + timeoutController.getRelativeTimeoutMillisForAttempt(op,
        curAttemptIndex);
  }

  boolean newAttemptAllowed(AsyncOperation op) {
    return curAttemptIndex < timeoutController.getMaxAttempts(op);
  }

  boolean retryOnExclusionChange(long curTimeMillis) {
    return curTimeMillis > attemptStartTimeMillis + timeoutController.getRelativeExclusionChangeRetryMillisForAttempt(
        op, curAttemptIndex);
  }

  void newAttempt(long curTimeMillis) {
    ++curAttemptIndex;
    attemptStartTimeMillis = curTimeMillis;
  }

  @Override
  public String toString() {
    return startTimeMillis + ":" + curAttemptIndex + ":" + timeoutController;
  }
}
