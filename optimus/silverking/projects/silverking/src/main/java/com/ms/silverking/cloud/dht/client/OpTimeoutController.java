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

/**
 * Controls timeout and retry behavior for operations. AsyncOperation is used here since all
 * operations are asynchronous internally.
 */
@OmitGeneration
public interface OpTimeoutController {
  /**
   * Return the maximum number of times that this operation should be attempted.
   *
   * @param op the relevant operation
   * @return the maximum number of times that this operation should be attempted
   */
  public int getMaxAttempts(AsyncOperation op);

  /**
   * Return the relative timeout in milliseconds for the given attempt.
   *
   * @param op the relevant operation
   * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts
   *     - 1.
   * @return the relative timeout in milliseconds for the given attempt
   */
  public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex);

  /**
   * Return the maximum relative timeout for the given operation. Once this timeout is triggered, no
   * further attempts of this operation will be made irrespective of the individual attempt timeout
   * or the maximum number of attempts.
   *
   * @param op the relevant operation
   * @return the maximum relative timeout for the given operation
   */
  public int getMaxRelativeTimeoutMillis(AsyncOperation op);

  /**
   * Return the relative retry interval in milliseconds for the given attempt for the case where the
   * exclusion set has changed.
   *
   * @param op the relevant operation
   * @param curAttemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of
   *     attempts - 1.
   * @return the relative timeout in milliseconds for the given attempt
   */
  public long getRelativeExclusionChangeRetryMillisForAttempt(
      AsyncOperation op, int curAttemptIndex);

  public static final int min_maxAttempts = 1;
  public static final int minInitialTimeout_ms = 5;
}
