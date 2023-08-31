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

/** Controls retry and timeout behavior of new connections */
public interface NewConnectionTimeoutController {
  /**
   * Return the maximum number of times that this connection establishment should be attempted.
   *
   * @param addrAndPort the target of the connection being established
   * @return the maximum number of times that this connection should be attempted
   */
  public int getMaxAttempts(AddrAndPort addrAndPort);

  /**
   * Return the relative timeout in milliseconds for the given attempt.
   *
   * @param addrAndPort the target of the connection being established
   * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts
   *     - 1.
   * @return the relative timeout in milliseconds for the given attempt
   */
  public int getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex);

  /**
   * Return the maximum relative timeout for the given connection establishment. Once this timeout
   * is triggered, no further attempts will be made irrespective of the individual attempt timeout
   * or the maximum number of attempts.
   *
   * @param addrAndPort the target of the connection being established
   * @return the maximum relative timeout for the given operation
   */
  public int getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort);
}
