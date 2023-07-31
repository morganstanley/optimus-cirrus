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
package com.ms.silverking.time;

import java.math.BigDecimal;
import java.util.concurrent.locks.Condition;

public interface Timer extends Stopwatch {
  /**
   * True iff the timer has expired
   *
   * @return true iff the timer has expired
   */
  public boolean hasExpired();

  /**
   * Wait uninterruptibly for expiration
   */
  public void waitForExpiration();

  /* Remaining time methods */

  /**
   * Get the remaining time in nanoseconds.
   *
   * @return the remaining time in nanoseconds
   */
  public long getRemainingNanos();

  /**
   * Get the remaining time in milliseconds (as a long.)
   *
   * @return the remaining time in milliseconds (as a long)
   */
  public long getRemainingMillisLong();

  /**
   * Get the remaining time in milliseconds.
   *
   * @return the remaining time in milliseconds
   */
  public int getRemainingMillis();

  /**
   * Get the remaining time in seconds.
   *
   * @return the remaining time in seconds
   */
  public double getRemainingSeconds();

  /**
   * Get the remaining time in seconds (as a BigDecimal.)
   *
   * @return the remaining time in seconds (as a BigDecimal)
   */
  public BigDecimal getRemainingSecondsBD();

  /* TimeLimit time methods */

  /**
   * Get the TimeLimit time in nanoseconds.
   *
   * @return the TimeLimit time in nanoseconds
   */
  public long getTimeLimitNanos();

  /**
   * Get the TimeLimit time in milliseconds (as a long.)
   *
   * @return the TimeLimit time in milliseconds (as a long)
   */
  public long getTimeLimitMillisLong();

  /**
   * Get the TimeLimit time in milliseconds.
   *
   * @return the TimeLimit time in milliseconds
   */
  public int getTimeLimitMillis();

  /**
   * Get the TimeLimit time in seconds.
   *
   * @return the TimeLimit time in seconds
   */
  public double getTimeLimitSeconds();

  /**
   * Get the TimeLimit time in seconds (as a BigDecimal.)
   *
   * @return the TimeLimit time in seconds (as a BigDecimal)
   */
  public BigDecimal getTimeLimitSecondsBD();

  /**
   * Wait on the provided Condition for the remaining time
   *
   * @param cv TODO (OPTIMUS-0000): describe
   * @return {@code false} if the waiting time detectably elapsed
   * before return from the method, else {@code true}
   * @throws InterruptedException TODO (OPTIMUS-0000): describe
   */
  public boolean await(Condition cv) throws InterruptedException;
}
