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

/**
 * Stopwatch functionality interface. Using this interface is preferred over using concrete
 * implementations, though there is a very small performance penalty involved.
 *
 * <p>Applications that require extreme performance may consider using a concrete implementation
 * directly.
 *
 * <p>By convention, concrete insantiations of this class are running upon creation with a start
 * time equal to the instantiation time.
 *
 * <p>Elapsed time is defined as elapsed time = stop time - start time. This is only defined when
 * the state is stopped.
 *
 * <p>Split time is defined as split time = current time - start time.
 */
public interface Stopwatch {
  /** The state of a stopwatch */
  public enum State {
    running,
    stopped
  };

  /* control methods */

  /**
   * Set the start time to the current time, and start running. Must be stopped when calling this
   * method.
   */
  public void start();

  /**
   * Set the stop time to the current time, and stop running. Must be running when calling this
   * method.
   */
  public void stop();

  /** Set the start time to the current time. */
  public void reset();

  /* elapsed time methods */

  /**
   * Get the elapsed StopWatch time in nanoseconds. Must be stopped when calling this method.
   *
   * @return the elapsed StopWatch time in nanoseconds
   */
  public long getElapsedNanos();

  /**
   * Get the elapsed StopWatch time in milliseconds (as a long.) Must be stopped when calling this
   * method.
   *
   * @return the elapsed StopWatch time in milliseconds (as a long)
   */
  public long getElapsedMillisLong();

  /**
   * Get the elapsed StopWatch time in milliseconds. Must be stopped when calling this method.
   *
   * @return the elapsed StopWatch time in milliseconds
   */
  public int getElapsedMillis();

  /**
   * Get the elapsed StopWatch time in seconds. Must be stopped when calling this method.
   *
   * @return the elapsed StopWatch time in seconds
   */
  public double getElapsedSeconds();

  /**
   * Get the elapsed StopWatch time in seconds (as a BigDecimal.) Must be stopped when calling this
   * method.
   *
   * @return the elapsed StopWatch time in seconds (as a BigDecimal)
   */
  public BigDecimal getElapsedSecondsBD();

  /* split time methods */

  /**
   * Get the split StopWatch time in nanoseconds.
   *
   * @return the split StopWatch time in nanoseconds
   */
  public long getSplitNanos();

  /**
   * Get the split StopWatch time in milliseconds.
   *
   * @return the split StopWatch time in milliseconds
   */
  public long getSplitMillisLong();

  /**
   * Get the split StopWatch time in milliseconds.
   *
   * @return the split StopWatch time in milliseconds
   */
  public int getSplitMillis();

  /**
   * Get the split StopWatch time in seconds.
   *
   * @return the split StopWatch time in seconds
   */
  public double getSplitSeconds();

  /**
   * Get the split StopWatch time in seconds (as a BigDecimal.)
   *
   * @return the split StopWatch time in seconds (as a BigDecimal)
   */
  public BigDecimal getSplitSecondsBD();

  /* misc. methods */

  /**
   * Get the name of this StopWatch.
   *
   * @return the name of this StopWatch
   */
  public String getName();

  /**
   * Get the State of this StopWatch.
   *
   * @return the State of this StopWatch
   */
  public State getState();

  /**
   * Check if the State of this StopWatch, is running.
   *
   * @return true if State of this StopWatch is running
   */
  public boolean isRunning();

  /**
   * Check if the State of this StopWatch, is stopped.
   *
   * @return true if State of this StopWatch is stopped
   */
  public boolean isStopped();

  /**
   * Get a string representation of this StopWatch including the elapsed time. Must be stopped when
   * calling this method.
   *
   * @return a string representation of this StopWatch including the elapsed time
   */
  public String toStringElapsed();

  /**
   * Get a string representation of this StopWatch including the split time.
   *
   * @return a string representation of this StopWatch including the split time
   */
  public String toStringSplit();
}
