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
package com.ms.silverking.util;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Extends Timer to ensure that all called TimerTasks are wrapped in SafeTimerTasks.
 * This prevents the Timer from breaking due to a thrown exception in a TimerTask.
 */
public class SafeTimer extends Timer {
  public SafeTimer() {
  }

  public SafeTimer(boolean isDaemon) {
    super(isDaemon);
  }

  public SafeTimer(String name) {
    super(name);
  }

  public SafeTimer(String name, boolean isDaemon) {
    super(name, isDaemon);
  }

  /**
   * Schedules the specified task for execution after the specified delay.
   *
   * @param task  task to be scheduled.
   * @param delay delay in milliseconds before task is to be executed.
   * @throws IllegalArgumentException if <tt>delay</tt> is negative, or
   *                                  <tt>delay + System.currentTimeMillis()</tt> is negative
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled, or timer thread terminated.
   * @throws NullPointerException     if {@code task} is null
   */
  public void schedule(SafeTimerTask task, long delay) {
    super.schedule(task, delay);
  }

  public void schedule(TimerTask task, long delay) {
    if (task instanceof SafeTimerTask) {
      schedule((SafeTimerTask) task, delay);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }

  /**
   * Schedules the specified task for execution at the specified time.  If
   * the time is in the past, the task is scheduled for immediate execution.
   *
   * @param task task to be scheduled.
   * @param time time at which task is to be executed.
   * @throws IllegalArgumentException if <tt>time.getTime()</tt> is negative, or
   *                                  the timer task is not a SafeTimerTask
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled
   * @throws NullPointerException     if {@code task} or {@code time} is null
   */
  public void schedule(SafeTimerTask task, Date time) {
    super.schedule(task, time);
  }

  public void schedule(TimerTask task, Date time) {
    if (task instanceof SafeTimerTask) {
      schedule((SafeTimerTask) task, time);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }

  /**
   * Schedules the specified task for repeated <i>fixed-delay execution</i>,
   * beginning after the specified delay.  Subsequent executions take place
   * at approximately regular intervals separated by the specified period.
   *
   * <p>In fixed-delay execution, each execution is scheduled relative to
   * the actual execution time of the previous execution.  If an execution
   * is delayed for any reason (such as garbage collection or other
   * background activity), subsequent executions will be delayed as well.
   * In the long run, the frequency of execution will generally be slightly
   * lower than the reciprocal of the specified period (assuming the system
   * clock underlying <tt>Object.wait(long)</tt> is accurate).
   *
   * <p>Fixed-delay execution is appropriate for recurring activities
   * that require "smoothness."  In other words, it is appropriate for
   * activities where it is more important to keep the frequency accurate
   * in the short run than in the long run.  This includes most animation
   * tasks, such as blinking a cursor at regular intervals.  It also includes
   * tasks wherein regular activity is performed in response to human
   * input, such as automatically repeating a character as long as a key
   * is held down.
   *
   * @param task   task to be scheduled.
   * @param delay  delay in milliseconds before task is to be executed.
   * @param period time in milliseconds between successive task executions.
   * @throws IllegalArgumentException if {@code delay < 0}, or
   *                                  {@code delay + System.currentTimeMillis() < 0}, or
   *                                  {@code period <= 0} ,or the timer task is not a SafeTimerTask
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled, or timer thread terminated.
   * @throws NullPointerException     if {@code task} is null
   */
  public void schedule(SafeTimerTask task, long delay, long period) {
    super.schedule(task, delay, period);
  }

  public void schedule(TimerTask task, long delay, long period) {
    if (task instanceof SafeTimerTask) {
      schedule((SafeTimerTask) task, delay, period);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }

  /**
   * Schedules the specified task for repeated <i>fixed-delay execution</i>,
   * beginning at the specified time. Subsequent executions take place at
   * approximately regular intervals, separated by the specified period.
   *
   * <p>In fixed-delay execution, each execution is scheduled relative to
   * the actual execution time of the previous execution.  If an execution
   * is delayed for any reason (such as garbage collection or other
   * background activity), subsequent executions will be delayed as well.
   * In the long run, the frequency of execution will generally be slightly
   * lower than the reciprocal of the specified period (assuming the system
   * clock underlying <tt>Object.wait(long)</tt> is accurate).  As a
   * consequence of the above, if the scheduled first time is in the past,
   * it is scheduled for immediate execution.
   *
   * <p>Fixed-delay execution is appropriate for recurring activities
   * that require "smoothness."  In other words, it is appropriate for
   * activities where it is more important to keep the frequency accurate
   * in the short run than in the long run.  This includes most animation
   * tasks, such as blinking a cursor at regular intervals.  It also includes
   * tasks wherein regular activity is performed in response to human
   * input, such as automatically repeating a character as long as a key
   * is held down.
   *
   * @param task      task to be scheduled.
   * @param firstTime First time at which task is to be executed.
   * @param period    time in milliseconds between successive task executions.
   * @throws IllegalArgumentException if {@code firstTime.getTime() < 0}, or
   *                                  {@code period <= 0}
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled, or timer thread terminated.
   * @throws NullPointerException     if {@code task} or {@code firstTime} is null
   */
  public void schedule(SafeTimerTask task, Date firstTime, long period) {
    super.schedule(task, firstTime, period);
  }

  public void schedule(TimerTask task, Date firstTime, long period) {
    if (task instanceof SafeTimerTask) {
      schedule((SafeTimerTask) task, firstTime, period);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }

  /**
   * Schedules the specified task for repeated <i>fixed-rate execution</i>,
   * beginning after the specified delay.  Subsequent executions take place
   * at approximately regular intervals, separated by the specified period.
   *
   * <p>In fixed-rate execution, each execution is scheduled relative to the
   * scheduled execution time of the initial execution.  If an execution is
   * delayed for any reason (such as garbage collection or other background
   * activity), two or more executions will occur in rapid succession to
   * "catch up."  In the long run, the frequency of execution will be
   * exactly the reciprocal of the specified period (assuming the system
   * clock underlying <tt>Object.wait(long)</tt> is accurate).
   *
   * <p>Fixed-rate execution is appropriate for recurring activities that
   * are sensitive to <i>absolute</i> time, such as ringing a chime every
   * hour on the hour, or running scheduled maintenance every day at a
   * particular time.  It is also appropriate for recurring activities
   * where the total time to perform a fixed number of executions is
   * important, such as a countdown timer that ticks once every second for
   * ten seconds.  Finally, fixed-rate execution is appropriate for
   * scheduling multiple repeating timer tasks that must remain synchronized
   * with respect to one another.
   *
   * @param task   task to be scheduled.
   * @param delay  delay in milliseconds before task is to be executed.
   * @param period time in milliseconds between successive task executions.
   * @throws IllegalArgumentException if {@code delay < 0}, or
   *                                  {@code delay + System.currentTimeMillis() < 0}, or
   *                                  {@code period <= 0}
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled, or timer thread terminated.
   * @throws NullPointerException     if {@code task} is null
   */
  public void scheduleAtFixedRate(SafeTimerTask task, long delay, long period) {
    super.scheduleAtFixedRate(task, delay, period);
  }

  public void scheduleAtFixedRate(TimerTask task, long delay, long period) {
    if (task instanceof SafeTimerTask) {
      scheduleAtFixedRate((SafeTimerTask) task, delay, period);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }

  /**
   * Schedules the specified task for repeated <i>fixed-rate execution</i>,
   * beginning at the specified time. Subsequent executions take place at
   * approximately regular intervals, separated by the specified period.
   *
   * <p>In fixed-rate execution, each execution is scheduled relative to the
   * scheduled execution time of the initial execution.  If an execution is
   * delayed for any reason (such as garbage collection or other background
   * activity), two or more executions will occur in rapid succession to
   * "catch up."  In the long run, the frequency of execution will be
   * exactly the reciprocal of the specified period (assuming the system
   * clock underlying <tt>Object.wait(long)</tt> is accurate).  As a
   * consequence of the above, if the scheduled first time is in the past,
   * then any "missed" executions will be scheduled for immediate "catch up"
   * execution.
   *
   * <p>Fixed-rate execution is appropriate for recurring activities that
   * are sensitive to <i>absolute</i> time, such as ringing a chime every
   * hour on the hour, or running scheduled maintenance every day at a
   * particular time.  It is also appropriate for recurring activities
   * where the total time to perform a fixed number of executions is
   * important, such as a countdown timer that ticks once every second for
   * ten seconds.  Finally, fixed-rate execution is appropriate for
   * scheduling multiple repeating timer tasks that must remain synchronized
   * with respect to one another.
   *
   * @param task      task to be scheduled.
   * @param firstTime First time at which task is to be executed.
   * @param period    time in milliseconds between successive task executions.
   * @throws IllegalArgumentException if {@code firstTime.getTime() < 0} or
   *                                  {@code period <= 0}
   * @throws IllegalStateException    if task was already scheduled or
   *                                  cancelled, timer was cancelled, or timer thread terminated.
   * @throws NullPointerException     if {@code task} or {@code firstTime} is null
   */
  public void scheduleAtFixedRate(SafeTimerTask task, Date firstTime, long period) {
    super.scheduleAtFixedRate(task, firstTime, period);
  }

  public void scheduleAtFixedRate(TimerTask task, Date firstTime, long period) {
    if (task instanceof SafeTimerTask) {
      scheduleAtFixedRate((SafeTimerTask) task, firstTime, period);
    } else {
      throw new IllegalArgumentException("Only SafeTimerTasks can be scheduled from SafeTimer");
    }
  }
}
