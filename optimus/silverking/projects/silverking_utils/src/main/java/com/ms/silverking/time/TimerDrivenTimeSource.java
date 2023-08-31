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

import java.util.TimerTask;

import com.ms.silverking.util.SafeTimer;
import com.ms.silverking.util.SafeTimerTask;

/**
 * A RelNanosAbsMillisTimeSource that utilizes a Timer class to provide very low-overhead time
 * reads. This timer is faster than SystemTimeSource, but is less accurate and more granular.
 */
public final class TimerDrivenTimeSource extends TimerTask implements RelNanosAbsMillisTimeSource {
  private final SafeTimer timer;
  private volatile long absTimeMillis;
  private volatile long relTimeNanos;

  static final long defaultPeriodMillis = 5;
  private static final String defaultTimerNameBase = "TimerDriveTimeSource_";

  private static String defaultTimerName() {
    return defaultTimerNameBase + System.currentTimeMillis();
  }

  public TimerDrivenTimeSource(SafeTimer timer, long periodMillis) {
    updateTimes();
    this.timer = timer;
    timer.scheduleAtFixedRate(new SafeTimerTask(this), 0, periodMillis);
  }

  public TimerDrivenTimeSource(SafeTimer timer) {
    this(timer, defaultPeriodMillis);
  }

  public TimerDrivenTimeSource(long periodMillis) {
    this(new SafeTimer(defaultTimerName(), true), periodMillis);
  }

  public TimerDrivenTimeSource() {
    this(defaultPeriodMillis);
  }

  public void stop() {
    timer.cancel();
  }

  @Override
  public long relTimeNanos() {
    return relTimeNanos;
  }

  @Override
  public long absTimeMillis() {
    return absTimeMillis;
  }

  @Override
  public int relMillisRemaining(long absDeadlineMillis) {
    return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineMillis, absTimeMillis());
  }

  @Override
  public String name() {
    return String.format(
        "TimerDrivenTimeSource(absTimeMillis=%d,relTimeNanos=%d)", absTimeMillis, relTimeNanos);
  }

  @Override
  public void run() {
    updateTimes();
  }

  private void updateTimes() {
    relTimeNanos = System.nanoTime();
    absTimeMillis = System.currentTimeMillis();
  }
}
