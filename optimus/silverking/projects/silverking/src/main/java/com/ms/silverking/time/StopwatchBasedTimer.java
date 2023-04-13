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

import static com.ms.silverking.time.TimeUtils.checkTooManyMillis;
import static com.ms.silverking.time.TimeUtils.nanos2millisLong;
import static com.ms.silverking.time.TimeUtils.nanos2seconds;
import static com.ms.silverking.time.TimeUtils.nanos2secondsBD;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class StopwatchBasedTimer implements Timer {
  private final Stopwatch sw;
  private final long limitNanos;

  private StopwatchBasedTimer(Stopwatch sw, long limitNanos) {
    this.sw = sw;
    this.limitNanos = limitNanos;
  }

  public StopwatchBasedTimer(Stopwatch sw, TimeUnit unit, long limit) {
    this(sw, TimeUnit.NANOSECONDS.convert(limit, unit));
  }

  @Override
  public boolean hasExpired() {
    return getRemainingNanos() <= 0;
  }

  @Override
  public void waitForExpiration() {
    while (!hasExpired()) {
      try {
        Thread.sleep(getRemainingMillisLong());
      } catch (InterruptedException ie) {
      }
    }
  }

  @Override
  public long getRemainingNanos() {
    return Math.max(limitNanos - sw.getSplitNanos(), 0);
  }

  @Override
  public long getRemainingMillisLong() {
    return nanos2millisLong(getRemainingNanos());
  }

  @Override
  public int getRemainingMillis() {
    long remainingMillis;

    remainingMillis = getRemainingMillisLong();
    checkTooManyMillis(remainingMillis);

    return (int) remainingMillis;
  }

  @Override
  public double getRemainingSeconds() {
    return nanos2seconds(getRemainingNanos());
  }

  @Override
  public BigDecimal getRemainingSecondsBD() {
    return nanos2secondsBD(getRemainingNanos());
  }

  @Override
  public long getTimeLimitNanos() {
    return limitNanos;
  }

  @Override
  public long getTimeLimitMillisLong() {
    return nanos2millisLong(getTimeLimitNanos());
  }

  @Override
  public int getTimeLimitMillis() {
    long timeLimitMillis;

    timeLimitMillis = getTimeLimitMillisLong();
    checkTooManyMillis(timeLimitMillis);

    return (int) timeLimitMillis;
  }

  @Override
  public double getTimeLimitSeconds() {
    return nanos2seconds(getTimeLimitNanos());
  }

  @Override
  public BigDecimal getTimeLimitSecondsBD() {
    return nanos2secondsBD(getTimeLimitNanos());
  }

  // StopwatchWrapper methods

  @Override
  public void start() {
    sw.start();
  }

  @Override
  public void stop() {
    sw.stop();
  }

  @Override
  public void reset() {
    sw.reset();
  }

  @Override
  public long getElapsedNanos() {
    return sw.getElapsedNanos();
  }

  @Override
  public long getElapsedMillisLong() {
    return sw.getElapsedMillisLong();
  }

  @Override
  public int getElapsedMillis() {
    return sw.getElapsedMillis();
  }

  @Override
  public double getElapsedSeconds() {
    return sw.getElapsedSeconds();
  }

  @Override
  public BigDecimal getElapsedSecondsBD() {
    return sw.getElapsedSecondsBD();
  }

  @Override
  public long getSplitNanos() {
    return sw.getSplitNanos();
  }

  @Override
  public long getSplitMillisLong() {
    return sw.getSplitMillisLong();
  }

  @Override
  public int getSplitMillis() {
    return sw.getSplitMillis();
  }

  @Override
  public double getSplitSeconds() {
    return sw.getSplitSeconds();
  }

  @Override
  public BigDecimal getSplitSecondsBD() {
    return sw.getSplitSecondsBD();
  }

  @Override
  public String getName() {
    return sw.getName();
  }

  @Override
  public State getState() {
    return sw.getState();
  }

  @Override
  public boolean isRunning() {
    return sw.isRunning();
  }

  @Override
  public boolean isStopped() {
    return sw.isStopped();
  }

  @Override
  public String toStringElapsed() {
    return sw.toStringElapsed() + ":" + getTimeLimitSeconds();
  }

  @Override
  public String toStringSplit() {
    return sw.toStringSplit() + ":" + getTimeLimitSeconds();
  }

  @Override
  public String toString() {
    return toStringSplit();
  }

  @Override
  public boolean await(Condition cv) throws InterruptedException {
    return cv.await(getRemainingMillisLong(), TimeUnit.MILLISECONDS);
  }
}
