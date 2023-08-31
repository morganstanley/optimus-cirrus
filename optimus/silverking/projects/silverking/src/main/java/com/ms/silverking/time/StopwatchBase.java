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

/**
 * Provides the core of a basic Stopwatch implementation. State stored by this abstract class is
 * restricted to start and stop times to keep memory utilization to a minimum. Concrete classes can
 * decide how much additional state is desired.
 *
 * <p><b>NOTE: quick checking in this class is minimal to keep this class lean; concrete classes can
 * add more if desired.</b>
 *
 * <p>This class is <b>not</b> completely threadsafe though portions may safely be used by multiple
 * threads.
 */
public abstract class StopwatchBase implements Stopwatch {
  private long startTimeNanos;
  private long stopTimeNanos;

  protected StopwatchBase(long startTimeNanos) {
    this.startTimeNanos = startTimeNanos;
  }

  protected abstract long relTimeNanos();

  protected void ensureState(State requiredState) {
    if (getState() != requiredState) {
      throw new RuntimeException("Stopwatch state not: " + requiredState);
    }
  }

  // control

  @Override
  public void start() {
    // (See quick checking note above)
    stopTimeNanos = 0;
    startTimeNanos = relTimeNanos();
  }

  @Override
  public void stop() {
    // (See quick checking note above)
    stopTimeNanos = relTimeNanos();
  }

  @Override
  public void reset() {
    stopTimeNanos = 0;
    startTimeNanos = relTimeNanos();
  }

  // elapsed

  @Override
  public long getElapsedNanos() {
    // (See quick checking note above)
    return stopTimeNanos - startTimeNanos;
  }

  @Override
  public long getElapsedMillisLong() {
    return nanos2millisLong(getElapsedNanos());
  }

  @Override
  public int getElapsedMillis() {
    long elapsedMillis;

    elapsedMillis = getElapsedMillisLong();
    checkTooManyMillis(elapsedMillis);

    return (int) elapsedMillis;
  }

  @Override
  public double getElapsedSeconds() {
    return nanos2seconds(getElapsedNanos());
  }

  @Override
  public BigDecimal getElapsedSecondsBD() {
    return nanos2secondsBD(getElapsedNanos());
  }

  // split

  @Override
  public long getSplitNanos() {
    long curTimeNanos;

    // (See quick checking note above)
    curTimeNanos = relTimeNanos();
    if (isStopped()) {
      curTimeNanos = stopTimeNanos;
    }
    return curTimeNanos - startTimeNanos;
  }

  @Override
  public long getSplitMillisLong() {
    return nanos2millisLong(getSplitNanos());
  }

  @Override
  public int getSplitMillis() {
    long splitMillis;

    splitMillis = getSplitMillisLong();
    checkTooManyMillis(splitMillis);

    return (int) splitMillis;
  }

  @Override
  public double getSplitSeconds() {
    return nanos2seconds(getSplitNanos());
  }

  @Override
  public BigDecimal getSplitSecondsBD() {
    return nanos2secondsBD(getSplitNanos());
  }

  // misc.

  @Override
  public String getName() {
    return "";
  }

  @Override
  public State getState() {
    if (isRunning()) {
      return State.running;
    } else {
      return State.stopped;
    }
  }

  @Override
  public boolean isRunning() {
    return stopTimeNanos == 0;
  }

  @Override
  public boolean isStopped() {
    return !isRunning();
  }

  public String toStringElapsed() {
    return getName() + ":" + getState() + ":" + getElapsedSeconds();
  }

  public String toStringSplit() {
    return getName() + ":" + getState() + ":" + getSplitSeconds();
  }

  @Override
  public String toString() {
    return toStringSplit();
  }
}
