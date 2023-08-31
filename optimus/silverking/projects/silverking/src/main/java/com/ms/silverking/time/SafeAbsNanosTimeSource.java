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

import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * SafeAbsNanosTimeSource returns a "safe" absolute time in nano seconds by filtering out observed
 * issues in System.nanoTime(). In particular, this class guards against both System.nanoTime()
 * forward and backward excursions. System.currentTimeMillis() provides the ground truth;.
 *
 * <p>NOTE: Presently, the safety features are turned off as they need additional testing before
 * introducing them
 */
public class SafeAbsNanosTimeSource implements AbsNanosTimeSource {
  public final long nanoOriginTimeInMillis;
  public final AtomicLong nanoOriginTimeInNanos;
  public final AtomicLong lastReturnedAbsTimeNanos;
  public final AtomicLong lastSystemNanos;
  public final long quickCheckThresholdNanos;
  public final long originDeltaToleranceNanos;

  private static Logger log = LoggerFactory.getLogger(SafeAbsNanosTimeSource.class);

  private final AtomicLong lastTimeNanos = new AtomicLong();

  private static final long nanosPerMilli = 1000000;

  private static final long defaultQuickCheckThresholdNanos;
  private static final String defaultQuickCheckThresholdNanosProperty =
      SafeAbsNanosTimeSource.class.getName() + ".DefaultQuickCheckThresholdNanos";
  private static final long defaultDefaultQuickCheckThresholdNanos = 20 * nanosPerMilli;

  private static final long defaultOriginDeltaToleranceNanos;
  private static final String defaultOriginDeltaToleranceNanosProperty =
      SafeAbsNanosTimeSource.class.getName() + ".DefaultOriginDeltaToleranceNanos";
  private static final long defaultDefaultOriginDeltaToleranceNanos = 100 * nanosPerMilli;

  static {
    String def;

    def =
        PropertiesHelper.systemHelper.getString(
            defaultQuickCheckThresholdNanosProperty, UndefinedAction.ZeroOnUndefined);
    if (def != null) {
      defaultQuickCheckThresholdNanos = Long.parseLong(def);
    } else {
      defaultQuickCheckThresholdNanos = defaultDefaultQuickCheckThresholdNanos;
    }

    def =
        PropertiesHelper.systemHelper.getString(
            defaultOriginDeltaToleranceNanosProperty, UndefinedAction.ZeroOnUndefined);
    if (def != null) {
      defaultOriginDeltaToleranceNanos = Long.parseLong(def);
    } else {
      defaultOriginDeltaToleranceNanos = defaultDefaultOriginDeltaToleranceNanos;
    }
  }

  public SafeAbsNanosTimeSource(
      long nanoOriginTimeInMillis, long quickCheckThresholdNanos, long originDeltaToleranceNanos) {
    this.nanoOriginTimeInMillis = nanoOriginTimeInMillis;
    this.quickCheckThresholdNanos = quickCheckThresholdNanos;
    this.originDeltaToleranceNanos = originDeltaToleranceNanos;
    nanoOriginTimeInNanos = new AtomicLong();
    lastReturnedAbsTimeNanos = new AtomicLong();
    lastSystemNanos = new AtomicLong();
    setNanosOriginTime(System.nanoTime());
  }

  public SafeAbsNanosTimeSource(long nanoOriginTimeInMillis) {
    this(nanoOriginTimeInMillis, defaultQuickCheckThresholdNanos, defaultOriginDeltaToleranceNanos);
  }

  /** When a problem is detected, recompute nanoOriginTimeInMillis. */
  private void setNanosOriginTime(long _systemTimeNanos) {
    long curTimeMillis;
    long curTimeNanos;
    long deltaMillis;
    long deltaNanos;
    long newOriginTimeInNanos;

    curTimeMillis = System.currentTimeMillis();
    curTimeNanos = _systemTimeNanos;
    deltaMillis = curTimeMillis - nanoOriginTimeInMillis;
    if (deltaMillis < 0) {
      log.error("{}", curTimeMillis);
      log.error("{}", nanoOriginTimeInMillis);
      throw new RuntimeException("deltaMillis < 0");
    }
    deltaNanos = deltaMillis * nanosPerMilli;
    newOriginTimeInNanos = curTimeNanos - deltaNanos;
    // FUTURE - put into mutex function new in Java 8
    if (Math.abs(nanoOriginTimeInNanos.get() - newOriginTimeInNanos) > originDeltaToleranceNanos) {
      if (log.isDebugEnabled()) {
        log.debug(
            "nanoOriginTimeInNanos {} -> {}", nanoOriginTimeInNanos.get(), newOriginTimeInNanos);
      }
      nanoOriginTimeInNanos.set(newOriginTimeInNanos);
    }
    // System.out.printf("nanoOriginTimeInNanos\t%s\n", nanoOriginTimeInNanos);
  }

  @Override
  public long getNanosOriginTime() {
    return nanoOriginTimeInNanos.get();
  }

  @Override
  public long absTimeNanos() {
    long t;
    long prev;

    t = System.nanoTime() - nanoOriginTimeInNanos.get();
    prev = lastTimeNanos.getAndUpdate(x -> x < t ? t : x + 1);
    return t > prev ? t : prev + 1;
    // return System.nanoTime() - nanoOriginTimeInNanos.get();
  }

  /*
  @Override
  public long absTimeNanos() {
      long    candidateAbsTimeNanos;
      long    safeAbsTimeNanos;
      long    _systemNanoTime;
      long    _lastReturnedNanos;
      long    _lastSystemNanos;
      long    _nanoOriginTime;

      _lastReturnedNanos = lastReturnedAbsTimeNanos.get();
      _lastSystemNanos = lastSystemNanos.get();
      _systemNanoTime = System.nanoTime(); // must only retrieve system time once
                                           // so that remaining logic is valid
      lastSystemNanos.set(_systemNanoTime);
      if (_systemNanoTime < _lastSystemNanos) {
          _systemNanoTime = _lastSystemNanos;
      }
      _nanoOriginTime = nanoOriginTimeInNanos.get();

      if (_systemNanoTime < _lastSystemNanos) {
          setNanosOriginTime(_systemNanoTime);
      }

      candidateAbsTimeNanos = _systemNanoTime - _nanoOriginTime;
      if (candidateAbsTimeNanos - _lastReturnedNanos > quickCheckThresholdNanos) {
          setNanosOriginTime(_systemNanoTime);
          candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();

          if (candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) {
              safeAbsTimeNanos = lastReturnedAbsTimeNanos.get();
          } else {
              safeAbsTimeNanos = candidateAbsTimeNanos;
              lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
          }
      } else {
          safeAbsTimeNanos = candidateAbsTimeNanos;
          lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
      }
      return safeAbsTimeNanos;
  }
  */

  /*
  @Override
  public long absTimeNanos() {
      long    candidateAbsTimeNanos;
      long    safeAbsTimeNanos;
      long    _systemNanoTime;

      _systemNanoTime = System.nanoTime(); // must only retrieve system time once
                                           // so that remaining logic is valid
      candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();
      if (candidateAbsTimeNanos - lastReturnedAbsTimeNanos.get() > quickCheckThresholdNanos
              || candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) { // if we suspect something is wrong
          if (debug) {
              System.out.printf("%s\t%d\t%d\n", Thread.currentThread().getName(), candidateAbsTimeNanos,
              lastReturnedAbsTimeNanos.get());
              System.out.printf("%s\t%s\n", (candidateAbsTimeNanos - lastReturnedAbsTimeNanos.get() >
              quickCheckThresholdNanos),
              (candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()));
          }
          setNanosOriginTime(_systemNanoTime);
          candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();

          if (candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) {
              safeAbsTimeNanos = lastReturnedAbsTimeNanos.get();
          } else {
              safeAbsTimeNanos = candidateAbsTimeNanos;
              lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
          }
      } else {
          safeAbsTimeNanos = candidateAbsTimeNanos;
          lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
      }
      return safeAbsTimeNanos;
  }
  */

  @Override
  public long relNanosRemaining(long absDeadlineNanos) {
    return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineNanos, absTimeNanos());
  }

  //    @Override
  //    public int hashCode() {
  //        return Long.hashCode(nanoOriginTimeInMillis)
  //                ^ Long.hashCode(quickCheckThresholdNanos)
  //                ^ Long.hashCode(originDeltaToleranceNanos);
  ////        return Long.hashCode(nanoOriginTimeInMillis)
  ////                ^ nanoOriginTimeInNanos.hashCode()
  ////                ^ lastReturnedAbsTimeNanos.hashCode()
  ////                ^ lastSystemNanos.hashCode()
  ////                ^ Long.hashCode(quickCheckThresholdNanos)
  ////                ^ Long.hashCode(originDeltaToleranceNanos);
  //    }

  //    @Override
  //    public boolean equals(Object o) {
  //        if (this == o) {
  //            return true;
  //        }
  //
  //        if (this.getClass() != o.getClass()) {
  //            return false;
  //        }
  //
  //        SafeAbsNanosTimeSource other = (SafeAbsNanosTimeSource)o;
  //
  //        return nanoOriginTimeInMillis == other.nanoOriginTimeInMillis
  //                && quickCheckThresholdNanos == other.quickCheckThresholdNanos
  //                && originDeltaToleranceNanos == other.originDeltaToleranceNanos;
  ////        return nanoOriginTimeInMillis == other.nanoOriginTimeInMillis
  ////                && nanoOriginTimeInNanos.equals(other.nanoOriginTimeInNanos)
  ////                && lastReturnedAbsTimeNanos.equals(other.lastReturnedAbsTimeNanos)
  ////                && lastSystemNanos.equals(other.lastSystemNanos)
  ////                && quickCheckThresholdNanos == other.quickCheckThresholdNanos
  ////                && originDeltaToleranceNanos == other.originDeltaToleranceNanos;
  //    }

}
