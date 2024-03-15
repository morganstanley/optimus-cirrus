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
package com.ms.silverking.util.jvm;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import optimus.utils.SystemFinalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Finalization {
  private final AbsMillisTimeSource absMillisTimeSource;
  private final Lock lock;
  private final boolean verboseFinalization;
  private long lastFinalizationMillis;

  private static Logger log = LoggerFactory.getLogger(Finalization.class);

  public Finalization(AbsMillisTimeSource absMillisTimeSource, boolean verboseFinalization) {
    this.absMillisTimeSource = absMillisTimeSource;
    this.verboseFinalization = verboseFinalization;
    lastFinalizationMillis = absMillisTimeSource.absTimeMillis();
    lock = new ReentrantLock();
  }

  public boolean forceFinalization(long minFinalizationIntervalMillis) {
    if (minFinalizationIntervalMillis < 0) {
      throw new RuntimeException("minFinalizationIntervalMillis may not be < 0");
    } else {
      long curTimeMillis;
      boolean finalized;

      curTimeMillis = absMillisTimeSource.absTimeMillis(); // ignore lock acquisition time
      lock.lock();
      try {
        // minFinalizationIntervalMillis == 0 => ignore lastFinalizationMillis and force a
        // finalization
        if ((curTimeMillis - lastFinalizationMillis > minFinalizationIntervalMillis)
            || (minFinalizationIntervalMillis == 0)) {
          Stopwatch sw;

          lastFinalizationMillis = curTimeMillis;
          sw = new SimpleStopwatch();
          System.gc();
          SystemFinalization.runFinalizers();
          sw.stop();
          finalized = true;
          if (verboseFinalization) {
            log.info("Finalization completed in {} seconds", sw.getElapsedSeconds());
          }
        } else {
          finalized = false;
        }
      } finally {
        lock.unlock();
      }
      return finalized;
    }
  }
}
