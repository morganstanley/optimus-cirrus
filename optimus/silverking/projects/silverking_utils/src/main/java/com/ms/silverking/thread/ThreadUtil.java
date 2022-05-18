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
package com.ms.silverking.thread;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtil {
  protected static final Random random;

  protected static final int DEF_EXCEPTION_PAUSE_MILLIS = 1000;

  private static Logger log = LoggerFactory.getLogger(ThreadUtil.class);

  static {
    random = new Random();
  }

  public static void sleepNanos(int nanos) {
    try {
      Thread.currentThread().sleep(0, nanos);
    } catch (InterruptedException ie) {
    }
  }

  public static void sleepSeconds(double seconds) {
    int millis;

    millis = (int) (seconds * 1000.0);
    sleep(millis);
  }

  public static void sleep(long millis) {
    if (millis > Integer.MAX_VALUE) {
      throw new RuntimeException("Invalid sleep");
    } else {
      sleep((int) millis);
    }
  }

  public static void sleep(int millis) {
    if (millis > 0) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void randomSleep(int minMillis, int maxMillis) {
    int millis;

    if (maxMillis < minMillis) {
      throw new RuntimeException("maxMillis < minMillis");
    }
    millis = minMillis + random.nextInt(Math.max(maxMillis - minMillis, 1));
    sleep(millis);
  }

  public static void randomSleep(int maxMillis) {
    randomSleep(0, maxMillis);
  }

  public static void await(Condition cv, long time, TimeUnit unit) {
    if (time > 0) {
      try {
        cv.await(time, unit);
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void await(Condition cv, int millis) {
    await(cv, millis, TimeUnit.MILLISECONDS);
  }

  public static void randomAwait(Condition cv, int minMillis, int maxMillis) {
    int millis;

    if (maxMillis < minMillis) {
      throw new RuntimeException("maxMillis < minMillis");
    }
    millis = minMillis + random.nextInt(Math.max(maxMillis - minMillis, 1));
    await(cv, millis);
  }

  public static void randomAwait(Condition cv, int maxMillis) {
    randomAwait(cv, 0, maxMillis);
  }

  //////////////////////////////////////////////////////////////////

  public static void sleepSeconds(double seconds, Object obj) {
    int millis;

    millis = (int) (seconds * 1000.0);
    sleep(millis, obj);
  }

  public static void sleep(int millis, Object obj) {
    if (millis > 0) {
      synchronized (obj) {
        try {
          obj.wait(millis);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  public static void randomSleep(int minMillis, int maxMillis, Object obj) {
    int millis;

    if (maxMillis < minMillis) {
      throw new RuntimeException("maxMillis < minMillis");
    }
    millis = minMillis + random.nextInt(Math.max(maxMillis - minMillis, 1));
    sleep(millis, obj);
  }

  public static void randomSleep(int maxMillis, Object obj) {
    randomSleep(0, maxMillis, obj);
  }

  public static void sleepForever() {
    while (true) {
      sleep(Integer.MAX_VALUE);
    }
  }

  //////////////////////////////////////////////////////////////////

  public static void pauseAfterException() {
    sleep(DEF_EXCEPTION_PAUSE_MILLIS);
  }

  public static Thread newDaemonThread(Runnable runnable, String name) {
    Thread thread;

    thread = new Thread(runnable, name);
    thread.setDaemon(true);
    return thread;
  }

  public static void printStackTraces() {
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      if (entry.getValue().length == 0 || entry.getValue()[0].toString().indexOf("sun.misc.Unsafe.park") < 0) {
        log.info("{}",entry.getKey().getName());
        for (StackTraceElement e : entry.getValue()) {
          log.info("  {}",e.toString());
        }
      }
    }
  }
}