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
package com.ms.silverking.util.memory;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ms.silverking.code.Constraint;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.util.jvm.Finalization;
import optimus.utils.SystemFinalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitor JVM memory. */
public class JVMMonitor implements Runnable {
  private boolean running;

  private final int minUpdateIntervalMillis;
  private final int maxUpdateIntervalMillis;
  private final int finalizationIntervalMillis;
  private final double absChangeTriggerMB = 10.0;
  private final double relChangeTrigger = 0.05;
  private final Runtime runtime;
  private final boolean display;
  private final Stopwatch finalizationSW;
  private final Stopwatch sw;
  private final AtomicBoolean cleanupHinted;
  private final List<JVMMemoryObserver> memoryObservers;
  private final Finalization finalization;

  private static Logger log = LoggerFactory.getLogger(JVMMonitor.class);

  private boolean deltaTriggered;

  private final double lowMemoryThresholdMB;
  private boolean memoryLow;

  private long freeMemory;
  private long maxMemory;
  private long totalMemory;

  private double freeMemoryMB;
  private double maxMemoryMB;
  private double totalMemoryMB;

  public JVMMonitor(
      int minUpdateIntervalMillis,
      int maxUpdateIntervalMillis,
      int finalizationIntervalMillis,
      boolean display,
      double lowMemoryThresholdMB,
      Finalization finalization) {
    this.minUpdateIntervalMillis = minUpdateIntervalMillis;
    this.maxUpdateIntervalMillis = maxUpdateIntervalMillis;
    this.finalizationIntervalMillis = finalizationIntervalMillis;
    this.display = display;
    if (lowMemoryThresholdMB < 0.0) {
      throw new RuntimeException("Bad lowMemoryThreshold: " + lowMemoryThresholdMB);
    }
    this.lowMemoryThresholdMB = lowMemoryThresholdMB;
    memoryLow = false;
    runtime = Runtime.getRuntime();
    running = true;
    cleanupHinted = new AtomicBoolean(false);
    finalizationSW = new SimpleStopwatch();
    sw = new SimpleStopwatch();
    memoryObservers = new Vector<JVMMemoryObserver>();
    this.finalization = finalization;
    new Thread(this, "JVMMonitor").start();
  }

  public JVMMonitor(
      int minUpdateIntervalMillis,
      int maxUpdateIntervalMillis,
      int finalizationIntervalMillis,
      boolean display) {
    this(
        minUpdateIntervalMillis,
        maxUpdateIntervalMillis,
        finalizationIntervalMillis,
        display,
        0,
        null);
  }

  public void addMemoryObserver(JVMMemoryObserver memoryObserver) {
    Constraint.ensureNotNull(memoryObserver);
    memoryObservers.add(memoryObserver);
  }

  public boolean memoryLow() {
    return memoryLow;
  }

  public void cleanupHint() {
    cleanupHinted.set(true);
  }

  private void notifyMemoryObservers(boolean memoryIsLow) {
    for (JVMMemoryObserver memoryObserver : memoryObservers) {
      memoryObserver.jvmMemoryLow(memoryIsLow);
    }
  }

  private void notifyMemoryObservers(long bytesFree) {
    for (JVMMemoryObserver memoryObserver : memoryObservers) {
      memoryObserver.jvmMemoryStatus(bytesFree);
    }
  }

  public void monitor() {
    boolean currentMemoryLow;

    if (finalizationSW.getSplitMillis() > finalizationIntervalMillis || cleanupHinted.get()) {
      finalizationSW.reset();
      log.info("Forcing finalization");
      if (finalization == null) {
        SystemFinalization.runFinalizers();
      } else {
        finalization.forceFinalization(0);
      }
      System.gc();
      cleanupHinted.set(false);
    }

    freeMemory = runtime.freeMemory();
    maxMemory = runtime.maxMemory();
    totalMemory = runtime.totalMemory();

    triggerCheck(freeMemoryMB, bytesToMB(freeMemory));
    triggerCheck(maxMemoryMB, bytesToMB(maxMemory));
    triggerCheck(totalMemoryMB, bytesToMB(totalMemory));

    freeMemoryMB = bytesToMB(freeMemory);
    maxMemoryMB = bytesToMB(maxMemory);
    totalMemoryMB = bytesToMB(totalMemory);

    if (freeMemoryMB < lowMemoryThresholdMB) {
      System.gc();
      freeMemoryMB = bytesToMB(freeMemory);
      maxMemoryMB = bytesToMB(maxMemory);
      totalMemoryMB = bytesToMB(totalMemory);
      currentMemoryLow = freeMemoryMB < lowMemoryThresholdMB;
      if (currentMemoryLow) {
        log.info("Memory is low: {}", freeMemoryMB);
      }
    } else {
      currentMemoryLow = false;
    }
    notifyMemoryObservers(freeMemory);
    if (currentMemoryLow != memoryLow) {
      memoryLow = currentMemoryLow;
      notifyMemoryObservers(currentMemoryLow);
    }
  }

  private void triggerCheck(double ov, double nv) {
    if (Math.abs(ov - nv) > absChangeTriggerMB) {
      deltaTriggered = true;
    }
    if (Math.abs(1.0 - ov / nv) > relChangeTrigger) {
      deltaTriggered = true;
    }
  }

  final double bytesToMB(long bytes) {
    double mb;

    mb = (double) bytes / (1024.0 * 1024.0);
    return mb;
  }

  public void displayStatus() {
    log.info(statusString());
  }

  public String statusString() {
    return String.format(
        "JVMMonitor: %f\t%4.2f\t%4.2f\t%4.2f\t%s",
        sw.getSplitSeconds(), freeMemoryMB, maxMemoryMB, totalMemoryMB, memoryLow);
  }

  public void run() {
    Stopwatch displaySW;

    ThreadUtil.randomSleep(minUpdateIntervalMillis);
    finalizationSW.reset();
    displaySW = new SimpleStopwatch();
    while (running) {
      try {
        monitor();
        if (display) {
          boolean displayTriggered;

          displayTriggered = false;
          if (displaySW.getSplitMillis() > maxUpdateIntervalMillis) {
            displayTriggered = true;
          }
          if (deltaTriggered) {
            displayTriggered = true;
          }
          if (displayTriggered) {
            deltaTriggered = false;
            displaySW.reset();
            displayStatus();
          }
        }
        ThreadUtil.sleep(minUpdateIntervalMillis);
      } catch (Exception e) {
        log.error("JVMMonitor , run", e);
        ThreadUtil.pauseAfterException();
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    new JVMMonitor(1000, 20 * 1000, 20 * 1000, true);
  }
}
