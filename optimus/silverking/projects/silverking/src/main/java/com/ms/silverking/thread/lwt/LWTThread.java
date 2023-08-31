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
package com.ms.silverking.thread.lwt;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A thread specialized to only do LWT work. */
public class LWTThread extends Thread implements LWTCompatibleThread {

  private static Logger log = LoggerFactory.getLogger(LWTThread.class);

  private final int workUnit;
  private final BlockingQueue<AssignedWork> q;
  private final LWTPoolImpl threadPool;
  private boolean running;
  private boolean active;
  private Lock idleLock;
  private Condition idleCV;
  private int depth;

  private final long[] workUnitStats;

  static final int defaultWorkUnit = 1;

  private static final boolean enableStats = false;

  /**
   * @param name
   * @param q
   */
  public LWTThread(
      String name, BlockingQueue<AssignedWork> q, LWTPoolImpl threadPool, int workUnit) {
    super(name);
    this.q = q;
    this.threadPool = threadPool;
    this.workUnit = workUnit;
    if (enableStats) {
      workUnitStats = new long[workUnit + 1];
    } else {
      workUnitStats = null;
    }
    running = true;
    idleLock = new ReentrantLock();
    idleCV = idleLock.newCondition();
    active = true;
    setDaemon(true);
  }

  public int getWorkUnit() {
    return workUnit;
  }

  public BlockingQueue<AssignedWork> getQueue() {
    return q;
  }

  public final void incrementDepth() {
    depth++;
  }

  public final void decrementDepth() {
    depth--;
  }

  public final int getDepth() {
    return depth;
  }

  public void lwtStop() {
    running = false;
  }

  @Override
  public void setBlocked() {
    threadPool.setBlocked(this);
  }

  @Override
  public void setNonBlocked() {
    threadPool.setNonBlocked(this);
  }

  public void setActive() {
    if (active) {
      throw new RuntimeException("Double activation");
    }
    active = true;
    idleLock.lock();
    try {
      idleCV.signalAll();
    } finally {
      idleLock.unlock();
    }
  }

  public void setIdle() {
    if (!active) {
      throw new RuntimeException("Double inactivation");
    }
    active = false;
  }

  public void run() {
    if (workUnit == 1) {
      runSingle();
    } else {
      runMultiple();
    }
  }

  private void runSingle() {
    ThreadState.setLWTThread();
    while (running) {
      try {
        if (active) {
          AssignedWork work;

          work = q.take();
          if (log.isDebugEnabled()) {
            log.debug("{} doWork {}", this, work);
          }
          try {
            work.doWork();
          } catch (Exception e) {
            log.error("", e);
            ThreadUtil.pauseAfterException();
          }
        } else {
          idleLock.lock();
          try {
            idleCV.await();
          } finally {
            idleLock.unlock();
          }
        }
      } catch (Exception e) {
        log.error("Caught exception while running on LWTThread", e);
      }
    }
  }

  private void activeMultiple(AssignedWork[] workList) {
    BaseWorker worker;
    int numWorkItems;

    try {
      numWorkItems = 0;
      AssignedWork entry = q.take();
      workList[numWorkItems] = entry;
      numWorkItems++;
      do {
        entry = q.poll();
        if (entry != null) {
          workList[numWorkItems] = entry;
          ++numWorkItems;
        }
      } while (entry != null && numWorkItems < workList.length);
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interruption not supported", ie);
    }
    if (enableStats) {
      ++workUnitStats[numWorkItems];
    }
    if (numWorkItems == 1) { // special case to speed up single work items
      try {
        workList[0].doWork();
      } catch (Exception e) {
        log.error("", e);
        ThreadUtil.pauseAfterException();
      }
    } else {
      int groupStart;
      int i; // index used to search for first incompatible worker

      groupStart = 0;
      while (groupStart < numWorkItems) {
        Object[] _workList; // work items copied out from workers

        worker = workList[groupStart].getWorker();
        // compute the multiple work list
        for (i = groupStart + 1; i < numWorkItems; i++) {
          if (workList[i].getWorker() != worker) {
            break;
          }
        }
        _workList = worker.newWorkArray(i - groupStart);
        if (_workList != null) {
          for (int j = groupStart; j < i; j++) {
            _workList[j - groupStart] = workList[j].getWork();
          }
          try {
            worker.doWork(_workList);
          } catch (Exception e) {
            log.error("", e);
            ThreadUtil.pauseAfterException();
          }
        } else {
          for (int j = groupStart; j < i; j++) {
            try {
              workList[j].doWork();
            } catch (Exception e) {
              log.error("", e);
              ThreadUtil.pauseAfterException();
            }
          }
        }
        groupStart = i;
      }
    }
  }

  private void runMultiple() {
    AssignedWork[] workList;

    ThreadState.setLWTThread();
    // System.out.println("LWTThread.runMultiple()\t"+ this);
    workList = new AssignedWork[workUnit];
    while (running) {
      try {
        if (active) {
          activeMultiple(workList);
          Arrays.fill(workList, null);
          // Above null fill is to ensure that complete work is GC'd
          // The current strategy is designed to reduce the overhead of
          // this clearing operation.
          // We could do this one-by-one if needed, but this is
          // unlikely due to the fact that tasks should be very quick.
        } else {
          idleLock.lock();
          try {
            idleCV.await();
          } finally {
            idleLock.unlock();
          }
        }
      } catch (Exception e) {
        log.error("", e);
        ThreadUtil.pauseAfterException();
      }
    }
  }

  public void gatherStats(long[] _workUnitStats) {
    for (int i = 0; i < workUnitStats.length; i++) {
      _workUnitStats[i] += workUnitStats[i];
    }
  }

  public void dumpStats() {
    if (enableStats) {
      LWTPoolImpl.dumpWorkUnitStats(workUnitStats, "LWTStats: " + getName());
    }
  }

  public boolean getRunningStatus() {
    return running;
  }
}
