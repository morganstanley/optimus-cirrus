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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BaseWorker with the ability pause work processing and group the paused work. An external source
 * tells this worker when to pause. When paused, all incoming work is queueud up. Upon being
 * unpaused, work in the queue is grouped and processed.
 */
public abstract class GroupingPausingBaseWorker<I> extends BaseWorker<I> {
  private volatile boolean paused;
  private final ConcurrentLinkedQueue<I> pauseQ;
  private final QWorker<I> qWorker;
  private final AtomicLong unpauseCount;

  private static Logger log = LoggerFactory.getLogger(GroupingPausingBaseWorker.class);

  private static final int maxWorkArrays = 64;

  private static final boolean debugPause = false;

  public GroupingPausingBaseWorker(
      LWTPool workPool,
      boolean allowsConcurrentWork,
      int workerMaxDirectCallDepth,
      int idleThreadThreshold) {
    super(workPool, allowsConcurrentWork, workerMaxDirectCallDepth, idleThreadThreshold);
    pauseQ = new ConcurrentLinkedQueue<>();
    paused = false;
    qWorker = new QWorker<>(this, pauseQ);
    unpauseCount = new AtomicLong();
  }

  public GroupingPausingBaseWorker(
      LWTPool workPool, boolean allowsConcurrentWork, int workerMaxDirectCallDepth) {
    this(
        workPool,
        allowsConcurrentWork,
        workerMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public GroupingPausingBaseWorker(LWTPool workPool, boolean allowsConcurrentWork) {
    this(
        workPool,
        allowsConcurrentWork,
        LWTConstants.defaultMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public GroupingPausingBaseWorker(boolean allowsConcurrentWork, int maxDirectCallDepth) {
    this(
        LWTPoolProvider.defaultConcurrentWorkPool,
        true,
        maxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public GroupingPausingBaseWorker(
      boolean allowsConcurrentWork, int maxDirectCallDepth, int idleThreadThreshold) {
    this(LWTPoolProvider.defaultConcurrentWorkPool, true, maxDirectCallDepth, idleThreadThreshold);
  }

  public GroupingPausingBaseWorker(boolean allowsConcurrentWork) {
    this(
        allowsConcurrentWork
            ? LWTPoolProvider.defaultConcurrentWorkPool
            : LWTPoolProvider.defaultNonConcurrentWorkPool,
        allowsConcurrentWork);
  }

  public GroupingPausingBaseWorker() {
    this(true, LWTConstants.defaultMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
  }

  public void pause() {
    if (debugPause) {
      log.debug("pause  {}", pauseQ.size());
    }
    paused = true;
  }

  public void unpause() {
    if (debugPause) {
      log.debug("unpause  {}", pauseQ.size());
    }
    if (paused) {
      paused = false;
      unpauseCount.incrementAndGet();
      qWorker.addWork(null, 0);
    } else {
      // qWorker.addWork(null, 0);
    }
  }

  public void addWorkForGrouping(I item, int callerMaxDirectCallDepth) {
    // if (debugPause) {
    //    System.out.println("awg:\t"+ pauseQ.size() +"\t"+ paused);
    // }
    if (!paused) {
      if (debugPause) {
        log.debug("   addWorkForGrouping.!paused1");
      }
      addWork(item, callerMaxDirectCallDepth);
    } else {
      long pc1;

      // If we're paused, then we need to queue the data, but
      // also make sure that we don't miss an unpause
      pc1 = unpauseCount.get();
      if (!paused) {
        if (debugPause) {
          log.debug("   doWork.!paused2");
        }
        addWork(item, callerMaxDirectCallDepth);
      } else {
        // If between the last read of "paused", an upause() (and its corresponding
        // QWorker.doWork())
        // has occurred, then this pauseQ.add() could result in stranded work.
        pauseQ.add(item);
        if (debugPause) {
          log.debug("   doWork.paused pauseQ.size() {}", pauseQ.size());
        }
        if (unpauseCount.get() != pc1) {
          // Unpause detected, call QWorker.doWork() to remove the work.
          qWorker.addWork(null);
        }
      }
    }
  }

  private final Object drainSync = new Object();

  public void drainPauseQ() {
    I[] work;

    synchronized (drainSync) {
      int qSize;

      qSize = pauseQ.size();
      if (qSize == 0) {
        return;
      }
      work = newWorkArray(qSize);
      for (int i = 0; i < work.length; i++) {
        work[i] = pauseQ.remove();
      }
    }
    // FUTURE - Note that this allows for reordered requests.
    // Probably OK since we only have that possibility if
    // users have submitted simultaneously
    // Could have a flag to turn this off for cases where it
    // is not OK
    doWork(work);
  }

  private static class QWorker<I> extends BaseWorker<Object> {
    private final GroupingPausingBaseWorker<I> gpbw;
    private final ConcurrentLinkedQueue<I> pauseQ;

    QWorker(GroupingPausingBaseWorker<I> gpbw, ConcurrentLinkedQueue<I> pauseQ) {
      this.gpbw = gpbw;
      this.pauseQ = pauseQ;
    }

    @Override
    public void doWork(Object item) {
      if (debugPause) {
        log.debug("GroupingPausingBaseWorker.QWorker.doWork() {}", pauseQ.size());
      }
      gpbw.drainPauseQ();
    }

    public Object[] newWorkArray(int size) {
      return new Object[size];
    }
  }
}
