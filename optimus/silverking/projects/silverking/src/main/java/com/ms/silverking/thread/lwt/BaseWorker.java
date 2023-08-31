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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nearly-complete implementation of Worker. Users extend and fill in the doWork() method to make it
 * complete. Despite the name, a Worker is a container for work NOT a thread which can execute the
 * work. Work can be "submitted" to the worker, which is pushed into an LWTPool. I.e. we are
 * submitting for future execution on a separate thread via some referenced pool. the LWTPool has
 * LWTThreads, which select work and execute it. The Worker describes how to execute that work
 *
 * @param <I> type of work objects
 */
public abstract class BaseWorker<I> {
  private final boolean allowsConcurrentWork;
  private final LWTPoolImpl threadPool;
  private final int workerMaxDirectCallDepth;
  private final int idleThreadThreshold;

  private static Logger log = LoggerFactory.getLogger(BaseWorker.class);

  public BaseWorker(
      LWTPool workPool,
      boolean allowsConcurrentWork,
      int workerMaxDirectCallDepth,
      int idleThreadThreshold) {
    assert workPool != null;
    if (workPool == null) {
      if (LWTPoolProvider.defaultConcurrentWorkPool == null
          && LWTPoolProvider.defaultNonConcurrentWorkPool == null) {
        throw new RuntimeException("WorkPools not created");
      } else {
        if (allowsConcurrentWork) {
          workPool = LWTPoolProvider.defaultConcurrentWorkPool;
        } else {
          workPool = LWTPoolProvider.defaultNonConcurrentWorkPool;
        }
      }
    }
    this.allowsConcurrentWork = allowsConcurrentWork;
    this.threadPool = (LWTPoolImpl) workPool;
    this.workerMaxDirectCallDepth = workerMaxDirectCallDepth;
    this.idleThreadThreshold = idleThreadThreshold;
  }

  public BaseWorker(LWTPool workPool, boolean allowsConcurrentWork, int workerMaxDirectCallDepth) {
    this(
        workPool,
        allowsConcurrentWork,
        workerMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public BaseWorker(LWTPool workPool, boolean allowsConcurrentWork) {
    this(
        workPool,
        allowsConcurrentWork,
        LWTConstants.defaultMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public BaseWorker(boolean allowsConcurrentWork, int maxDirectCallDepth) {
    this(
        LWTPoolProvider.defaultConcurrentWorkPool,
        true,
        maxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public BaseWorker(boolean allowsConcurrentWork, int maxDirectCallDepth, int idleThreadThreshold) {
    this(LWTPoolProvider.defaultConcurrentWorkPool, true, maxDirectCallDepth, idleThreadThreshold);
  }

  public BaseWorker(boolean allowsConcurrentWork) {
    this(
        allowsConcurrentWork
            ? LWTPoolProvider.defaultConcurrentWorkPool
            : LWTPoolProvider.defaultNonConcurrentWorkPool,
        allowsConcurrentWork);
  }

  public BaseWorker() {
    this(true, LWTConstants.defaultMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
  }

  //

  public final boolean allowsConcurrentWork() {
    return allowsConcurrentWork;
  }

  public final void addWork(I item) {
    addWork(item, Integer.MAX_VALUE, idleThreadThreshold);
  }

  public final void addWork(I item, int callerMaxDirectCallDepth) {
    addWork(item, callerMaxDirectCallDepth, idleThreadThreshold);
  }

  public final void addWork(I item, int callerMaxDirectCallDepth, int idleThreadThreshold) {
    addPrioritizedWork(item, 0, callerMaxDirectCallDepth, idleThreadThreshold, false);
  }

  public final void addWork(
      I item, int callerMaxDirectCallDepth, int idleThreadThreshold, boolean schedulable) {
    addPrioritizedWork(item, 0, callerMaxDirectCallDepth, idleThreadThreshold, schedulable);
  }

  public final void addPrioritizedWork(I item, int priority) {
    addPrioritizedWork(item, priority, Integer.MAX_VALUE, idleThreadThreshold, false);
  }

  /**
   * Fundamental work addition methods. Other methods feed into this method.
   *
   * @param item
   * @param priority
   * @param callerMaxDirectCallDepth
   * @param idleThreadThreshold
   */
  private final void addPrioritizedWork(
      I item,
      int priority,
      int callerMaxDirectCallDepth,
      int idleThreadThreshold,
      boolean schedulable) {
    int maxDirectCallDepth;

    /*
     * Two cases here: 1 - We queue the work to the pool. 2 - We can bypass the queue and
     * directly do the work.
     *
     * We prefer 2, but must use 1 for: non-lwt threads, whenever the call depth exceeds
     * the limit specified, or whenever too many threads are idle.
     * The idle thread threshold is used to prevent cores from going idle while a single
     * core does all of the work.
     */
    maxDirectCallDepth = Math.min(callerMaxDirectCallDepth, workerMaxDirectCallDepth);
    if (schedulable
        || threadPool.numIdleThreads() > idleThreadThreshold
        || ThreadState.getDepth() >= maxDirectCallDepth) {
      // Schedulable work is eligible for queuing as we may want to throttle it
      // Some non-schedulable work is also eligible for queuing depending on idle threads and call
      // depth.
      // We propagate the schedulable flag to the pool so, if the pool is scheduling,
      // it can distinguish the two work types and dispatch to the scheduler or execution queue as
      // appropriate
      // For example, initial unforwarded requests are schedulable if msg throttling is enabled (if
      // not, they are directly called)
      // A request ready to be sent to a proxy is unschedulable but queueable (so we would want to
      // dispatch to execution queue)
      // An unthrottled request when we have idle threads or a high call depth can be dispatched to
      // the execution queue
      // TODO(OPTIMUS-46970): Avoid hopping a thread boundary here if possible when handling local
      // forwards
      // N.b. this implies we are forced to cross an extra thread boundary when msg throttling is
      // enabled,
      // You could throttle at the point of forward (either to local or remote), but by that point
      // we have done some execution
      // and it becomes harder to distinguish on the remote if we need to throttle again (without
      // sending extra information)
      // So there is a trade-off here to make the throttling model simple and throttle at ingestion
      // before any execution,
      // at the cost of one thread hop. Ideally we would not hop threads for a local forward -
      // instead we could
      // callDoWork on that item and have the local forward be the thing enqueued for scheduling.
      // Remote could be similar
      // though if you're going off box the cost here starts to matter much less.
      if (LWTConstants.enableLogging && log.isDebugEnabled()) {
        log.debug(" queueing {}", item);
      }
      threadPool.addWork(this, item, schedulable);
    } else {
      // Directly call this work
      // For example, unforwarded work when msg Throttling is disabled is called here to prepare for
      // forwarding
      // this is handy as we don't unnecessarily cross a thread boundary until we are ready to send
      // to the remote
      if (LWTConstants.enableLogging && log.isDebugEnabled()) {
        log.debug("  direct call {}", item);
      }
      try {
        callDoWork(item);
      } catch (RuntimeException re) {
        re.printStackTrace();
        // FUTURE - add option to rethrow
      }
    }
  }

  public void callDoWork(I[] items) {
    ThreadState.incrementDepth();
    try {
      doWork(items);
    } finally {
      ThreadState.decrementDepth();
    }
  }

  public final void callDoWork(I item) {
    ThreadState.incrementDepth();
    try {
      doWork(item);
    } finally {
      ThreadState.decrementDepth();
    }
  }

  public void doWork(I[] items, int startIndex, int endIndex) {
    for (int i = startIndex; i <= endIndex; i++) {
      doWork(items[i]);
    }
  }

  public void doWork(I[] items) {
    // System.out.println("BaseWorker.doWork([])");
    for (I item : items) {
      doWork(item);
    }
  }

  public abstract void doWork(I item);

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(':');
    sb.append(allowsConcurrentWork);
    return sb.toString();
  }

  // private AtomicBoolean  foo = new AtomicBoolean(false);

  public I[] newWorkArray(int size) {
    // if (foo.compareAndSet(false, true)) {
    //    ThreadUtil.printStackTraces();
    // }
    // throw new RuntimeException(this +" doesn't support multiple work");
    return null;
  }

  public void stopLWTPool() {
    threadPool.stop();
  }
}
