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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.id.UUIDBase;

/**
 * Concrete WorkerGroup implementation.
 *
 * @param <I>
 */
class WorkerGroupImpl<I> implements WorkerGroup<I> {
  private final String name;
  private final List<BaseWorker<I>> workers;
  private final AtomicInteger nextWorker;
  private final int maxDirectCallDepth;
  private final int idleThreadThreshold;

  public WorkerGroupImpl(String name, int maxDirectCallDepth, int idleThreadThreshold) {
    if (name != null) {
      this.name = name;
    } else {
      this.name = new UUIDBase().toString();
    }
    workers = new CopyOnWriteArrayList<>();
    nextWorker = new AtomicInteger();
    this.maxDirectCallDepth = maxDirectCallDepth;
    this.idleThreadThreshold = idleThreadThreshold;
  }

  public WorkerGroupImpl(
      String name, BaseWorker<I>[] workers, int maxDirectCallDepth, int idleThreadThreshold) {
    this(name, maxDirectCallDepth, idleThreadThreshold);
    for (BaseWorker<I> worker : workers) {
      addWorker(worker);
    }
  }

  public WorkerGroupImpl(BaseWorker<I>[] workers) {
    this(
        null,
        workers,
        LWTConstants.defaultMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  public WorkerGroupImpl(
      String name,
      Collection<BaseWorker<I>> workers,
      int maxDirectCallDepth,
      int idleThreadThreshold) {
    this(name, maxDirectCallDepth, idleThreadThreshold);
    for (BaseWorker<I> worker : workers) {
      addWorker(worker);
    }
  }

  public WorkerGroupImpl(
      Collection<BaseWorker<I>> workers, int maxDirectCallDepth, int idleThreadThreshold) {
    this(
        null,
        workers,
        LWTConstants.defaultMaxDirectCallDepth,
        LWTConstants.defaultIdleThreadThreshold);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void addWorker(BaseWorker<I> worker) {
    workers.add(worker);
  }

  @Override
  public void broadcastWork(I item) {
    // System.out.println("broadcast: "+ item);
    for (BaseWorker<I> worker : workers) {
      // System.out.println("adding to worker: "+ worker);
      worker.addWork(item, maxDirectCallDepth, idleThreadThreshold);
    }
  }

  @Override
  public void scatterWork(I[] items) {
    for (I item : items) {
      addWork(item);
    }
  }

  @Override
  public void scatterWork(Collection<I> items) {
    for (I item : items) {
      addWork(item);
    }
  }

  @Override
  public void addWork(I item) {
    workers
        .get(Math.abs(nextWorker.getAndIncrement() % workers.size()))
        .addWork(item, maxDirectCallDepth, idleThreadThreshold);
  }
}
