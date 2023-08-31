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
package com.ms.silverking.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class LightLinkedBlockingQueue<T> implements BlockingQueue<T> {
  private final ConcurrentLinkedQueue<T> q;
  private final AtomicInteger potentialWaiters;
  private final Lock lock;
  private final Condition nonEmpty;
  private final long spinsBeforeParking;

  private static final long defaultSpinsBeforeParking = 10000;

  public LightLinkedBlockingQueue(long spinsBeforeParking) {
    this.spinsBeforeParking = spinsBeforeParking;
    q = new ConcurrentLinkedQueue<T>();
    potentialWaiters = new AtomicInteger();
    lock = new ReentrantLock();
    nonEmpty = lock.newCondition();
  }

  public LightLinkedBlockingQueue() {
    this(defaultSpinsBeforeParking);
  }

  public LightLinkedBlockingQueue(Collection<? extends T> c) {
    this();
    addAll(c);
  }

  @Override
  public boolean add(T e) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public boolean contains(Object o) {
    return q.contains(o);
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public boolean offer(T e) {
    return q.offer(e);
  }

  protected final void maybeSignalWaiters() {
    if (potentialWaiters.get() > 0) {
      lock.lock();
      try {
        nonEmpty.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public void put(T e) throws InterruptedException {
    q.add(e);
    maybeSignalWaiters();
  }

  public void putAll(Collection<? extends T> c) throws InterruptedException {
    q.addAll(c);
    maybeSignalWaiters();
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    T entry;
    long spin;

    // optimistic attempt to dequeue an entry without locking
    spin = 0;
    do {
      entry = q.poll();
    } while (entry == null && ++spin < spinsBeforeParking);
    if (entry == null) {
      // optimistic attempt failed, so we might need to wait
      // coordinate with writers
      potentialWaiters.incrementAndGet();
      lock.lock();
      try {
        while ((entry = q.poll()) == null) {
          boolean noTimeout;

          // strictly speaking, we should accumulate wait time, don't do that yet
          noTimeout = nonEmpty.await(timeout, unit);
          if (!noTimeout) {
            return null;
          }
        }
      } finally {
        lock.unlock();
        potentialWaiters.decrementAndGet();
      }
    }
    return entry;
  }

  @Override
  public T take() throws InterruptedException {
    T entry;
    int spin;

    // optimistic attempt to dequeue an entry without locking
    spin = 0;
    do {
      entry = q.poll();
    } while (entry == null && ++spin < spinsBeforeParking);
    if (entry == null) {
      // optimistic attempt failed, so we might need to wait
      // coordinate with writers
      potentialWaiters.incrementAndGet();
      lock.lock();
      try {
        while ((entry = q.poll()) == null) {
          nonEmpty.await();
        }
      } finally {
        lock.unlock();
        potentialWaiters.decrementAndGet();
      }
    }
    return entry;
  }

  public int takeMultiple(T[] taken) throws InterruptedException {
    T entry;
    int spin;
    int numTaken;

    assert taken.length >= 2;
    numTaken = 0;
    // optimistic attempt to dequeue an entry without locking
    spin = 0;
    do {
      entry = q.poll();
    } while (entry == null && ++spin < spinsBeforeParking);
    if (entry == null) {
      // optimistic attempt failed, so we might need to wait
      // coordinate with writers
      potentialWaiters.incrementAndGet();
      lock.lock();
      try {
        while ((entry = q.poll()) == null) {
          nonEmpty.await();
          // nonEmpty.awaitNanos(1000 * 1000); // FUTURE think about this
        }
      } finally {
        lock.unlock();
        potentialWaiters.decrementAndGet();
      }
      taken[0] = entry;
      numTaken = 1;
    } else {
      taken[0] = entry;
      numTaken = 1;
      do {
        entry = q.poll();
        if (entry != null) {
          taken[numTaken] = entry;
          ++numTaken;
        }
      } while (entry != null && numTaken < taken.length);
    }
    return numTaken;
  }

  /*
  public int takeAll(Collection<? extends T> c) throws InterruptedException {
      int    numTaken;

      // optimistic attempt to dequeue an entry without locking
      q.
      entry = q.poll();
      if (entry == null) {
          // optimistic attempt failed, so we might need to wait
          // coordinate with writers
          potentialWaiters.incrementAndGet();
          lock.lock();
          try {
              while ((entry = q.poll()) == null) {
                  nonEmpty.await();
              }
          } finally {
              lock.unlock();
              potentialWaiters.decrementAndGet();
          }
      }
      return entry;
  }
  */

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean remove(Object o) {
    return q.remove(o);
  }

  @Override
  public T element() {
    return q.element();
  }

  @Override
  public T peek() {
    return q.peek();
  }

  @Override
  public T poll() {
    return q.poll();
  }

  @Override
  public T remove() {
    return q.remove();
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return q.addAll(c);
  }

  @Override
  public void clear() {
    q.clear();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return q.containsAll(c);
  }

  @Override
  public boolean isEmpty() {
    return q.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    return q.iterator();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return q.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return q.retainAll(c);
  }

  @Override
  public int size() {
    return q.size();
  }

  @Override
  public Object[] toArray() {
    return q.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return q.toArray(a);
  }
}
