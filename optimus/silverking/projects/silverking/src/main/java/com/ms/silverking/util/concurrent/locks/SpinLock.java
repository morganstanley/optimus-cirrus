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
package com.ms.silverking.util.concurrent.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * User-level spin lock. Use with care as the scheduler may preempt the thread
 * that holds the lock at any time.
 */
public class SpinLock implements Lock {
  private final AtomicBoolean locked;

  public SpinLock() {
    locked = new AtomicBoolean();
  }

  @Override
  public void lock() {
    while (!locked.compareAndSet(false, true))
      ;
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tryLock() {
    return locked.compareAndSet(false, true);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    long timeNanos;
    long t1;
    boolean acquired;

    t1 = System.nanoTime();
    timeNanos = unit.convert(time, TimeUnit.NANOSECONDS);
    do {
      acquired = locked.compareAndSet(false, true);
    } while (!acquired && System.nanoTime() - t1 < timeNanos);
    return acquired;
  }

  @Override
  public void unlock() {
    locked.getAndSet(false);
    // consider owner verification
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
