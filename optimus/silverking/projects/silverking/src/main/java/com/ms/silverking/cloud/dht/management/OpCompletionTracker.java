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
package com.ms.silverking.cloud.dht.management;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

public class OpCompletionTracker {
  private final ConcurrentMap<UUIDBase, OpCompletionState> completionState;

  public OpCompletionTracker() {
    completionState = new ConcurrentHashMap<>();
  }

  public boolean waitForCompletion(UUIDBase uuid, int time, TimeUnit unit) {
    Pair<Set<UUIDBase>, Set<UUIDBase>> result;

    result = waitForCompletion(ImmutableSet.of(uuid), time, unit);
    return result.getV1().size() == 0 && result.getV2().size() == 0;
  }

  public Pair<Set<UUIDBase>, Set<UUIDBase>> waitForCompletion(Set<UUIDBase> uuids, int time, TimeUnit unit) {
    Timer waitTimer;
    Set<UUIDBase> incompleteOps;
    Set<UUIDBase> failedOps;

    incompleteOps = new HashSet<>();
    failedOps = new HashSet<>();
    waitTimer = new SimpleTimer(unit, time);
    for (UUIDBase uuid : uuids) {
      completionState.putIfAbsent(uuid, new OpCompletionState());
    }
    try {
      for (UUIDBase uuid : uuids) {
        OpCompletionState opCompletionState;
        boolean complete;

        opCompletionState = completionState.get(uuid);
        complete = opCompletionState.waitForCompletion(waitTimer.getRemainingMillis(), TimeUnit.MILLISECONDS);
        if (!complete) {
          incompleteOps.add(uuid);
        }
        if (opCompletionState.getResult().hasFailed()) {
          failedOps.add(uuid);
        }
      }
    } finally {
      for (UUIDBase uuid : uuids) {
        completionState.remove(uuid);
      }
    }
    return new Pair<>(incompleteOps, failedOps);
  }

  public void setComplete(UUIDBase uuid, OpResult result) {
    OpCompletionState opCompletionState;

    opCompletionState = completionState.get(uuid);
    if (opCompletionState == null) {
      // We allow for the slight chance of a leak for now
      completionState.putIfAbsent(uuid, new OpCompletionState());
      opCompletionState = completionState.get(uuid);
    }
    if (opCompletionState == null) {
      // This completion state was removed immediately after we added it. Ignore.
    } else {
      opCompletionState.setComplete(result);
    }
  }

  private static class OpCompletionState {
    private OpResult result;
    private Lock lock;
    private Condition cv;

    OpCompletionState() {
      this.lock = new ReentrantLock();
      this.cv = lock.newCondition();
      result = OpResult.INCOMPLETE;
    }

    boolean waitForCompletion(long time, TimeUnit unit) {
      Timer timer;

      timer = new SimpleTimer(unit, time);
      lock.lock();
      try {
        while (!result.isComplete() && !timer.hasExpired()) {
          try {
            timer.await(cv);
          } catch (InterruptedException e) {
          }
        }
      } finally {
        lock.unlock();
      }
      return result.isComplete();
    }

    void setComplete(OpResult result) {
      lock.lock();
      try {
        if (!this.result.isComplete()) {
          this.result = result;
        }
        cv.signalAll();
      } finally {
        lock.unlock();
      }
    }

    OpResult getResult() {
      return result;
    }
  }
}
