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

import com.ms.silverking.numeric.MutableInteger;

/**
 * Per-thread LWT state.
 */
final class ThreadState {
  private static final ThreadLocal<MutableInteger> depth = new ThreadLocal<MutableInteger>();
  private static final ThreadLocal<Boolean> isLWTThread = new ThreadLocal<Boolean>();

  public static void setLWTThread() {
    isLWTThread.set(new Boolean(true));
  }

  public static boolean isLWTThread() {
    //if (Thread.currentThread() instanceof LWTThread) {
    if (Thread.currentThread() instanceof LWTCompatibleThread) {
      return true;
    } else {
      Boolean isLWT;

      isLWT = isLWTThread.get();
      return isLWT != null && isLWT.booleanValue();
    }
  }

  public static boolean isLWTCompatibleThread() {
    return Thread.currentThread() instanceof LWTCompatibleThread;
  }

  public static int getDepth() {
    Thread curThread;

    curThread = Thread.currentThread();
    if (curThread instanceof LWTThread) {
      return ((LWTThread) curThread).getDepth();
    } else {
      MutableInteger d;

      d = depth.get();
      if (d == null) {
        d = new MutableInteger();
        depth.set(d);
      }
      return d.getValue();
    }
  }

  public static void incrementDepth() {
    Thread curThread;

    curThread = Thread.currentThread();
    if (curThread instanceof LWTThread) {
      ((LWTThread) curThread).incrementDepth();
    } else {
      MutableInteger d;

      d = depth.get();
      if (d == null) {
        d = new MutableInteger();
        depth.set(d);
      }
      d.increment();
    }
  }

  public static void decrementDepth() {
    Thread curThread;

    curThread = Thread.currentThread();
    if (curThread instanceof LWTThread) {
      ((LWTThread) curThread).decrementDepth();
    } else {
      depth.get().decrement();
    }
  }
}
