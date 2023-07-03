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
package optimus.graph.chaos;

import java.util.Random;

/**
 * Causes random delays to occur when the chaosHook is triggered. Useful for flushing out race
 * conditions when repeatedly stress testing concurrent code by varying the ordering of actions
 * between different threads.
 */
public class RandomDelayMonkey implements ChaosMonkeyRuntime.ChaosMonkey {
  private ThreadLocal<ThreadData> threadLocalData = ThreadLocal.withInitial(() -> new ThreadData());

  public static volatile int MaxNanosBetweenDelays = 500 * 1000;
  public static volatile int MaxLoopsPerDelay = 1000 * 1000;

  static int DummyField = 0;

  private static class ThreadData {
    public Random random = new Random(Thread.currentThread().getId());
    public long nextChaosNanoTime = 0;
  }

  public void doChaos() {
    ThreadData data = threadLocalData.get();
    Random random = data.random;
    if (System.nanoTime() > data.nextChaosNanoTime) {
      // waste time for a while
      int i = 0;
      int limit = random.nextInt(MaxLoopsPerDelay);
      for (int j = 0; j < limit; j++) {
        i += j;
      }

      // just to prevent JIT from eliminating the above loop completely
      DummyField = i;

      data.nextChaosNanoTime = System.nanoTime() + random.nextInt(MaxNanosBetweenDelays);
    }
  }
}
