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
package optimus.core;

import java.util.concurrent.TimeUnit;

/**
 * Thread safe fixed-window rate limiter.
 *
 * <p>Note: The implementation uses uses a circular array of timestamps to track the last N calls
 */
public class RateLimiter {
  private static final long INTERVAL = TimeUnit.SECONDS.toNanos(1);
  private final long[] timestamps;
  private int index;

  public RateLimiter(int limitPerSecond) {
    var limit = limitPerSecond > 0 ? limitPerSecond : 1; // Just a protection
    timestamps = new long[limit];
  }

  public synchronized boolean allow() {
    var now = System.nanoTime();
    if (now - timestamps[index] >= INTERVAL) {
      timestamps[index] = now;
      index = (index + 1) % timestamps.length;
      return true;
    }
    return false;
  }
}
