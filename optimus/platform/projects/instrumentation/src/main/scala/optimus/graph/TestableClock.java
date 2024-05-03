/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the Licejnse at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.graph;

import optimus.graph.diagnostics.ProfiledEvent;

public class TestableClock {

  private static volatile NanoClock clock = null;

  public static final String allowTestableClockSysProp = "optimus.scheduler.allowTestableClock";
  public static final boolean allowTestableClock =
      DiagnosticSettings.getBoolProperty(allowTestableClockSysProp, false);

  public static long nanoTime() {
    if (allowTestableClock) {
      NanoClock c = clock;
      if (c != null) {
        return c.nanoTime();
      }
    }
    return System.nanoTime();
  }

  /**
   * Events with a 'happens before'/'happens after' relationship must have a synchronised view of a
   * global clock.
   *
   * <p>'Happens before' state must be published so that we can register the time of that event. We
   * publish local time, which is the minimum global time for this thread. Note: by observing events
   * from other threads we continuously topologically sort our events.
   *
   * <p>e.g. Called when node task events can be 'published', such as: 1. Node made visible to be
   * consumed, potentially by other threads (start is observed after the node is visible) 2.
   * Completed and the result can be consumed (result is observed after node completes)
   *
   * <p>However, we have to remember to do this in initAsCompleted and initAsFailed, since nodes
   * that call these methods don't follow the normal start -> complete -> stop code path, and might
   * not even be visible to profiler in the case of overrides to CompletableNode instead of
   * CompletableNodeM.
   */
  public static void publishEvent(int eventId, ProfiledEvent state) {
    if (TestableClock.allowTestableClock) {
      NanoClock c = clock;
      if (c != null) c.publishEvent(eventId, state);
    }
  }

  /**
   * Update the local view of the global clock by synchronising matching 'happens after'/'happens
   * before' events. Don't forget to call this method BEFORE nanoTime calls.
   *
   * <p>e.g. For node task events, this is called in two places: 1. When a node starts, since the
   * ProfiledNodeState.started event has already been published by that time 2. When we take a
   * dependency on a child node, since a caller who uses the child result will always call
   * OGTrace.dependency, AND when this call happens we are guaranteed that all profiling data on
   * timings is ready to be read by parents
   */
  public static void consumeEvent(int eventId, ProfiledEvent state) {
    if (TestableClock.allowTestableClock) {
      NanoClock c = clock;
      if (c != null) c.consumeEvent(eventId, state);
    }
  }

  public static void consumeTimeInMs(long ms, NanoClockTopic topic) {
    if (TestableClock.allowTestableClock) {
      NanoClock c = clock;
      if (c != null) c.consumeTimeInMs(ms, topic);
    }
  }

  public static void setClock(NanoClock clock_) {
    if (TestableClock.allowTestableClock) clock = clock_;
    else
      throw new IllegalArgumentException(
          "you must set -D"
              + TestableClock.allowTestableClockSysProp
              + "=true to use OGTrace.setClock");
  }
}
