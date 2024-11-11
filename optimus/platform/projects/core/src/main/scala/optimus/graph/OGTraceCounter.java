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
package optimus.graph;

import java.util.concurrent.atomic.AtomicInteger;

import optimus.core.CoreHelpers;
import optimus.graph.diagnostics.messages.AdaptedCounter;
import optimus.graph.diagnostics.messages.DALNodesInFlightCounter;

public class OGTraceCounter {
  public static final int DISCRETE_VALUE = 0;
  public static final int BOOKMARK = 1; // just draw line

  public static final int SAMPLED_VALUE = 2; // Line drawn between all the values
  // Sample has a start and an end and rectangle drawn for those lines
  public static final int BLOCK_VALUE = 3;

  public enum UIState {
    Awake,
    Idle
  }

  /**
   * In order to maintain backward compatibility IDs should never change, it's OK to leave holes in
   * this list
   */
  // Note: also add new counters to AllOGCounters.allCounters
  public enum CounterId {
    ADAPTED_NODES_ID,
    DAL_NODES_ID,
    DAL_REQS_FLIGHT_ID,
    NATIVE_MEMORY_ID,
    HEAP_MEMORY_ID,
    GC_ID,
    CACHE_ENTRIES_ID,
    CACHE_CLEARS_ID,
    SYNC_STACKS_ID,
    BLOCKING_ID,
    DAL_REQS_ID,
    CONTENDED_MONITOR_ID,
    BOOKMARKS_ID,
    USER_ACTIONS_ID,
    ACTIVE_GUI_ID,
    IDLE_GUI_ID,
    MAX_ID,
    STARTUP_ID,
    HANDLER_ID,
    DTQ_ID;

    public final int id;

    CounterId() {
      this.id = ordinal();
    }
  }

  public static void resetAdaptedCountersForTest() {
    adaptedCount.set(0);
    adaptedDALCount.set(0);
  }

  private static final AtomicInteger adaptedCount = new AtomicInteger(0);
  private static final AtomicInteger adaptedDALCount = new AtomicInteger(0);

  public static void adaptedChangeBy(PluginType pluginType, int value) {
    if (pluginType == PluginType.DAL())
      DALNodesInFlightCounter.report(adaptedDALCount.addAndGet(value));
    AdaptedCounter.report(adaptedCount.addAndGet(value));
  }

  public static void ensureLoaded() {}

  static {
    CoreHelpers.registerGCListener(new OGTraceGCListener());
  }
}
