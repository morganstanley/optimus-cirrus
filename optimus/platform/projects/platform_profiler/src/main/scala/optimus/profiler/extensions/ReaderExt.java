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
package optimus.profiler.extensions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;

import optimus.graph.OGTraceReader;
import optimus.graph.diagnostics.NodeName;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.profiler.recipes.PNodeTaskInfoGrp;

public class ReaderExt {
  /** Flat grouping of nodes between start, end */
  public static Collection<PNodeTaskInfoGrp> selectNodes(
      OGTraceReader reader, long start, long end, boolean runningOnly) {
    Collection<PNodeTask> tasks = reader.getRawTasks(false);
    PNodeTaskInfo[] allInfos = reader.getAllTaskInfosCombined(); // Index based PNTI

    HashMap<Integer, PNodeTaskInfoGrp> r = new HashMap<>();
    for (PNodeTask tsk : tasks) {
      if (tsk.isActive(start, end)) {
        boolean matched = !runningOnly || tsk.isRunning(start, end);
        if (matched) {
          PNodeTaskInfoGrp grp = r.get(tsk.infoId());
          if (grp == null)
            r.put(tsk.infoId(), grp = new PNodeTaskInfoGrp(allInfos[tsk.infoId()], 0));
          grp.count_$eq(grp.count() + 1);
          grp.selfTime_$eq(grp.selfTime() + tsk.selfTime);
        }
      }
    }

    return r.values();
  }

  /** find all tasks of this info type */
  public static ArrayList<PNodeTask> getTasksByPnti(
      OGTraceReader reader, PNodeTaskInfo info, long minTime, long maxTime) {
    Collection<PNodeTask> tasks = reader.getRawTasks();
    ArrayList<PNodeTask> r = new ArrayList<>();
    int id = info.id;
    for (PNodeTask task : tasks) {
      if (task.infoId() == id && task.isActive(minTime, maxTime)) r.add(task);
    }
    return r;
  }

  /** Use for testing only when info will queried for values and proxies need to be merged */
  public static PNodeTaskInfo findInfoInHotSpotsByName(OGTraceReader reader, NodeName find) {
    var allInfos = reader.getHotspots();
    return findInfoByName(allInfos.toArray(new PNodeTaskInfo[0]), find);
  }

  public static PNodeTaskInfo findInfoByName(OGTraceReader reader, NodeName find) {
    PNodeTaskInfo[] allInfos = reader.getAllTaskInfosCombined();
    return findInfoByName(allInfos, find);
  }

  public static PNodeTaskInfo findInfoByName(PNodeTaskInfo[] allInfos, NodeName find) {
    for (PNodeTaskInfo pnti : allInfos) {
      if (pnti != null && pnti.nodeName().equals(find)) return pnti;
    }
    return null;
  }

  @SuppressWarnings(
      "SameParameterValue") // Reserved for future use. Sorting is really only useful for test
  // stability
  private static ArrayList<PNodeTask> findTasksByName(
      OGTraceReader reader, NodeName find, boolean sortByStartTime) {
    PNodeTaskInfo pnti = findInfoByName(reader, find);
    if (pnti == null) return new ArrayList<>();
    ArrayList<PNodeTask> r = getTasksByPnti(reader, pnti, Long.MIN_VALUE, Long.MAX_VALUE);
    if (sortByStartTime)
      r.sort(Comparator.comparingLong(p -> p.firstStartTime)); // [SEE_STABLE_ORDERING]
    return r;
  }

  public static ArrayList<PNodeTask> findTasksByNameSorted(OGTraceReader reader, NodeName find) {
    return findTasksByName(reader, find, true);
  }
}
