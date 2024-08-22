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
package optimus.profiler.recipes;

import java.util.ArrayList;
import java.util.Collection;

import optimus.graph.OGTraceReader;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.PNodeTaskInfo;

public class NodeStacks {
  public static boolean CALLER = false;
  public static boolean CALLEE = true;

  private static class Worker {
    PNodeTaskInfo[] allInfos; // Index based PNTI
    boolean skipNonCacheable;

    Worker(PNodeTaskInfo[] allInfos, boolean skipNonCacheable) {
      this.allInfos = allInfos;
      this.skipNonCacheable = skipNonCacheable;
    }

    boolean elide(PNodeTaskInfo pnti) {
      if (pnti == null) return false;
      if (skipNonCacheable && !pnti.getCacheable()) return true;
      return pnti.isProfilerProxy();
    }

    void addChildren(PNodeTaskInfoGrp grp, PNodeTask task, boolean toCallees) {
      ArrayList<PNodeTask> children = toCallees ? task.callees : task.callers;
      if (children == null) return;
      for (PNodeTask child : children) {
        if (child.visitedID > 0) continue;
        child.visitedID = 1;
        PNodeTaskInfo childPnti = allInfos[child.infoId()];
        if (elide(childPnti)) {
          addChildren(grp, child, toCallees);
        } else {
          PNodeTaskInfoGrp pntiGrp = grp.childrenMap().get(child.infoId());
          if (pntiGrp == null) {
            pntiGrp = new PNodeTaskInfoGrp(childPnti, grp.level() + 1);
            grp.childrenMap().put(child.infoId(), pntiGrp);
          }
          pntiGrp.count_$eq(pntiGrp.count() + 1);
          pntiGrp.selfTime_$eq(pntiGrp.selfTime() + child.selfTime);
          addChildren(pntiGrp, child, toCallees);
        }
      }
    }
  }

  /** Groups but doesn't complete the processing (done by trim functions...) */
  public static PNodeTaskInfoGrp getAsStack(
      OGTraceReader reader, PNodeTaskInfo pnti, boolean toCallee, boolean skipNonCacheable) {
    Collection<PNodeTask> tasks = reader.getRawTasks();
    PNodeTaskInfoGrp grp = new PNodeTaskInfoGrp(pnti, 0);

    Worker worker = new Worker(reader.getAllTaskInfosCombined(), skipNonCacheable);

    for (PNodeTask task : tasks) {
      if (task.infoId() == pnti.id) {
        grp.count_$eq(grp.count() + 1);
        worker.addChildren(grp, task, toCallee);
      }
    }
    for (PNodeTask task : tasks) {
      task.visitedID = 0;
    }
    return grp;
  }

  /**
   * Groups and aligns 2 readers, but doesn't complete the processing (done by trim functions...)
   */
  public static PNodeTaskInfoGrp getParentsAsStack(
      OGTraceReader org,
      PNodeTaskInfo topOrg,
      OGTraceReader cmp,
      PNodeTaskInfo topCmp,
      boolean skipNonCacheable) {
    PNodeTaskInfoGrp grpOrg = getAsStack(org, topOrg, CALLER, skipNonCacheable);
    if (topCmp != null) {
      PNodeTaskInfoGrp grpCmp = getAsStack(cmp, topCmp, CALLER, skipNonCacheable);
      grpOrg.insertCompareGroups(grpCmp);
    }
    return grpOrg;
  }
}
