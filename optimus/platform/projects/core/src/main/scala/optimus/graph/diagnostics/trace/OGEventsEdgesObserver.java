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
package optimus.graph.diagnostics.trace;

import static optimus.graph.OGTrace.trace;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLongArray;

import optimus.graph.Node;
import optimus.graph.NodeTask;
import optimus.graph.OGTrace;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.platform.EvaluationQueue;

// aka (affectionately known as) FourByFour
public class OGEventsEdgesObserver extends OGEventsGlobalGraphObserver {
  @Override
  public String name() {
    return "edges";
  }

  @Override
  public String title() {
    return name();
  }

  OGEventsEdgesObserver() {
    super();
  }

  @Override
  public String description() {
    return "<html>(Experimental) Track dependencies between nodes (~2MB overhead)</html>";
  }

  @Override
  public boolean includeInUI() {
    return false;
  }

  private static final int length = 8192;
  private static final int shift = 5; // ie 32
  private static final int setMask = 31;

  // ~4k properties, but make this 8k x 8k so that we account for proxy nodeTaskInfos
  private static final int array_length = (length * length) >> shift;
  private static AtomicLongArray _edges;

  private static long edgesAt(int offset) {
    if (_edges == null) _edges = new AtomicLongArray(array_length);
    return _edges.get(offset);
  }

  private static boolean casEdgesAt(int offset, long preVal, long newVal) {
    if (_edges == null) _edges = new AtomicLongArray(array_length);
    return _edges.compareAndSet(offset, preVal, newVal);
  }

  private static int indexInMap(int id1, int id2) {
    return (id1 * length + id2) >> shift;
  }

  private static long setBit(int id2) {
    return 1L << (id2 & setMask);
  }

  @Override
  public void dependency(NodeTask fromTask, NodeTask toTask, EvaluationQueue eq) {
    dependency(fromTask.getProfileId(), toTask.getProfileId());
    trace.ensureProfileRecorded(toTask.getProfileId(), toTask);
    super.dependency(fromTask, toTask, eq);
  }

  public static void dependency(int id1, int id2) {
    int offset = indexInMap(id1, id2);
    long setBit = setBit(id2);
    long prevVal, newVal;
    do {
      prevVal = edgesAt(offset);
      if ((prevVal & setBit) != 0) return; // bit already set
      newVal = prevVal | setBit;
    } while (!casEdgesAt(offset, prevVal, newVal));
  }

  /**
   * Don't call it while threads are still writing to edges table! Used in tests after
   * waitForSchedulerToQuiesce call
   */
  public static boolean hasEdge(Node<?> fromTask, Node<?> toTask) {
    return hasEdge(fromTask.getProfileId(), toTask.getProfileId());
  }

  public static void reset() {
    _edges = null;
  }

  public static boolean hasEdge(int id1, int id2) {
    long prevVal = edgesAt(indexInMap(id1, id2));
    long setBit = setBit(id2);
    return (prevVal & setBit) != 0;
  }

  public static ArrayList<PNodeTaskInfo> asAdjacencyView(boolean stableSort) {
    PNodeTaskInfo[] pntis = OGTrace.trace.getCollectedPNTIs();
    ArrayList<PNodeTaskInfo> r = new ArrayList<>();

    for (int i = 0; i < pntis.length; i++) {
      PNodeTaskInfo pnti = pntis[i];
      if (pnti == null) continue;

      OGTrace.trace.connectToLiveProfile(pnti);
      r.add(pnti);
      ArrayList<PNodeTaskInfo> row = new ArrayList<>();

      for (int j = 0; j < pntis.length; j++) {
        if (hasEdge(i, j)) row.add(pntis[j]);
      }
      if (!row.isEmpty()) pnti.edges = row;
    }
    if (stableSort) r.sort(Comparator.comparing(PNodeTaskInfo::fullNamePackageShortened));
    return r;
  }
}
