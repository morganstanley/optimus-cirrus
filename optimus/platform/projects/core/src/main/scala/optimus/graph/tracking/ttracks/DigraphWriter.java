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
package optimus.graph.tracking.ttracks;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import optimus.graph.NodeTask;
import optimus.graph.TweakableKey;
import optimus.graph.tracking.TrackedNode;
import optimus.graph.tracking.TraversalIdSource;
import optimus.graph.tracking.UserNodeTracker;
import optimus.platform.util.PrettyStringBuilder;

final class DigraphWriter extends AdvancedGraphTraverser {

  static final String NIL_NAME = "";
  static final String ROOT_SHAPE = "house";
  static final String NULL_COLOUR = "red";

  private PrettyStringBuilder sb = new PrettyStringBuilder();
  private IdentityHashMap<Object, Integer> ids = new IdentityHashMap<>();

  public String visit(
      Iterator<Map.Entry<TweakableKey, TTrackRoot>> iterator, TraversalIdSource idSource) {
    sb.appendln("digraph {");
    sb.appendln("graph[rankdir=\"BT\" ordering=out]");

    ArrayList<Integer> idsOfRoots = new ArrayList<>();
    idSource.withFreshTraversalId(
        startID -> {
          int endID = startID;

          while (iterator.hasNext()) {
            Map.Entry<TweakableKey, TTrackRoot> e = iterator.next();
            TTrack ttrack = e.getValue();
            TweakableKey key = e.getKey();
            idsOfRoots.add(idOf(ttrack, key));
            endID = doVisit(ttrack, endID, startID);
          }
          return endID;
        });

    sb.append("{rank=same ");
    for (Integer i : idsOfRoots) {
      sb.append("n" + i);
      sb.append(" ");
    }
    ;
    sb.appendln("}");
    sb.appendln("}");
    return sb.toString();
  }

  // not great but otherwise we need a dependency in core, and this is just a debugging util
  private boolean skipString(NodeTask ntsk) {
    return (ntsk instanceof TrackedNode) || TTrack.nameOf(ntsk).contains("ConverterHelper");
  }

  private boolean skipString(TweakableKey key) {
    return (key instanceof TrackedNode)
        || (key.propertyInfo()
            .entityInfo
            .runtimeClass()
            .getSimpleName()
            .contains("ConverterHelper"));
  }

  private Integer idOf(TTrack ttrack, TweakableKey key) {
    Integer i = ids.get(ttrack);
    if (i != null) return i;
    i = ids.size() + 1;
    ids.put(ttrack, i);
    sb.append("n" + i);
    sb.append("[label=\"");
    TTrackRef ref = ttrack.nodes; // Add a loop
    if (key != null && !skipString(key))
      sb.append(
          "["
              + key.propertyInfo().entityInfo.runtimeClass().getSimpleName()
              + "."
              + key.propertyInfo().name()
              + "]");
    while (ref != null) {
      NodeTask ntsk = ref.get();
      if (ntsk != null) {
        if (key != null || ref != ttrack.nodes) sb.append("\\n");
        if (!skipString(ntsk)) sb.append(TTrack.nameOf(ntsk));
      }
      ref = ref.nref;
    }

    sb.append("\"");
    if (key != null) {
      sb.append(" shape=");
      sb.append(ROOT_SHAPE);
    }
    if (ttrack.nodes == TTrackRef.Nil) {
      sb.append(" color=");
      sb.append(NULL_COLOUR);
    }
    sb.appendln("]");
    return i;
  }

  @Override
  protected void edge(Frame curr, TTrack caller, int prevMinCallerID, int prevInitID, int curID) {
    int i1 = idOf(caller, null);
    int i2 = idOf(curr.ttrack, null);
    sb.append("n" + i2);
    sb.append("->");
    sb.append("n" + i1);
    sb.appendln(";");
  }
}
