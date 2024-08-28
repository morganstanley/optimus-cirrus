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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import optimus.graph.NodeTask;

/**
 * This visitor allows us to visit each NodeTask in the TTrack graph, which is useful for these
 * basic stats. Could be improved using multiple 'colours' for each traversal. This would give us
 * more accurate 'impact' scores because we would identify shared TTrack nodes in the graph, and
 * exclude them from per-property scores. We should revisit this idea if/when more accurate data is
 * needed, but for now the figures collected are enough to give useful information for identifying
 * expensive nodes (in terms of tracking).
 */
final class TTrackStatsCollector extends MarkingGraphTraverser {
  // branching, ie, breadth
  public final Histogram branchingStats = new Histogram(new UniformReservoir());
  public final Histogram nodeRefsPerTTrackStats = new Histogram(new UniformReservoir());
  public int numNodes; // sum of lengths of TTrack.nodes linked lists
  // number of invalidated but not GCd nodes - maybe should have pruned earlier
  public int numInvalids;
  public int numNils; // number of non-cacheable nodes
  public int numInvalidatedAndCleared; // number of invalidated and GCd nodes (ie, little nulls)

  // Note - this is currently only used here and not tested or
  // displayed in profiler. May want to include this later.
  public int graphSize; // number of TTracks in graph

  private int nodeRefsOnThisTTrack;

  @Override
  public void preVisit(TTrack ttrack) {
    graphSize += 1;
    nodeRefsOnThisTTrack = 0;
    visitAllRefs(ttrack.nodes);
    numNodes += nodeRefsOnThisTTrack;
    nodeRefsPerTTrackStats.update(nodeRefsOnThisTTrack);
  }

  /** Reports only Nodes */
  @Override
  public void visitRef(NodeTask ntsk, TTrackRef tref) {
    nodeRefsOnThisTTrack++;
  }

  // called when the WeakReference returns null - node has been garbage collected already
  @Override
  public void visitRef(TTrackRef tref) {
    if (tref == TTrackRef.Nil) numNils += 1;
    else if (tref == TTrackRef.Invalid) numInvalids += 1;
    else
      numInvalidatedAndCleared += 1; // ie, tref.get == null, and it's neither a Invalid nor a Nil
  }

  @Override
  public void postVisit(TTrack ttrack) {
    branchingStats.update(top().breadth);
  }
}
