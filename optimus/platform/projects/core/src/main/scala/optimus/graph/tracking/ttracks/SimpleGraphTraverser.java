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

import optimus.graph.tracking.TraversalIdSource;

/**
 * abstract base for simple ttrack graph traversal. traverses all TTracks where shouldVisit returns
 * true on the callee. doesn't visit TTrackRefs automatically.
 */
abstract class SimpleGraphTraverser extends TTrackRefTraverser {

  public final void visit(Iterable<TTrackRoot> roots, int id) {
    for (TTrack ttrack : roots) {
      doVisit(ttrack, id);
    }
  }

  /**
   * Used if a new traversal id is required to force visits to already-seen nodes (for example,
   * Graph Profiler collects graph statistics per-property, and the TTrack graphs for different
   * properties may overlap. We don't want to skip a visit to a node that is part of the graph for a
   * property just because it has already been visited in a traversal when collecting stats for
   * another property.)
   *
   * @param roots
   * @param idSource - a generator that provides a new traversal ID (guaranteed to be bigger than
   *     any ID used to mark existing nodes in the TTrack graph, so that nodes will be visited in
   *     this traversal)
   */
  public final void visit(Iterable<TTrackRoot> roots, TraversalIdSource idSource) {
    idSource.withFreshTraversalId(
        startId -> {
          visit(roots, startId);
          return startId;
        });
  }

  final void doVisit(TTrack start, int id) {
    if (shouldVisit(start, id)) {
      Frame frame = push(start);
      do {
        // get next caller
        TTrack caller = frame.ttrack.ptracks.at(++frame.breadth);
        // if there was a caller
        if (caller != null) {
          // if we should visit it, push it on the stack (and then we'll loop around and process its
          // ptracks immediately)
          if (shouldVisit(caller, frame.ttrack.lastUpdateTraversalID)) {
            frame = push(caller);
          }
        }
        // else we have processed all callers already (n.b. at(n) == null implies that at(k) == null
        // for all k > n),
        // so we're done and can pop
        else {
          postVisit(frame.ttrack);
          frame = popAndGet();
        }
      } while (frame != null);
    }
  }

  /**
   * @param caller the TTrack we are considering to visit
   * @param calleeId the lastTraversalID of caller's callee
   * @return true if we should visit caller, in which case shouldVisit will be called for each of
   *     caller's callers, else false
   */
  protected abstract boolean shouldVisit(TTrack caller, int calleeId);

  /** called after we are done with all of the ttrack's callers */
  protected void postVisit(TTrack ttrack) {}
}
