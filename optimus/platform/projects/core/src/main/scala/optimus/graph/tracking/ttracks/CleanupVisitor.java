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

import java.util.Iterator;
import java.util.Map;

import optimus.graph.NodeTask;
import optimus.graph.TweakableKey;
import optimus.graph.tracking.CleanupScheduler.InterruptionFlag;
import optimus.graph.tracking.TraversalIdSource;

/*
 * Cleans the TTrack graph by removing garbage-collected NodeTasks and duplicate or redundant edges.
 * If a TTrack has no callers or nodes following a cleanup, its TTrackRoot is removed from the map
 * of TTracks maintained in TweakableTracker.
 *
 * Simplified example of duplicate edge removal:
 * c = a + b, a = b + 3 (ttrack graph will have a redundant edge between b and c, so we can drop it)
 *
 * val i = 1
 * @node(tweak = true) def c = a + b
 * @node(tweak = true) def a = b + 3
 * @node(tweak = true) def b = i + 1
 *
 * The graph (conceptually) looks like this:
 *      c
 *     / \
 *    /   \
 *   a <-- b
 *
 * The edge between b and c is redundant, because if we invalidate b, we have to invalidate a, and
 * then c -- there is no need to maintain the edge between b and c and invalidate c again. We can
 * reduce the graph to this:
 *      c
 *      |
 *      a
 *      |
 *      b
 *
 * We can detect that we have already found a path between b and c using a second traversal ID. The
 * algorithm runs something like this:
 * 0. Push b onto the stack (b.traversalID = 0 > startID? No => visit)
 *    (startID = 0, currentID = 0, b.traversalID = 0, b.minIDSeenBefore = INF)
 *
 * 1. a is a caller of b so push a onto the stack (a.traversalID = 0 > startID? No => visit)
 *    (currentID = 1, a.traversalID = 0, a.minIDSeenBefore = INF)
 *     b.minIDSeenBefore = INF > a.traversalID = 0 => b.minIDSeenBefore = 0
 *     a.traversalID = currentID = 1
 *
 * 2. c is a caller of a so push c onto the stack (c.traversalID = 0 > startID? No => visit)
 *    (currentID = 2, c.traversalID = 0)
 *     a.minIDSeenBefore = INF > c.traversalID = 0 => a.minIDSeenBefore = 0
 *     c.traversalID = currentID = 2
 *
 * 3. c has no callers so pop c
 *
 * 4. a has no parents so pop a
 *
 * 5. c is a caller of b so 'push' c onto the stack (c.traversalID = 2 > startID? Yes => seen before)
 *    (startID = 0, currentID = 3, c.traversalID = 2)
 *     b.minIDSeenBefore = 0 < c.traversalID = 2 => this is a duplicate edge, drop it
 *
 *     Note - this is not really a push in the algorithm below (c doesn't go on the stack again)
 *     because we don't want to re-visit the entire sub-graph starting at c. We can still do some
 *     processing based on the previous and current stack frames.
 *
 * 6. c has no callers so 'pop' c
 *
 * 7. b has no more callers so pop b
 *
 * Notes:
 * - The order in which we visit b's callers is not guaranteed. The algorithm won't work if we
 *   traverse the duplicate edge first (ie, if we push c onto the stack before a) because we don't
 *   know the first time round that there is another path from b to c
 * - The actual graph generated for the code snippet above is slightly different because we include
 *   links to tweakable nodes
 *
 *
 * Note: We only do the redundant edge optimization if the redundant edge has only one caller.
 * Otherwise, we can end up with a situation where we add more new edges than we removed,
 *
 *     a   b   c
 *      \  |  /
 *  <redundant edge>
 *      /  |  \
 *     x   y   z
 *
 * If we drop the middle node, then we end up with 9 edges rather than 6 (each of x, y and z to each
 *  of a, b, c).
 */
final class CleanupVisitor extends AdvancedGraphTraverser {
  // statistics counters (for information only - do not affect the algorithm)
  int edgesTraversed,
      edgesRemoved,
      rootsTraversed,
      rootsRemoved,
      nodeRefsRemoved,
      redundantTTracksRemoved,
      excessCapacityRemoved = 0;

  @Override
  protected void postVisit(TTrack ttrack) {
    excessCapacityRemoved += ttrack.ptracks.compress(ttrack);
  }

  @Override
  protected void edge(Frame callee, TTrack caller, int prevMinCallerID, int prevInitID, int curID) {
    TTrack ttrack = callee.ttrack;
    int minSeenBefore = prevMinCallerID;
    // if the previous ttrack we processed and popped (ie, our caller) was a redundant edge, rewire
    // current ptracks to point directly to its caller, bypassing it altogether

    // if previous (ie, caller) ttrack ID before we visited it was lower than the minimum caller ID
    // it found when traversing its callers, reset our minCallerID to that.
    if (caller.nodes == TTrackRef.Nil) {
      if (caller.ptracks.at(1) == null) {
        TTrack onlyCaller = caller.ptracks.at(0);
        if (onlyCaller != null) {
          ttrack.ptracks.set(callee.breadth, ttrack, onlyCaller);
          redundantTTracksRemoved++;
          if (curID >= 0) {
            minSeenBefore = onlyCaller.lastUpdateTraversalID;
            onlyCaller.lastUpdateTraversalID = curID + 1;
          }
        } else {
          /* will drop later */
        }
      }
    } else if (prevInitID < minSeenBefore) minSeenBefore = prevInitID;

    // update our minCallerID if we found a lower one
    if (callee.minCallerID > minSeenBefore) callee.minCallerID = minSeenBefore;
    // if our current traversalID is lower than the minCallerID (ie, the caller's traversal ID in
    // the usual case) then we can drop the edge - it's a redundant duplicate
    if (ttrack.lastUpdateTraversalID < minSeenBefore || caller.nodes == TTrackRef.Invalid) {
      ttrack.ptracks.drop(callee.breadth, ttrack);
      edgesRemoved++;
      callee.breadth--; // Need to retry this slot
    }
  }

  // clean up the nodes on the TTrack
  @Override
  protected void preVisit(TTrack ttrack) {
    // we want to keep the Invalid/Nil as a sentinel, so always keep valid nodes!

    // In principle `nodes` should never be null while we are cleaning up, because it is only null
    // while evaluations are in progress, and evaluations are never run during clean up... In
    // practice, it can be null for non-completable nodes.
    if (ttrack.nodes != null) {
      // Eat all expired nodes at the front
      while (ttrack.nodes.get() == null) {
        TTrackRef next = ttrack.nodes.nref;
        if (next == null) {
          // keeps Invalid or Nil unchanged
          // a ref that gets its referent cleared by GC transforms to Nil
          if (ttrack.nodes != TTrackRef.Invalid && ttrack.nodes != TTrackRef.Nil) {
            ttrack.nodes = TTrackRef.Nil;
            nodeRefsRemoved++;
          }
          return;
        }
        ttrack.nodes = next;
        nodeRefsRemoved++;
      }
      TTrackRef prevNode = ttrack.nodes;
      TTrackRef cnodes = ttrack.nodes.nref;
      while (cnodes != null) {
        NodeTask n = cnodes.get();
        if (n == null) {
          prevNode.nref = cnodes.nref;
          nodeRefsRemoved++;
        } else prevNode = cnodes;
        cnodes = cnodes.nref;
      }
    }
  }

  protected void clean(
      Iterator<Map.Entry<TweakableKey, TTrackRoot>> iterator,
      TraversalIdSource idSource,
      InterruptionFlag interrupt) {
    idSource.withFreshTraversalId(
        startID -> {
          int endID = startID;
          while (iterator.hasNext() && !interrupt.isInterrupted()) {
            Map.Entry<TweakableKey, TTrackRoot> e = iterator.next();
            TTrack ttrack = e.getValue();
            TweakableKey key = e.getKey();
            // if tracking has been disabled (presumably recently since we have ttracks for it!)
            // just drop completely
            if (!key.propertyInfo().trackForInvalidation()) {
              iterator.remove();
              rootsRemoved++;
            } else { // else run cleanup and drop only if empty after cleanup
              endID = doVisit(ttrack, endID, startID);
              if (ttrack.ptracks.at(0) == null && ttrack.nodes == TTrackRef.Nil) {
                iterator.remove();
                rootsRemoved++;
              }
            }
            rootsTraversed++;
          }
          edgesTraversed += endID - startID;
          return endID;
        });
  }
}
