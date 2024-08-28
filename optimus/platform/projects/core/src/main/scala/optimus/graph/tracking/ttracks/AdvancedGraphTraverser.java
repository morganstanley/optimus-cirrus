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
 * A depth-first graph traverser that visits and processes all TTracks and edges, without visiting
 * TTracks more than once.
 */
abstract class AdvancedGraphTraverser extends TTrackRefTraverser {

  public void visit(TTrack start, TraversalIdSource idSource) {
    idSource.withFreshTraversalId(startID -> doVisit(start, startID, startID));
  }

  protected final int doVisit(TTrack start, int traversalID, final int startID) {
    int curID = traversalID;
    preVisit(start);
    Frame frame = push(start);
    start.lastUpdateTraversalID = ++curID;
    do {
      TTrack ttrack = frame.ttrack;
      frame.breadth++;
      TTrack caller = ttrack.ptracks.at(frame.breadth);
      if (caller != null) {
        if (caller.lastUpdateTraversalID < startID) { // ie, we have not already visited this TTrack
          preVisit(caller);
          frame = push(caller);
        } else {
          // we have seen the TTrack before so we don't want to do a full re-visit, but we can
          // process the edge that
          // led us to this TTrack for the second time (e.g. we may be able to drop the edge in
          // cleanup)
          edge(frame, caller, caller.lastUpdateTraversalID, caller.lastUpdateTraversalID, curID);
        }
        caller.lastUpdateTraversalID = ++curID;
      } else {
        postVisit(ttrack);
        Frame prev = frame;
        frame = popAndGet();
        if (frame == null) return curID;
        // if we haven't seen the TTrack before, but still want to process the edge, we pass curID =
        // -1 (e.g in cleanup
        // this means that we don't try to drop this edge as a duplicate but we can still detect a
        // 'middle man' node)
        edge(frame, prev.ttrack, prev.minCallerID, prev.initID, -1);
      }
    } while (true);
  }

  // caller is the previous ttrack we processed (ie, the one we just popped)
  // curID == -1 means that we process the result of the previous call as normal once we pop. A
  // non-negative curID
  // means we have seen the caller ttrack already so don't want to redo the whole visit on that
  // subbranch
  protected void edge(Frame curr, TTrack caller, int prevMinCallerID, int prevInitID, int curID) {}

  protected void preVisit(TTrack ttrack) {}

  protected void postVisit(TTrack ttrack) {}
}
