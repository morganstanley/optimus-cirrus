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
 * A simple graph traverser which sets the lastUpdateTraversalId an all visited nodes and avoids
 * re-visiting already marked nodes
 */
class MarkingGraphTraverser extends SimpleGraphTraverser {
  public final void visit(TTrack root, TraversalIdSource idSource) {
    idSource.withFreshTraversalId(
        startId -> {
          doVisit(root, startId);
          return startId;
        });
  }

  @Override
  protected final boolean shouldVisit(TTrack caller, int calleeId) {
    if (caller.lastUpdateTraversalID != calleeId) {
      preVisit(caller);
      caller.lastUpdateTraversalID = calleeId;
      return true;
    } else return false;
  }

  /** called before we visit each previous unmarked TTrack */
  protected void preVisit(TTrack ttrack) {}
}
