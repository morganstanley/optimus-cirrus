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

/** A graph traverser which calculates the maximum depth of any TTrack */
final class GraphMaxDepthTraverser extends SimpleGraphTraverser {
  private int startId;
  private int maxId;

  public int maxDepth(Iterable<TTrackRoot> roots, TraversalIdSource idSource) {
    idSource.withFreshTraversalId(
        startId -> {
          this.startId = startId;
          this.maxId = startId;
          visit(roots, startId);
          return maxId;
        });
    // since we increment the ID once per level, maxId - startId is the total number of levels
    // we've traversed, and we deduct 1 because we don't want to count the TTrack root (for backward
    // compatibility with the old traversers)
    return maxId - startId - 1;
  }

  @Override
  protected boolean shouldVisit(TTrack caller, int calleeId) {
    // we mark each level with an ID one higher than the level before.
    // we only re-traverse a node if we encounter it again at a higher depth than
    // we found it before, and we track the maximum ID for all nodes.
    int nextLevelId = calleeId + 1;
    if (caller.lastUpdateTraversalID < nextLevelId) {
      caller.lastUpdateTraversalID = nextLevelId;
      if (nextLevelId > maxId) maxId = nextLevelId;
      return true;
    } else return false;
  }
}
