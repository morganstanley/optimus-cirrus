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

import optimus.graph.NodeTask;
import optimus.graph.PropertyNode;
import optimus.graph.diagnostics.PNodeInvalidate;
import optimus.graph.tracking.TrackedNode;
import optimus.graph.tracking.TrackedNodeInvalidationObserver;
import optimus.graph.tracking.TraversalIdSource;

/** Methods to visit TTrack graphs and invalidate the nodes */
public class Invalidators {
  public static void invalidate(
      TTrack ttrack, TraversalIdSource idSource, TrackedNodeInvalidationObserver observer) {
    invalidate(ttrack, null, idSource, observer);
  }

  static PNodeInvalidate invalidate(
      final TTrack ttrack,
      final PNodeInvalidate pninv,
      TraversalIdSource idSource,
      TrackedNodeInvalidationObserver observer) {
    new AllTTrackRefsGraphTraverser() {
      @Override
      protected void visitRef(NodeTask task, TTrackRef tref) {
        task.invalidateCache(observer);
        if (pninv != null) {
          if (task instanceof TrackedNode) {
            pninv.addUTrack((TrackedNode<?>) task);
          } else if (task instanceof PropertyNode<?>) {
            /* User-facing invalidates should not report xsft or cancellation scope proxies because they
              don't represent 'wasted' actual work.

              In the case of cross-scenario this might under-count the impact of invalidation because the underlying
              node may not be able to be reused, whereas proxy might be reusable.

              Adding XS proxies to the invalidate count might be over-counting still because the invalidation of the
              proxy might not result in recomputation of the underlying node (but we can't know that upfront).

              This works for xsft even though underlying nodes of xsft proxies aren't actually in cache:
              because they are cacheable they are ttracked and are reachable through ttracks, so invalidations
              get counted here.
            */
            if (!task.executionInfo().isProxy()) // note check executionInfo v propertyInfo
            pninv.add(((PropertyNode<?>) task).propertyInfo());
          }
        }
      }

      @Override
      protected void postVisit(TTrack ttrack) {
        ttrack.ptracks = Ptracks0.instance;
        ttrack.nodes = TTrackRef.Invalid;
      }
    }.visit(ttrack, idSource);
    return pninv;
  }
}
