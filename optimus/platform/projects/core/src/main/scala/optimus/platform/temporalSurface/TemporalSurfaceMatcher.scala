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
package optimus.platform.temporalSurface

import optimus.platform.annotations.nodeSync
import optimus.platform.temporalSurface.operations._
import optimus.graph.Node
import java.io.Serializable

import optimus.dsi.partitioning.Partition

trait TemporalSurfaceScopeMatcher extends Serializable {
  @nodeSync
  def matchScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchScope
  def matchScope$queued(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): Node[MatchScope]

  @nodeSync
  def matchItemScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): MatchItemScope
  def matchItemScope$queued(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): Node[MatchItemScope]
}

trait TemporalSurfaceMatcher extends Serializable {

  /**
   * determine to what extend the operation matches this matcher
   */
  @nodeSync
  def matchQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchResult
  def matchQuery$queued(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): Node[MatchResult]

  /**
   * determine to what extend the operations temporality is determined by this matcher
   */
  @nodeSync
  def matchSourceQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): MatchSourceQuery
  def matchSourceQuery$queued(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface): Node[MatchSourceQuery]

  /**
   * determine to what extend a particular key within the scope of an operation matches this matcher Note - the result
   * may also present the data blob as a side effect
   */
  @nodeSync
  def matchItem(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): Option[(MatchAssignment, Option[operation.ItemData])]
  def matchItem$queued(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
      key: operation.ItemKey): Node[Option[(MatchAssignment, Option[operation.ItemData])]]

  def ||(other: TemporalSurfaceMatcher): TemporalSurfaceMatcher

  // this method is used for notification (TickingSurface)
  private[optimus] def partitions: Set[Partition]

  /**
   * is there any overlap between the data space described in this matcher, and the `namespace` supplied
   * @param namespace
   *   the namespace to test
   * @return
   *   true if this namespace has at least a partial or potential overlap with the matcher
   */
  private[optimus] def matchesNamespace(namespace: String): Boolean
}
