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
package optimus.platform.temporalSurface.impl

import optimus.graph.Node
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.operations._
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData

private[temporalSurface] object EntityQueryData {
  def unapply(x: EntityQueryData): Option[
    (
        LeafTemporalSurface,
        Boolean,
        Boolean, //
        List[OperationAssignment[x.TemporalityType, Nothing]], //
        Map[_ <: x.ItemKey, List[OperationAssignment[x.TemporalityType, x.ItemData]]], //
        Map[_ <: x.ItemKey, List[OperationAssignment[x.TemporalityType, x.ItemData]]])
  ] = //
    if (x == null) None
    else
      Some(
        x.sourceTemporality,
        x.complete,
        x.individual,
        x.globalAssignments,
        x.individualAssigned,
        x.individualUnassigned)
}
private[temporalSurface] trait EntityQueryData {
  type TemporalityType = operation.TemporalityType
  type ItemKey = operation.ItemKey
  type ItemData = operation.ItemData

  val assignmentId: Int
  val operation: TemporalSurfaceQuery
  val sourceTemporality: LeafTemporalSurface
  val complete: Boolean
  val globalAssignments: List[OperationAssignment[TemporalityType, Nothing]]

  /**
   * there are individual assignments (separated as they may be not values, but all of none => still complete)
   */
  def individual: Boolean
  val individualAssigned: Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]]
  val individualUnassigned: Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]]
  val parent: Option[EntityQueryData]

  def assignEntire(
      source: List[TemporalSurface],
      context: List[TemporalContext],
      temporalitySource: LeafTemporalSurface): EntityQueryData
  def assignEntireError(source: List[_ <: TemporalSurface], context: List[TemporalContext]): EntityQueryData
  @nodeSync def assignIndividual(
      source: List[TemporalSurface],
      context: List[TemporalContext],
      temporalitySource: LeafTemporalSurface,
      fn: NodeFunction1[ItemKey, Option[(MatchAssignment, Option[ItemData])]],
      tsProf: Option[TemporalSurfaceProfilingData]
  ): EntityQueryData
  def assignIndividual$queued(
      source: List[TemporalSurface],
      context: List[TemporalContext],
      temporalitySource: LeafTemporalSurface,
      fn: NodeFunction1[ItemKey, Option[(MatchAssignment, Option[ItemData])]],
      tsProf: Option[TemporalSurfaceProfilingData]): Node[EntityQueryData]

  def withCompletion(otherOp: TemporalSurfaceQuery)(
      newAssignmentId: Int,
      newCompleted: Map[otherOp.ItemKey, List[OperationAssignment[otherOp.TemporalityType, otherOp.ItemData]]],
      stillUnassigned: Map[otherOp.ItemKey, List[OperationAssignment[otherOp.TemporalityType, otherOp.ItemData]]])
      : EntityQueryData

  protected def doCopy(
      assignmentId: Int,
      complete: Boolean = complete,
      globalAssignments: List[OperationAssignment[TemporalityType, Nothing]] = globalAssignments,
      individualUnassigned: Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]] = individualUnassigned,
      individualAssigned: Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]] = individualAssigned,
      parent: Option[EntityQueryData] = parent): EntityQueryData

  /**
   * called when we have reached the end of the navigation Reverses the order of the completions
   */
  def completedTreeWalk: EntityQueryData
}
