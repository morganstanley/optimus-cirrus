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

import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.dal.QueryTemporality
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.operations._
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData

import scala.collection.immutable.List
import scala.collection.immutable.Map

private[temporalSurface] object QueryDataImpl {
  @node def apply(operation: TemporalSurfaceQuery, ts: TemporalContextImpl): EntityQueryData = {
    operation match {
      case eq: TemporalSurfaceQuery =>
        val surface = ts.findFirstPossiblyMatchingLeaf(eq)
        val noAssignments = Map[eq.ItemKey, List[OperationAssignment[eq.TemporalityType, eq.ItemData]]]()
        EntityQueryDataImpl(eq, surface, 1, complete = false, Nil, noAssignments, noAssignments, None)
    }
  }
}

private[temporalSurface] final case class EntityQueryDataImpl(
    operation: TemporalSurfaceQuery,
    sourceTemporality: LeafTemporalSurface,
    assignmentId: Int,
    complete: Boolean,
    globalAssignments: List[OperationAssignment[QueryTemporality.At, Nothing]],
    individualUnassigned: Map[EntityReference, List[OperationAssignment[QueryTemporality.At, PersistentEntity]]],
    individualAssigned: Map[EntityReference, List[OperationAssignment[QueryTemporality.At, PersistentEntity]]],
    parent: Option[EntityQueryData])
    extends EntityQueryData {
  override def individual: Boolean =
    individualAssigned.nonEmpty || individualUnassigned.nonEmpty

  // TODO (OPTIMUS-24012) this is not yet true, but should become inexpressible due to class hierarchy
  // assert(!individual || globalAssignments.isEmpty)

  protected def doCopy(
      assignmentId: Int,
      complete: Boolean,
      globalAssignments: List[OperationAssignment[TemporalityType, Nothing]],
      individualUnassigned: Map[EntityReference, List[OperationAssignment[TemporalityType, PersistentEntity]]],
      individualAssigned: Map[EntityReference, List[OperationAssignment[TemporalityType, PersistentEntity]]],
      parent: Option[EntityQueryData]): EntityQueryData =
    this.copy(
      assignmentId = assignmentId,
      complete = complete,
      globalAssignments = globalAssignments,
      individualUnassigned = individualUnassigned,
      individualAssigned = individualAssigned,
      parent = parent
    )

  def assignEntire(
      source: List[TemporalSurface],
      context: List[TemporalContext],
      temporalitySource: LeafTemporalSurface): EntityQueryData = {
    val newAssignmentId = this.assignmentId + 1
    if (this.complete) this
    else {
      if (!individual) {
        // global assignment, just set it as the global assignment
        val newGlobal =
          OperationTemporalAssignment(source, newAssignmentId, context, temporalitySource)
        doCopy(
          assignmentId = newAssignmentId,
          complete = true,
          globalAssignments = newGlobal :: this.globalAssignments
        )
      } else {
        // we have individual assignments, so instead of using a global assignment, fill in all unassignments
        val newAssignment =
          OperationTemporalAssignment(source, assignmentId, context, temporalitySource)
        assignIndividualInternal(
          newAssignmentId,
          source,
          context,
          asNode { key: ItemKey =>
            if (individualUnassigned contains key) Some(newAssignment) else None
          })
      }
    }
  }

  def assignEntireError(source: List[_ <: TemporalSurface], context: List[TemporalContext]) = {
    val newAssignmentId = this.assignmentId + 1
    if (complete) this
    else
      parent match {
        case None =>
          val newGlobal = OperationErrorAssignment(source, newAssignmentId, context)
          doCopy(
            assignmentId = newAssignmentId,
            complete = newGlobal.matchAssignment.complete,
            globalAssignments = newGlobal :: this.globalAssignments)
        case Some(parentData) =>
          val err = Some(OperationErrorAssignment(source, newAssignmentId, context))
          assignIndividualInternal(
            newAssignmentId,
            source,
            context,
            asNode { k =>
              if (individualUnassigned.isDefinedAt(k)) err else None
            })
      }
  }

  @node def assignIndividual(
      source: List[TemporalSurface],
      context: List[TemporalContext],
      temporalitySource: LeafTemporalSurface,
      fn: NodeFunction1[ItemKey, Option[(MatchAssignment, Option[ItemData])]],
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData = {
    val newAssignmentId = this.assignmentId + 1
    assignIndividualInternal(
      newAssignmentId,
      source,
      context,
      asNode { k =>
        fn(k) match {
          case Some((AlwaysMatch, None)) =>
            tsProf.foreach(_.recordDataDependantHit())
            Some(OperationTemporalAssignment(source, assignmentId, context, temporalitySource))
          case Some((AlwaysMatch, Some(data))) =>
            tsProf.foreach(_.recordDataDependantHit())
            Some(OperationDataAssignment(source, assignmentId, context, temporalitySource, data))
          case Some(state) => throw new RuntimeException(s"Unexpected assignment status: $state")
          case None        => None
        }
      }
    )
  }

  @node private def assignIndividualInternal(
      newAssignmentId: Int,
      source: List[TemporalSurface],
      context: List[TemporalContext],
      fn: NodeFunction1[ItemKey, Option[OperationAssignment[TemporalityType, ItemData]]]): EntityQueryData = {

    val unassigned: Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]] =
      if (individual) individualUnassigned
      else
        operation.dataAccess
          .getItemKeys(operation, sourceTemporality) // separate for data dependant matches
          .map { key =>
            (key -> Nil)
          }
          .toMap

    // TODO (OPTIMUS-13439): async(...
    val (newCompleted, updatedUnassigned) = unassigned.apar.flatMap { case (k, v) =>
      fn(k) match {
        case None    => None
        case Some(a) => Some(k, a :: v)
      }
    } partition (_._2.head.matchAssignment.complete) // updateUnassigned is for "maybe" matching, which are not completed yet, but its assignment is updated
    val nowUnassigned =
      unassigned ++ updatedUnassigned -- newCompleted.keys // update both keys(eRef) and values(assignment)
    val nowAssigned = individualAssigned ++ newCompleted
    if ((newCompleted isEmpty) && (nowUnassigned isEmpty)) this
    else {
      doCopy(
        assignmentId = newAssignmentId,
        complete = nowUnassigned.isEmpty,
        individualUnassigned = nowUnassigned,
        individualAssigned = nowAssigned,
        parent = parent map { _.withCompletion(operation)(newAssignmentId, newCompleted, nowUnassigned) }
      )
    }
  }

  def withCompletion(otherOp: TemporalSurfaceQuery)(
      newAssignmentId: Int,
      newCompleted_ : Map[otherOp.ItemKey, List[OperationAssignment[otherOp.TemporalityType, otherOp.ItemData]]],
      stillUnassigned_ : Map[otherOp.ItemKey, List[OperationAssignment[otherOp.TemporalityType, otherOp.ItemData]]])
      : EntityQueryData = {

    require(otherOp eq operation)
    require(!complete)
    require(individual)
    require(newAssignmentId > assignmentId)

    val newCompleted = newCompleted_.asInstanceOf[Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]]]
    val stillUnassigned =
      stillUnassigned_.asInstanceOf[Map[ItemKey, List[OperationAssignment[TemporalityType, ItemData]]]]

    require(newCompleted.keySet.forall { individualUnassigned.contains(_) })
    require(stillUnassigned.keySet.forall { individualUnassigned.contains(_) })

    val nowUnassigned = individualUnassigned ++ stillUnassigned -- newCompleted.keys
    val nowAssigned = individualAssigned ++ newCompleted

    val p = parent.get
    doCopy(
      assignmentId = newAssignmentId,
      complete = nowUnassigned.isEmpty,
      individualUnassigned = nowUnassigned,
      individualAssigned = nowAssigned,
      parent = this.parent map { _.withCompletion(operation)(newAssignmentId, newCompleted, stillUnassigned) }
    )
  }

  /**
   * called when we have reached the end of the navigation Reverses the order of the completions
   */
  def completedTreeWalk: EntityQueryData = {
    require(parent isEmpty)

    def reverseMap(kv: (ItemKey, List[OperationAssignment[TemporalityType, ItemData]])) = (kv._1, kv._2.reverse)

    doCopy(
      assignmentId = assignmentId,
      globalAssignments = globalAssignments reverse,
      individualUnassigned = individualUnassigned map reverseMap,
      individualAssigned = individualAssigned map reverseMap
    )
  }
}
