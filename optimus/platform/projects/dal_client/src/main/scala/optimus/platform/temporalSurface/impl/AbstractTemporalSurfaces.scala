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

import java.time.Instant
import optimus.graph.DiagnosticSettings
import optimus.graph.Settings
import optimus.platform._
import optimus.platform.dal.{EntityResolver, QueryTemporality}
import optimus.platform.dal.EntitySerializer
import optimus.platform.storable._
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.operations._
import optimus.platform.AsyncImplicits._
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData

import scala.annotation.nowarn
import scala.collection.mutable

trait TemporalSurfaceImpl { this: TemporalSurface =>
  def temporalSurface = this

  private[optimus] def resolverOption: Option[EntityResolver] = None
  // may be null
  private[optimus] def tag: Option[String]
}

trait TemporalContextImpl extends TemporalSurfaceImpl with TemporalContext {
  this: TemporalSurface =>
  override type surfaceForFrozenType <: TemporalContextImpl

  /**
   * Finds the exact temporality which matches the specified query, throwing an exception if there is any ambiguity
   * (e.g. if different temporalities might apply to different parts of the query result).
   */
  // finds all possible matching leaves not just the first one
  // if more than one leaf matches and the vt,tt is the same for all, use that vt,tt
  // otherwise throw an exception
  @node final def operationTemporalityFor(operation: TemporalSurfaceQuery): operation.TemporalityType = {
    findPossiblyMatchingLeaves(operation, false) match {
      case (AlwaysMatch, surfaces) => // all leaves that Always match the query
        val first = surfaces.head.temporalSurface.currentTemporality
        if (
          surfaces.tail.exists { surface =>
            surface.temporalSurface.currentTemporality != first
          }
        ) {
          log.warn(
            s"cannot tell source temporality from $this for $operation, possible matches: $surfaces. Node call stack ${EvaluationContext.currentNode
                .waitersToNodeStack(false, true, false, 9999)}")
          throw new TemporalSurfaceTemporalityException(s"cannot determine single temporality of $operation from $this")
        }
        first
      case (result, leafTemporalSurfaces) =>
        log.warn(
          s"cannot tell source temporality from $this for $operation, possible matches: $leafTemporalSurfaces with result $result. Node call stack ${EvaluationContext.currentNode
              .waitersToNodeStack(false, true, false, 9999)}")
        throw new TemporalSurfaceTemporalityException(s"cannot determine the temporality of $operation from $this")
    }
  }

  /**
   * Finds the first leaf surface which could match any part of the specified query operation. This method is imprecise
   * because depending on the data in the DAL, other leaf surfaces might match some or all of the query.
   */
  @node final def findFirstPossiblyMatchingLeaf(operation: TemporalSurfaceQuery): LeafTemporalSurface = {
    findPossiblyMatchingLeaves(operation, true) match {
      case (AlwaysMatch, leafTemporalSurface :: _) => leafTemporalSurface.temporalSurface
      case (CantTell, leafTemporalSurface :: _)    => leafTemporalSurface.temporalSurface
      case (NeverMatch, _) =>
        log.warn(
          s"cannot tell source temporality from $this for $operation. Node call stack ${EvaluationContext.currentNode
              .waitersToNodeStack(false, true, false, 9999)}")
        throw new TemporalSurfaceTemporalityException(s"cannot determine the temporality of $operation from $this")

      // the following cases cant happen (ie meaningless)
      case (AlwaysMatch, Nil) =>
        throw new IllegalStateException(
          "Should not happen contact the graph team - can't always match and not have a TS")
      case (CantTell, Nil) =>
        throw new IllegalStateException(
          "Should not happen contact the graph team - can't not be able to tell and not have a TS")
    }
  }

  // TODO (OPTIMUS-13434): @si?
  @node protected def load(pe: PersistentEntity, storageInfo: StorageInfo): Entity = {
    // TODO (OPTIMUS-13434): do we need to generate DSIStorageInfo here?
    // val storageInfo = DSIStorageInfo.fromPersistentEntity(pe)
    val e = EntitySerializer.deserialize(pe.serialized, this, storageInfo)
    e
  }

  /**
   * determine the assignments from a completed assignment. requires that assignments all have a common root
   */
  @node private def determineCompletedAssignment(data: EntityQueryData)(
      key: data.operation.ItemKey): (TemporalContext, data.operation.ItemData) = {
    val globalOptions = data.globalAssignments
    val individualOptions =
      if (!data.individual)
        Nil
      else
        data.individualAssigned.get(key) orElse data.individualUnassigned.get(key) orElse None getOrElse Nil

    val options =
      if (globalOptions isEmpty) individualOptions
      else if (individualOptions isEmpty) globalOptions
      else {
        (globalOptions ++ individualOptions).sortBy(_.assignmentId)
      }
    require(options.nonEmpty)
    // determine the common temporal context
    // this is the common shared root of the possible temporal contexts
    val tc = options.foldLeft(options.head.context) { (current, option) =>
      current.dropWhile { !option.context.contains(_) }
    }
    // determine the current swimlane for the item
    val currentSwimline = options.collectFirst {
      case a @ OperationAssignment(source, assignmentId, matchAssignment: CompleteMatchAssignment, context) => a
    }
    currentSwimline match {
      case None | Some(_: OperationErrorAssignment) =>
        log.error(s"cannot determine temporal context for ${data.operation.itemKeyName} $key - $currentSwimline")
        throw new IllegalStateException("cannot determine temporal context")
      case Some(OperationDataAssignment(source, _, _, temporalitySource, _)) =>
        (tc.head, data.operation.dataAccess.getSingleItemData(data.operation, temporalitySource)(key))
      case Some(OperationTemporalAssignment(source, _, _, temporalitySource)) =>
        (tc.head, data.operation.dataAccess.getSingleItemData(data.operation, temporalitySource)(key))
    }
  }

  // get here via closures from DalExProv#get via IterableRewriter
  /** see [[optimus.platform.TemporalContext#dataAccess]] */
  @node override private[optimus] final def dataAccess(operation: TemporalSurfaceQuery): operation.ResultType = {
    val currentTsProf = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(this)
    pluginHack.start(this, operation, currentTsProf).asInstanceOf[operation.ResultType]
  }

  // <~ pluginHack.start <~ TempCtxImpl#dataAccess <~ EntResReadImpl#findByIndex <~ DALExProvider#get
  @node private[impl] final def dataAccessImpl(
      operation: TemporalSurfaceQuery,
      tsProf: Option[TemporalSurfaceProfilingData]): operation.ResultType = { // operation contains query key
    import operations.OperationHelper._
    val start = this :: Nil
    val data = QueryDataImpl(operation, this)
    val treeResult = queryTemporalSurface(start, data, start, true, tsProf).completedTreeWalk

    type ResultTemporalityType = treeResult.operation.TemporalityType

    val dataResult = treeResult match {
      case EntityQueryData(_, false, true, _, assigned, unassigned) if unassigned.exists(_._2.isEmpty) =>
        // no complete assignments possible we have individual assignments still unassigned, and with no options to resolve
        throw new TemporalSurfaceNoDataException(
          s"op=${operation.getClass}, assigned=$assigned, unassigned=$unassigned: individual assignments still unassigned")
      case EntityQueryData(_, false, false, Nil, assigned, unassigned) =>
        // no complete assignments possible no individual assignments and no global ones
        // TODO (OPTIMUS-13434): consider a check for no data before we throw
        throw new TemporalSurfaceNoDataException(
          s"op=${operation.getClass}, assigned=$assigned, unassigned=$unassigned: no complete assignments possible no individual assignments and no global ones")

      case EntityQueryData(
            _,
            true,
            false,
            (assignment: OperationTemporalAssignment[ResultTemporalityType] @unchecked) :: Nil,
            _,
            _) =>
        // happy days! - we have a single global assignment
        log.debug(s"dataAccess - query: $operation - single global assignment: $assignment")
        val tsUsed = assignment.temporalitySource
        val itemData = operation.dataAccess.getItemData(treeResult.operation, tsUsed, tsUsed)
        val tc = assignment.context.head
        treeResult.operation.toResult(key => (tc, itemData(key)), itemData.keySet)

      case EntityQueryData(_, _, true, Nil, assigned, unassigned) =>
        // individual assignments to work through, but no global ones
        log.debug(s"dataAccess - query $operation - individual assigned $assigned, unassigned $unassigned")
        val assigner = determineCompletedAssignment(treeResult) _
        val keys = treeResult.individualAssigned.keys ++ treeResult.individualUnassigned.keySet
        treeResult.operation.toResult(key => assigner(key), keys)
    }
    coerceResult(treeResult.operation, operation)(dataResult)
  }

  /**
   * Finds the exact temporality for the query operation, failing if there is any ambiguity.
   */
  @scenarioIndependent
  @node private[optimus /*platform*/ ] final def findExactTemporality(
      operation: TemporalSurfaceQuery): TemporalQueryResult = {
    val data = QueryDataImpl(operation, this)
    val start = this :: Nil
    val currentTsProf = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(this)
    val treeResult = queryTemporalSurface(start, data, start, true, currentTsProf).completedTreeWalk
    type ResultTemporalityType = treeResult.operation.TemporalityType

    treeResult match {
      case EntityQueryData(
            _,
            true,
            false,
            (assignment: OperationTemporalAssignment[ResultTemporalityType] @unchecked) :: Nil,
            _,
            _) =>
        // happy days! - we have a single global assignment
        log.debug(s"dataAccess - query: $operation - single global assignment: $assignment")
        val tsUsed = assignment.temporalitySource
        val ts = tsUsed.currentTemporality
        EntityTemporalInformation(ts.validTime, ts.txTime, tsUsed, assignment.context)
      case _ => NotPresentInTemporalContext
    }
  }
  @transient final lazy val ttContext: TransactionTimeContext = new SimpleTTContext(this)
}

private[optimus] final class SimpleTTContext(private val underlying: TemporalContextImpl)
    extends TransactionTimeContext {

  override def equals(obj: scala.Any) = obj match {
    case other: SimpleTTContext =>
      (other.underlying eq this.underlying) ||
      ((other.underlying, this.underlying) match {
        case (otherFlatTC: FlatTemporalContext, thisFlatTC: FlatTemporalContext) =>
          (otherFlatTC.tt == thisFlatTC.tt) && (otherFlatTC.matcher == thisFlatTC.matcher)
        case (otherBranchTC: BranchTemporalSurface, thisBranchTC: BranchTemporalSurface) =>
          (otherBranchTC.scope == thisBranchTC.scope) && (otherBranchTC.children.zip(thisBranchTC.children) forall {
            case (otherChild: FlatTemporalSurface, thisChild: FlatTemporalSurface) =>
              (otherChild.tt == thisChild.tt && otherChild.matcher == thisChild.matcher)
            case _ => false
          })
        case _ => false
      })
    case _ => false
  }
  override def hashCode() = underlying.hashCode

  private[optimus] override def getTTForEvent(eventClassName: String) = {
    val eventNamespace = eventClassName.substring(0, eventClassName.lastIndexOf('.'))
    def collectTTs(tc: TemporalSurface): Seq[Instant] = {
      val tts = mutable.ListBuffer.empty[Instant]

      @nowarn("msg=10500 optimus.platform.temporalSurface.impl.FixedTracingTemporalSurface")
      def innerCollectTTs(tc: TemporalSurface): Unit = tc match {
        case leaf: FixedLeafTemporalSurfaceImpl =>
          if (leaf.matcher.matchesNamespace(eventNamespace)) tts += leaf.tt
        case tracingLeaf: FixedTracingTemporalSurface => innerCollectTTs(tracingLeaf.content)
        case branch: FixedBranchTemporalContext       => branch.children.foreach(innerCollectTTs)
        case branch: FixedBranchTemporalSurfaceImpl   => branch.children.foreach(innerCollectTTs)
        case x =>
          throw new IllegalArgumentException(
            s"getTTForEvent is only supported for FixedBranchTemporalContext and FixedTracingTemporalSurface, not $x. " +
              s"SimpleTTContext is ${underlying.details}")
      }

      innerCollectTTs(tc)
      tts.toList
    }

    underlying match {
      case branchTemporalSurface: BranchTemporalSurface =>
        require(
          branchTemporalSurface.scope == DataFreeTemporalSurfaceMatchers.allScope,
          "Matcher scope must be Matcher.all for getTTForEvent")
        val temporalSurfaceMatchedTTs = collectTTs(branchTemporalSurface)
        if (temporalSurfaceMatchedTTs.isEmpty)
          throw new IllegalArgumentException(s"No valid child surfaces found for $eventClassName")
        else temporalSurfaceMatchedTTs.head
      case _ => unsafeTxTime
    }
  }

  private[optimus] override def unsafeTxTime = {
    var currentTT: Option[Instant] = Option.empty[Instant]
    def collectTT(ts: TemporalSurface): Unit = {
      ts match {
        case leaf: LeafTemporalSurfaceImpl if (leaf.canTick) =>
          throw new UnsupportedOperationException(
            s"Cannot determine the ttContext of $this as it contains a ticking surface")
        case leaf: LeafTemporalSurfaceImpl =>
          currentTT match {
            case None => currentTT = Some(leaf.currentTemporality.txTime)
            case Some(tt) =>
              if (leaf.currentTemporality.txTime != tt)
                throw new UnsupportedOperationException(
                  s"Cannot determine the ttContext of $this as it contains more that one TT. $tt and ${leaf.currentTemporality.txTime}")
          }
        case branch: BranchTemporalSurfaceImpl =>
          branch.children foreach collectTT
        case _ =>
          throw new UnsupportedOperationException(s"Custom temporal surfaces are not supported $ts ${ts.getClass} ")
      }
    }

    collectTT(underlying)
    currentTT.getOrElse(
      throw new UnsupportedOperationException(s"Cannot determine the ttContext of $this as it contains no TTs"))
  }

  private[optimus] override def frozen =
    if (underlying.canTick) new SimpleTTContext(underlying.frozen()) else this
}

trait LeafTemporalSurfaceImpl extends LeafTemporalSurface with TemporalSurfaceImpl {

  type leafForFrozenType <: LeafTemporalSurface

  /** see [[optimus.platform.temporalSurface.TemporalSurface#queryTemporalSurface]] */
  @node override private[temporalSurface] def queryTemporalSurface(
      callingScope: List[TemporalSurface],
      queryData: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData = {
    val (time, result) = AdvancedUtils.timed {
      log.debug(s"LeafTemporalSurfaceImpl id ${this.id} processing operation ${queryData.operation}")
      tsProf.foreach(_.recordVisit())

      val query = queryData.operation
      val (time, matcherResult) = AdvancedUtils.timed(matcher.matchQuery(query, this))
      tsProf.foreach(_.recMatchWalltime(time))

      matcherResult match {
        case AlwaysMatch =>
          tsProf.foreach(_.recordHit())
          tsProf.foreach(_.recordMatcherHit())
          queryData.assignEntire(callingScope, temporalContextChain, this)
        case ErrorMatch =>
          queryData.assignEntireError(callingScope, temporalContextChain)
        case NeverMatch => queryData // do nothing
        case CantTell   =>
          //            val currentTemporality = queryData.resolveTemporality(currentTemporality)
          val matcherFn: NodeFunction1[query.ItemKey, Option[(MatchAssignment, Option[queryData.ItemData])]] = asNode {
            key: query.ItemKey =>
              val (time, res) = AdvancedUtils.timed(matcher.matchItem(query, this)(key))
              tsProf.foreach(_.recMatchWalltime(time))
              res
          }
          // TODO(OPTIMUS-17205): Clean the type cast
          val matcherFnCorrectlyTyped =
            matcherFn
              .asInstanceOf[NodeFunction1[queryData.ItemKey, Option[(MatchAssignment, Option[queryData.ItemData])]]]
          val (time, res) = AdvancedUtils.timed(
            queryData.assignIndividual(callingScope, temporalContextChain, this, matcherFnCorrectlyTyped, tsProf))
          tsProf.foreach(_.recDataDepWalltime(time))
          res
      }
    }
    tsProf.foreach(_.recWallTime(time))
    result
  }

  private[optimus] def currentTemporality: QueryTemporality.At

  /**
   * implements [findPossiblyMatchingLeaves]
   */
  @node private[temporalSurface] override def findPossiblyMatchingLeavesImpl(
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      firstOnly: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): (MatchSourceQuery, List[EntityTemporalInformation]) = {
    val (time, result) = AdvancedUtils.timed {
      tsProf.foreach(_.recordVisit())

      val (time, matchRes) = AdvancedUtils.timed(matcher.matchSourceQuery(operation, this))
      tsProf.foreach(_.recMatchWalltime(time))

      val res = matchRes match {
        case r @ (AlwaysMatch | CantTell) =>
          if (r == AlwaysMatch) {
            tsProf.foreach(_.recordHit())
            tsProf.foreach(_.recordMatcherHit())
          }
          val info =
            EntityTemporalInformation(
              currentTemporality.validTime,
              currentTemporality.txTime,
              this,
              temporalContextChain)
          (r, info :: Nil)
        case NeverMatch => (NeverMatch, Nil)
      }

      log.debug(
        s"resolved source temporality from $this for $operation is ${res._1} possible matches leafs are: ${res._2}")
      res
    }
    tsProf.foreach(_.recWallTime(time))
    result
  }

  override protected def descriptionDetail(sep: String) = {
    val s = super.descriptionDetail(sep)
    s"$s${if (!s.isEmpty) sep else ""}currentTemporality=$currentTemporality"
  }

  override protected[this] def hashCodeImpl = {
    val platform = 17
    super.hashCodeImpl * platform + (if (this.canTick) 0 else currentTemporality.hashCode)
  }
  override def extendEquals(ots: TemporalSurface) =
    if (!super.extendEquals(ots)) false
    else
      ots match {
        case lts: LeafTemporalSurfaceImpl if !lts.canTick && !canTick =>
          currentTemporality == lts.currentTemporality
        case _ => false
      }
}

@entity private[temporalSurface] object FixedTemporalContextCache {
  @scenarioIndependent @node def loadEntity(
      tc: FixedTemporalContextImpl,
      pe: PersistentEntity,
      storageInfo: StorageInfo): Entity = tc.doLoad(pe, storageInfo)

  // TODO (OPTIMUS-15821): graph team SMEs to review reactive memory usage. For now, to prevent every entity received
  // via reactive being cached you can optionally disable caching on this node.
  loadEntity_info.setCacheable(
    DiagnosticSettings.getBoolProperty("optimus.platform.temporalSurface.loadEntity.cacheable", true)
  )
}

trait FixedTemporalContextImpl extends TemporalContextImpl with FixedTemporalSurface with TemporalSurfaceImpl {
  // TODO (OPTIMUS-13434): review the correct way to do this
  @scenarioIndependent @node override final def deserialize(pe: PersistentEntity, storageInfo: StorageInfo): Entity =
    FixedTemporalContextCache.loadEntity(this, pe, storageInfo)

  @scenarioIndependent @node private[temporalSurface] final def doLoad(
      pe: PersistentEntity,
      storageInfo: StorageInfo): Entity = load(pe, storageInfo)
}
trait FixedLeafTemporalSurfaceImpl extends FixedTemporalSurface with LeafTemporalSurfaceImpl {

  private[optimus /*platform*/ ] val vt: Instant
  private[optimus /*platform*/ ] val tt: Instant

  override protected def showId = false
  override protected def descriptionDetail(sep: String): String = s"vt=$vt, tt=$tt"
  override protected def descriptionSummary(sep: String): String = descriptionDetail(sep)

  // transient lazy vals as there are not required for all use cases, but may are still good to cache when generated
  @transient final protected[optimus] lazy val currentTemporality = QueryTemporality.At(vt, tt)
}

/**
 * basic branch temporal surface implementation
 */
private[optimus] trait BranchTemporalSurfaceImpl extends BranchTemporalSurface with TemporalSurfaceImpl {

  type t = childType

  children.foldLeft(Map[EntityResolver, TemporalSurface]()) { (map, child) =>
    child.resolverOption match {
      case None => map
      case Some(resolver) =>
        val upd = map + (resolver -> child)
        if (upd.size > 1)
          throw new IllegalStateException(
            s"entity resolvers may not be mixed within a temporal context - ${upd.keySet}")
        upd
    }
  }

  /** used by [queryTemporalSurface] to process child temporal surfaces - does a DFS on the children */
  @node private def queryTemporalSurfaceChildren(
      callingScope: List[TemporalSurface],
      operation: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData = {
    val childResult = children.aseq.foldLeft(operation, considerDelegation) {
      (input: (EntityQueryData, Boolean), child) =>
        if (input._1.complete) input
        else {
          tsProf.foreach(_.recordVisitedChildren())
          val childProf =
            tsProf.flatMap(prof => TemporalSurfaceProfilingDataManager.maybeCreateChildProfilingEntry(child, prof))

          // visit will be recorded inside [child.queryTemporalSurface] so no need to record the visit here
          val (operationData, isContext) = input
          val (time, childResult) = AdvancedUtils.timed(
            child.queryTemporalSurface(
              child :: callingScope,
              operationData,
              // append the child to chain if it is a context because we need to pass
              // all enclosing TemporalContexts that we have traversed while considerDelegation was true
              if (isContext) child.addToChain(temporalContextChain) else temporalContextChain,
              isContext,
              childProf
            )
          )

          val stillConsiderDelegation = isContext && childResult.globalAssignments.isEmpty
          (childResult, stillConsiderDelegation)
        }
    }
    childResult._1
  }

  /** see [optimus.platform.temporalSurface.TemporalSurface#queryTemporalSurface] */
  @node override private[temporalSurface] def queryTemporalSurface(
      callingScope: List[TemporalSurface],
      queryData: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData = {
    val (time, result) = AdvancedUtils.timed {
      log.debug(
        s"BranchTemporalSurfaceImpl id ${this.id} processing operation ${queryData.operation}. Consider Delegation = $considerDelegation")
      tsProf.foreach(_.recordVisit())

      val (matchTime, scopeRes) = AdvancedUtils.timed(scope.matchScope(queryData.operation, this))
      tsProf.foreach(_.recMatchWalltime(matchTime))

      scopeRes match {
        case AlwaysMatch =>
          tsProf.foreach(_.recordMatcherHit())
          queryTemporalSurfaceChildren(callingScope, queryData, temporalContextChain, considerDelegation, tsProf)
        case NeverMatch => queryData // no nothing
        case CantTell   => ??? // TODO (OPTIMUS-13434): fix me
        case ErrorMatch => queryData.assignEntireError(callingScope, temporalContextChain)
      }
    }
    tsProf.foreach(_.recWallTime(time))
    result
  }

  /**
   * used by [findPossiblyMatchingLeavesImpl] to process child surfaces
   *
   * @param children
   *   remaining children to consider
   * @param operation
   *   the operatation in question
   * @param temporalContextChain
   *   all enclosing TemporalContexts that we have traversed
   * @param contributingMatchesSoFar
   *   leaves that match so far ( as called recursively)
   * @param tsProf
   *   profiling associated with this TS
   * @return
   *   (AlwaysMatch|CantTell|NeverMatch, List[EntityTemporalInformation])
   */
  @node private def findPossiblyMatchingLeavesInChildren(
      children: List[childType],
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      firstOnly: Boolean,
      contributingMatchesSoFar: List[EntityTemporalInformation],
      tsProf: Option[TemporalSurfaceProfilingData]): (MatchSourceQuery, List[EntityTemporalInformation]) = {
    if (children.isEmpty)
      (if (contributingMatchesSoFar isEmpty) NeverMatch else CantTell, contributingMatchesSoFar)
    else {
      tsProf.foreach(_.recordVisitedChildren())
      val child: childType = children.head
      val childProf =
        tsProf.flatMap(prof => TemporalSurfaceProfilingDataManager.maybeCreateChildProfilingEntry(child, prof))

      val result =
        child.findPossiblyMatchingLeavesImpl(operation, child.addToChain(temporalContextChain), firstOnly, childProf)

      result match {
        case (AlwaysMatch, leafSurfaces) =>
          (AlwaysMatch, contributingMatchesSoFar ::: leafSurfaces)
        case (NeverMatch, _) =>
          findPossiblyMatchingLeavesInChildren(
            children.tail,
            operation,
            temporalContextChain,
            firstOnly,
            contributingMatchesSoFar,
            tsProf)
        case (CantTell, leafSurfaces) =>
          if (firstOnly) (CantTell, contributingMatchesSoFar ::: leafSurfaces)
          else {
            findPossiblyMatchingLeavesInChildren(
              children.tail,
              operation,
              temporalContextChain,
              firstOnly,
              contributingMatchesSoFar ::: leafSurfaces,
              tsProf)
          }
      }
    }
  }

  // this would be more accurate it we included the remaining data space (at least classes) not yet completely matched
  /**
   * implements [findPossiblyMatchingLeaves]
   */
  // queries which surfaces match in tree of surfaces
  @node private[temporalSurface] override def findPossiblyMatchingLeavesImpl(
      operation: TemporalSurfaceQuery,
      temporalContextChain: List[TemporalContext],
      firstOnly: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): (MatchSourceQuery, List[EntityTemporalInformation]) = {
    val (time, result) = AdvancedUtils.timed {
      tsProf.foreach(_.recordVisit())

      val (matchTime, scopeRes) = AdvancedUtils.timed(scope.matchScope(operation, this))
      tsProf.foreach(_.recMatchWalltime(matchTime))

      val res = scopeRes match {
        case AlwaysMatch =>
          tsProf.foreach(_.recordMatcherHit())
          findPossiblyMatchingLeavesInChildren(children, operation, temporalContextChain, firstOnly, Nil, tsProf)
        case NeverMatch => (NeverMatch, Nil)
        case ErrorMatch => throw new TemporalSurfaceTemporalityException("")
        case CantTell =>
          operation match {
            case _: EntityClassBasedQuery[_] =>
              (NeverMatch, Nil) // TODO (OPTIMUS-48032): investigate why we don't search children
            case er: DataQueryByEntityReference[_] =>
              val (time, itemMatchRes) = AdvancedUtils.timed(scope.matchItemScope(er, this)(er.eRef))
              tsProf.foreach(_.recMatchWalltime(time))
              itemMatchRes match {
                case AlwaysMatch =>
                  tsProf.foreach(_.recordMatcherHit())
                  findPossiblyMatchingLeavesInChildren(
                    children,
                    operation,
                    temporalContextChain,
                    firstOnly,
                    Nil,
                    tsProf)
                case NeverMatch => (NeverMatch, Nil)
                case ErrorMatch => throw new TemporalSurfaceTemporalityException("")
              }
            case _: TemporalSurfaceQuery =>
              (NeverMatch, Nil)
          }
      }
      log.debug(
        s"resolved source temporality from $this for $operation is ${res._1} possible matches leafs are: ${res._2}")
      res
    }
    tsProf.foreach(_.recWallTime(time))
    result
  }
}

/**
 * allows us to configure a plugin and SS
 */
@entity private[impl] object pluginHack {
  @node def start(
      tc: TemporalContextImpl,
      operation: TemporalSurfaceQuery,
      tsProf: Option[TemporalSurfaceProfilingData]): AnyRef =
    tc.dataAccessImpl(operation, tsProf)

  if (Settings.traceDalAccessOrTemporalSurfaceCommands) {
    start_info.setPlugin(TemporalSurfaceDataAccessTracePlugin)
  }

  start_info.setCacheable(false)
}
