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
package optimus.platform.bitemporal

import java.time.Instant

import optimus.core.Collections
import optimus.platform.internal.SimpleGlobalStateHolder
import optimus.platform.TimeInterval
import optimus.platform.ValidTimeInterval
import optimus.platform.bitemporal.DateConversions._

import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.collection.compat._

private[platform] class BitemporalSpaceSetting {
  val KEY_ENABLE_FINAL_ACTIONS_MERGE = "optims.platform.bitemporal.mergeFinalActions"
  @volatile var mergeFinalActionsEnabled = System.getProperty(KEY_ENABLE_FINAL_ACTIONS_MERGE, "true").toBoolean
  def isMergeFinalActionsEnabled = mergeFinalActionsEnabled
  def setMergeFinalActionsEnabled(b: Boolean): Unit = mergeFinalActionsEnabled = b
}

private[platform] object BitemporalSpaceSetting
    extends SimpleGlobalStateHolder[BitemporalSpaceSetting]({ () =>
      new BitemporalSpaceSetting
    }) {
  def isMergeFinalActionsEnabled = getState.isMergeFinalActionsEnabled
  def setMergeFinalActionsEnabled(b: Boolean): Unit = getState.setMergeFinalActionsEnabled(b)
}

abstract class AbstractBitemporalSpace[T](val latestTxTime: Option[Instant] = None) extends BitemporalSpace[T] {
  final override def updateInterval(item: Option[T], vtInterval: ValidTimeInterval): Update = {
    val actions = Seq.newBuilder[UpdateAction[T]]

    // Do we still need to insert this interval? We might just grow an existing one with the same data.
    var needsInsert = true

    val affectedRectangles = onValidIntervalWithMeets(vtInterval)

    def updateRectangle(rect: Rectangle[T]): Unit = {
      val startsBefore = rect.vtInterval.from isBefore vtInterval.from
      val endsAfter = rect.vtInterval.to isAfter vtInterval.to
      val sameData = (item == Some(rect.data))

      def extendIfNeeded(): Unit = {
        if (!(rect.vtInterval contains vtInterval))
          actions += AdjustVTInterval(rect, rect.vtInterval union vtInterval)
        needsInsert = false
      }

      if (sameData && needsInsert)
        extendIfNeeded()
      else if (startsBefore && endsAfter) {
        // Split the original rectangle into two parts
        actions += AdjustVTInterval(rect, rect.vtInterval.copy(to = vtInterval.from))
        actions += Insert(new ValidSegment(rect.data, rect.vtInterval.copy(from = vtInterval.to)))
      } else if (startsBefore) {
        if (rect.vtInterval.to isAfter vtInterval.from)
          actions += AdjustVTInterval(rect, ValidTimeInterval(rect.vtInterval.from, vtInterval.from))
      } else if (endsAfter) {
        if (rect.vtInterval.from isBefore vtInterval.to)
          actions += AdjustVTInterval(rect, ValidTimeInterval(vtInterval.to, rect.vtInterval.to))
      } else {
        actions += Invalidate(rect)
      }
    }

    affectedRectangles foreach updateRectangle
    if (needsInsert && item.isDefined)
      actions += Insert(new ValidSegment(item.get, vtInterval))

    actions.result()
  }

  final override def insertPoint(
      item: Option[T],
      vtf: Instant,
      filter: Rectangle[T] => Boolean = { _ =>
        false
      }) = {
    // Requirement: This returns ordered segments
    val rects = onTxTime(TimeInterval.Infinity)
    // Find a segment that defines the VTT of the inserted data.
    // If we're inserting INTO a segment, 'item' is valid until the END of that segment
    // O/w 'item' is valid until the START of the next segment
    // o/w if no such segment, valid until infinity.
    val rectOption = rects.find { vtf isBefore _.vtInterval.to }

    def insert(vtt: Instant, curRect: Option[Rectangle[T]] = None) = item.toList flatMap { value =>
      val nextRect = rects.find { r =>
        r.vtInterval.from == vtt && filter(r)
      }
      if (curRect.isDefined && filter(curRect.get) && nextRect.isDefined) {
        Seq(Invalidate(nextRect.get), Insert(ValidSegment(value, ValidTimeInterval(vtf, nextRect.get.vtInterval.to))))
      } else {
        Seq(Insert(ValidSegment(value, ValidTimeInterval(vtf, vtt))))
      }
    }

    rectOption match {
      case Some(rect) if (rect.vtInterval.from isAfter vtf) =>
        // There's nothing in the region we're inserting the point into.
        // So the inserted segment is valid UNTIL the found segment
        insert(rect.vtInterval.from)
      case Some(rect) =>
        // We're inserting INTO the found segment
        val adjust =
          if (rect.vtInterval.from == vtf)
            Invalidate(rect)
          else
            AdjustVTInterval(rect, rect.vtInterval.copy(to = vtf))

        adjust :: insert(rect.vtInterval.to, Some(rect))
      case None => insert(TimeInterval.Infinity)

    }
  }

  final override def correctPoint(item: Option[T], ovtf: Instant, nvtf: Instant) = {
    val segs = onTxTime(TimeInterval.Infinity)
    val pair = segs filter { s =>
      s.vtInterval.to == ovtf || s.vtInterval.from == ovtf
    }

    def validateTargetSegment(vti: ValidTimeInterval, target: Rectangle[T]) = {
      segs.foreach { s =>
        if ((s != target) && (s.vtInterval overlaps vti))
          throw new BitemporalSegmentException(s, "Illegal correction into non-adjacent timeslice")
      }
    }

    def updateAfter(after: Rectangle[T], validate: Boolean) = {
      if (after.vtInterval.from != ovtf)
        throw new BitemporalSegmentException(
          after,
          "Valid segment containing supplied VTF " + ovtf + " begins at " + after.vtInterval.from)
      if (nvtf >= after.vtInterval.to)
        throw new BitemporalSegmentException(
          after,
          "Valid segment beginning with supplied VTF ends before proposed new VTF")
      else {
        val changed = item.forall(_ != after.data)
        val newVti = ValidTimeInterval(nvtf, after.vtInterval.to)
        if (validate) validateTargetSegment(newVti, after)
        if (changed) {
          Invalidate(after) :: (item.map { i =>
            Insert(new ValidSegment(i, newVti))
          }.toList)
        } else {
          AdjustVTInterval(after, newVti) :: Nil
        }
      }
    }

    def updateBefore(before: Rectangle[T], validate: Boolean) = {
      if (!(before.vtInterval.from isBefore nvtf))
        throw new BitemporalSegmentException(before, "existing prior segment's VTF is not before proposed new VTT")
      val newVti = before.vtInterval.copy(to = nvtf)
      if (validate) validateTargetSegment(newVti, before)
      AdjustVTInterval(before, newVti)
    }

    def insert(value: T) = {
      val vtt = segs.find { nvtf isBefore _.vtInterval.from } map { _.vtInterval.from } getOrElse {
        TimeInterval.Infinity
      }
      Insert(ValidSegment(value, ValidTimeInterval(nvtf, vtt)))
    }

    pair match {
      case Seq(before, after) =>
        updateBefore(before, false) :: updateAfter(after, false)
      case Seq(after) if after.vtInterval.from == ovtf =>
        updateAfter(after, true)
      case Seq(before) if before.vtInterval.to == ovtf =>
        updateBefore(before, true) :: item.toList.map(insert)
      case _ =>
        throw new BitemporalException("Cannot find valid segment adjacent to supplied VTF " + ovtf)
    }
  }

  final override def adjacentToValidTime(vt: Instant) = {
    val segs = onTxTime(TimeInterval.Infinity)
    segs filter { s =>
      s.vtInterval.to == vt || s.vtInterval.from == vt
    }
  }

  final override def invalidatePoint(vt: Instant, invItem: Option[T]) = {
    val pair = adjacentToValidTime(vt)

    pair match {
      case Seq(before, after) =>
        Invalidate(after) :: AdjustVTInterval(before, before.vtInterval.copy(to = after.vtInterval.to)) :: Nil
      case Seq(after) if after.vtInterval.from == vt =>
        Invalidate(after) :: (invItem.toList map { i =>
          val vtt = onTxTime(TimeInterval.Infinity)
            .find(_.vtInterval.from isAfter vt)
            .map(_.vtInterval.from)
            .getOrElse(TimeInterval.Infinity)
          Insert(ValidSegment(i, ValidTimeInterval(vt, vtt)))
        })
      case _ =>
        throw new BitemporalException("Cannot find valid segment adjacent to supplied VTF " + vt)
    }
  }

  final override def revertPoint(vt: Instant) = {
    val pair = adjacentToValidTime(vt)
    pair match {
      case Seq(before, after) =>
        Invalidate(after) :: AdjustVTInterval(before, before.vtInterval.copy(to = after.vtInterval.to)) :: Nil
      case _ =>
        throw new BitemporalException(
          s"Found unexpected valid segment of size: ${pair.size} adjacent to supplied VTF $vt")
    }
  }

  def filterCurrentTS(pred: T => Boolean) = {
    // XXX use unsorted timeseries
    val ts = onTxTime(TimeInterval.Infinity)

    // XXX 'actions' can ever be empty? if so, the assert in the ctor of UpdateResult will fail
    ts filter { r =>
      !pred(r.data)
    } map Invalidate.apply
  }

  private def checkTT(tt: Instant, allowedUpdateAtLatestTxTime: Boolean): Unit = {
    latestTxTime foreach { last =>
      val condition = if (allowedUpdateAtLatestTxTime) tt < last else tt <= last
      if (condition)
        throw new BitemporalOutdatedTTException(
          s"Given transaction time $tt must occur after last known transaction time $last")
    }
  }

  // exposing for tests
  private[optimus] def executeWithResult(
      actions: Update,
      tt: Instant,
      allowedUpdateAtLatestTxTime: Boolean): UpdateResult[T] = {
    checkTT(tt, allowedUpdateAtLatestTxTime)

    val (space, affected) = execute(actions, tt)
    new UpdateResult[T](space, actions, affected)
  }

  override def performRawActions(actions: Update, tt: Instant, allowedUpdateAtLatestTxTime: Boolean) =
    executeWithResult(actions, tt, allowedUpdateAtLatestTxTime)

  final override def updateMultiple(tt: Instant, allowedUpdateAtLatestTxTime: Boolean, mergeMultiVts: Boolean)(
      ops: Seq[BitemporalSpace[T] => Update]): UpdateResult[T] = {
    val newOps = ops map { op =>
      new UpdateDescriptor {
        def updateSpace(space: BitemporalSpace[T]): Update = op(space)
        def validateActions(actions: Update) = {}
      }
    }

    val (_, finalActions) = unsafeMultipleUpdate(tt, allowedUpdateAtLatestTxTime, mergeMultiVts)(newOps)
    executeWithResult(finalActions, tt, allowedUpdateAtLatestTxTime)
  }

  final override def updateIntervals(tt: Instant, allowedUpdateAtLatestTxTime: Boolean, mergeMultiVts: Boolean)(
      ops: Seq[(Option[T], ValidTimeInterval)])(
      validate: (Option[T], Seq[UpdateAction[T]]) => Unit): UpdateResult[T] = {
    val (_, finalActions) = unsafeUpdateIntervals(tt, allowedUpdateAtLatestTxTime, mergeMultiVts)(ops)(validate)
    executeWithResult(finalActions, tt, allowedUpdateAtLatestTxTime)
  }

  // We have a separate method that returns the degenerate space for tests.
  final private[optimus] def unsafeUpdateIntervals(
      tt: Instant,
      allowedUpdateAtLatestTxTime: Boolean,
      mergeMultiVts: Boolean)(ops: Seq[(Option[T], ValidTimeInterval)])(
      validate: (Option[T], Seq[UpdateAction[T]]) => Unit) = {
    val newOps = ops map { case (value, vtInterval) =>
      new UpdateDescriptor {
        def updateSpace(space: BitemporalSpace[T]): Update = {
          space.updateInterval(value, vtInterval)
        }
        def validateActions(actions: Update) = {
          validate(value, actions)
        }
      }
    }

    unsafeMultipleUpdate(tt, allowedUpdateAtLatestTxTime, mergeMultiVts)(newOps)
  }

  abstract class UpdateDescriptor {
    def updateSpace(space: BitemporalSpace[T]): Update
    def validateActions(actions: Seq[UpdateAction[T]]): Unit
  }

  final private[optimus] def unsafeMultipleUpdate(
      tt: Instant,
      allowedUpdateAtLatestTxTime: Boolean,
      mergeMultiVts: Boolean)(ops: Seq[UpdateDescriptor]) = {
    checkTT(tt, allowedUpdateAtLatestTxTime)

    def isFresh(rect: Rectangle[T]) = rect.index > maxTimeSliceCount

    /*
     * Map from rectangle index to intermediate state.
     *
     * Note that we need a valid sequence of UpdateActions, not just the final space, in order
     * to compute time slice actions.
     *
     * Invariants: any key in this map must correspond to either
     *   a) a rectangle that existed in the initial bitemporal space
     *   b) a newly-created rectangle that is open until tt infinity
     *
     * this means that after processing each individual operation, the actions contained in the map's
     * values will produce a space where the current valid timeline (i.e. onTxTime(TimeInterval.Infinity))
     * is the same as the current intermediate bitemporal space.
     *
     * The final state produced through iteration is not usable as it contains degenerate rectangles (with empty tt interval).
     * So we use the produced actions here to generate a cleaned-up bitemporal space.
     *
     * In the end state (once unique indexes are represented as timeslices, we won't need to produce a final state at all and
     * can simply throw the result space away and return the update actions.
     */
    val state = mutable.Map.empty[Int, UpdateAction[T]]

    def updateAction(act: UpdateAction[T], vti: ValidTimeInterval) = {
      act match {
        case Insert(seg) =>
          Insert(seg.copy(vtInterval = vti))
        case AdjustVTInterval(existing, _) =>
          AdjustVTInterval(existing, vti)
        case Invalidate(_) =>
          throw new IllegalStateException("Cannot update invalidated rectangle")
      }
    }

    def coalesce(updates: Seq[UpdateAction[T]], affected: Seq[Rectangle[T]]): Unit = {
      import AbstractBitemporalSpace.coalesceFail

      Collections.foreach2(updates, affected) { (act, rect) =>
        act match {
          case AdjustVTInterval(existing, vti) =>
            state.get(existing.index) match {
              case None =>
                if (isFresh(existing)) throw coalesceFail
                state(rect.index) = act
              case Some(oldAction) =>
                if (!isFresh(existing)) throw coalesceFail
                state -= existing.index
                state(rect.index) = updateAction(oldAction, vti)
            }
          case Invalidate(existing) =>
            state.get(existing.index) match {
              case Some(oldAction) =>
                if (!isFresh(existing)) throw coalesceFail
                state -= existing.index
                oldAction match {
                  case AdjustVTInterval(oldRect, _) =>
                    if (!isFresh(oldRect))
                      state(oldRect.index) = Invalidate(oldRect)
                  case _ => // do nothing
                }
              case None =>
                if (isFresh(existing)) throw coalesceFail
                state(rect.index) = act
            }
          case Insert(_) =>
            state(rect.index) = act
        }
      }
    }

    val finalSpace = ops.foldLeft(this: BitemporalSpace[T]) { (space, op) =>
      val actions = op.updateSpace(space)
      op.validateActions(actions)
      val (newSpace, rects) = space.execute(actions, tt)
      coalesce(actions, rects)
      newSpace
    }

    def mergeActions(a: UpdateAction[T], b: UpdateAction[T], tail: List[UpdateAction[T]]): List[UpdateAction[T]] = {
      (a, b) match {
        case (AdjustVTInterval(existingA, ia), AdjustVTInterval(existingB, ib))
            if existingA.data == existingB.data && ia.to == ib.from =>
          AdjustVTInterval(existingA, ValidTimeInterval(ia.from, ib.to)) :: Invalidate(existingB) :: tail
        case (AdjustVTInterval(existing, ia), Insert(ValidSegment(data, ib)))
            if existing.data == data && ia.to == ib.from =>
          AdjustVTInterval(existing, ValidTimeInterval(ia.from, ib.to)) :: tail
        case (Insert(ValidSegment(data, ib)), AdjustVTInterval(existing, ia))
            if existing.data == data && ia.to == ib.from =>
          AdjustVTInterval(existing, ValidTimeInterval(ia.from, ib.to)) :: tail
        case (Insert(ValidSegment(da, ia)), Insert(ValidSegment(db, ib))) if da == db && ia.to == ib.from =>
          Insert(ValidSegment(da, ValidTimeInterval(ia.from, ib.to))) :: tail
        // TODO (OPTIMUS-41200): remove the following case after we fixed all overlaps in unique index
        // the following case could happen if there is already overlap in this space, avoid generating more overlap
        case (AdjustVTInterval(existingA, ia), AdjustVTInterval(existingB, ib))
            if existingA.data == existingB.data && ia.overlaps(ib) =>
          AdjustVTInterval(existingA, ia.union(ib)) :: Invalidate(existingB) :: tail
        case (AdjustVTInterval(existing, ia), Insert(ValidSegment(data, ib)))
            if existing.data == data && ia.overlaps(ib) =>
          AdjustVTInterval(existing, ia.union(ib)) :: tail
        case (Insert(ValidSegment(data, ib)), AdjustVTInterval(existing, ia))
            if existing.data == data && ia.overlaps(ib) =>
          AdjustVTInterval(existing, ia.union(ib)) :: tail
        case (Insert(ValidSegment(dataA, vtIntervalA)), Insert(ValidSegment(dataB, vtIntervalB)))
            if dataA == dataB && vtIntervalA.overlaps(vtIntervalB) =>
          Insert(ValidSegment(dataA, vtIntervalA.union(vtIntervalB))) :: tail
        // END of duplicate check
        case _ => a :: b :: tail
      }
    }

    def cancelActions(a: UpdateAction[T], b: UpdateAction[T], tail: List[UpdateAction[T]]): List[UpdateAction[T]] = {
      (a, b) match {
        case (Invalidate(existing), Insert(ValidSegment(data, vtInterval)))
            if existing.data == data && existing.vtInterval == vtInterval =>
          tail
        case (Insert(ValidSegment(data, vtInterval)), Invalidate(existing))
            if existing.data == data && existing.vtInterval == vtInterval =>
          tail
        case _ => a :: b :: tail
      }
    }

    // we will merge Invalidate & Insert,  AdjustVT & Insert that can form a intact rectangle (with same data)
    def merge(actions: Seq[UpdateAction[T]]): Seq[UpdateAction[T]] = {
      if (!mergeMultiVts || actions.isEmpty || !BitemporalSpaceSetting.isMergeFinalActionsEnabled) {
        actions.filter {
          case AdjustVTInterval(rect, vti) if rect.vtInterval == vti => false
          case _                                                     => true
        }
      } else {
        val (adjustOrInserts, invalidates) = actions.partition {
          case AdjustVTInterval(existing, vtInterval) => true
          case Invalidate(existing)                   => false
          case Insert(ValidSegment(data, vtInterval)) => true
        }

        val mergedAdjustOrInserts = adjustOrInserts
          .sortBy {
            case AdjustVTInterval(existing, vtInterval) => vtInterval.from
            case Invalidate(existing)                   => throw new IllegalStateException
            case Insert(ValidSegment(data, vtInterval)) => vtInterval.from
          }(Ordering[Instant].reverse)
          .foldLeft(List.empty[UpdateAction[T]]) {
            case (Nil, action) => action :: Nil
            case (head :: tail, action) =>
              mergeActions(action, head, tail)
          }
          .filter {
            case AdjustVTInterval(rect, vti) if rect.vtInterval == vti => false
            case _                                                     => true
          }

        if (invalidates.nonEmpty) {
          (invalidates ++ mergedAdjustOrInserts)
            .sortBy {
              case AdjustVTInterval(existing, vtInterval) => vtInterval.from
              case Invalidate(existing)                   => existing.vtInterval.from
              case Insert(ValidSegment(data, vtInterval)) => vtInterval.from
            }(Ordering[Instant].reverse)
            .foldLeft(List.empty[UpdateAction[T]]) {
              case (Nil, action) => action :: Nil
              case (head :: tail, action) =>
                cancelActions(action, head, tail)
            }
        } else mergedAdjustOrInserts
      }
    }

    (finalSpace, merge(state.values.toVector))
  }
}

object AbstractBitemporalSpace {
  private val coalesceFail = new BitemporalCoalesceRequirementFailureException(
    "Requirement failed in bitemporal space coalesce")
}

/**
 * When adjusting the valid time interval of a bitemporal rectangle, possibly transform the data from the existing
 * rectangle to put in the new rectangle.
 */
final case class SimpleBitemporalSpace[T] private (
    rects: Seq[Rectangle[T]],
    override val latestTxTime: Option[Instant],
    timeSliceCount: Int)
    extends AbstractBitemporalSpace[T] {
  def this() = this(Nil, None, 0)

  def maxTimeSliceCount = timeSliceCount

  if (!(latestTxTime.isDefined || rects.isEmpty))
    throw new IllegalArgumentException("SimpleBitemporalSpace check failed")

  def all: Seq[Rectangle[T]] = rects

  def onTxTime(tt: Instant): Seq[Rectangle[T]] = {
    rects filter { _.ttInterval.contains(tt) } sortWith { (l, r) =>
      l.vtInterval.from isBefore r.vtInterval.from
    }
  }

  def beforeTxTime(tt: Instant): Seq[Rectangle[T]] = {
    rects filter { _.ttInterval.from.isBefore(tt) }
  }

  def onValidTime(vt: Instant): Seq[Rectangle[T]] = {
    rects filter { _.vtInterval.contains(vt) } sortWith { (l, r) =>
      l.ttInterval.from isBefore r.ttInterval.from
    }
  }

  protected def onValidIntervalWithMeets(vt: ValidTimeInterval) = {
    rects filter { r =>
      (r.ttInterval.to == TimeInterval.Infinity) && (r.vtInterval.overlaps(vt) || r.vtInterval.meets(vt))
    }
  }

  // Returns unsorted rectangles.
  def onValidInterval(vt: ValidTimeInterval, tt: Instant) = {
    rects filter { r =>
      r.ttInterval.contains(tt) && r.vtInterval.overlaps(vt)
    }
  }

  def at(vt: Instant, tt: Instant): Option[Rectangle[T]] = {
    rects find { r =>
      r.vtInterval.contains(vt) && r.ttInterval.contains(tt)
    }
  }

  final def execute(actions: Update, tt: Instant): (BitemporalSpace[T], Seq[Rectangle[T]]) = {
    val newRects: mutable.Buffer[Rectangle[T]] = rects.toBuffer
    var currentMaxTimeSliceCount = maxTimeSliceCount

    def doInsert(newData: ValidSegment[T]): Rectangle[T] = {
      val ttInterval = TimeInterval(tt, TimeInterval.Infinity)
      currentMaxTimeSliceCount = currentMaxTimeSliceCount + 1
      val inserted = new Rectangle[T](newData.data, ttInterval, newData.vtInterval, currentMaxTimeSliceCount)
      inserted +=: newRects
      inserted
    }

    def invalidateRect(existing: Rectangle[T]) = {
      val invalidated = existing.copy(ttInterval = existing.ttInterval.copy(to = tt))
      newRects.update(newRects.indexOf(existing), invalidated)
      invalidated
    }

    def doInvalidate(existing: Rectangle[T]): Rectangle[T] = {
      invalidateRect(existing)
    }

    def doAdjustVTInterval(existing: Rectangle[T], vtInterval: ValidTimeInterval): Rectangle[T] = {
      val ttInterval = TimeInterval(tt, TimeInterval.Infinity)
      val newData = existing.data
      currentMaxTimeSliceCount = currentMaxTimeSliceCount + 1
      val adjusted = new Rectangle(newData, ttInterval, vtInterval, currentMaxTimeSliceCount)
      invalidateRect(existing)
      adjusted +=: newRects
      adjusted
    }

    def executeAction(action: UpdateAction[T]): Rectangle[T] = {
      action match {
        case Insert(newData)                        => doInsert(newData)
        case Invalidate(existing)                   => doInvalidate(existing)
        case AdjustVTInterval(existing, vtInterval) => doAdjustVTInterval(existing, vtInterval)
      }
    }

    val result = actions map executeAction
    val space = new SimpleBitemporalSpace(newRects, Some(tt), currentMaxTimeSliceCount)

    (space, result)
  }

  override def map[B](f: T => B): BitemporalSpace[B] = {
    val newRects = rects map { _ map f }
    new SimpleBitemporalSpace(newRects, latestTxTime, maxTimeSliceCount)
  }

  override def toString(): String = {
    val sb = new StringBuilder

    val tts: SortedSet[Instant] = (rects.iterator
      .map { _.ttInterval.to }
      .filter { _ != TimeInterval.Infinity })
      .iterator
      .++(rects map { _.ttInterval.from })
      .to(SortedSet)

    tts.foreach { tt =>
      sb ++= "=== " ++= tt.toString ++= " ===\n"
      onTxTime(tt) foreach { r =>
        sb ++= " * " ++= r.toString ++= "\n"
      }
    }
    sb.toString
  }
}

object SimpleBitemporalSpace {
  def apply[T](rects: Seq[Rectangle[T]], timeSliceCount: Int): SimpleBitemporalSpace[T] = {
    // NB: This has *massive* overhead on entities with lots of versions (it's quadratic time).
    // I'm leaving it here in comments in case we need to debug this and re-enable it temporarily.
    //    for (
    //      ra <- rects;
    //      rb <- rects if (ra ne rb) && (ra overlaps rb)
    //    ) {
    //      throw new BitemporalException("Cannot create bitemporal space with overlapping rectangles")
    //    }

    val latestTxTime =
      if (rects.isEmpty)
        None
      else
        Some(rects.map(_.ttInterval.from).max)

    new SimpleBitemporalSpace[T](rects, latestTxTime, timeSliceCount)
  }
}
