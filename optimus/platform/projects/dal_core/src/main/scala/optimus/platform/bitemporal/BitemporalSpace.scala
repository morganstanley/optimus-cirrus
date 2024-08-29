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
import optimus.platform.ValidTimeInterval

/**
 * A base trait for an immutable container to hold multiple states of a bitemporal object, along with methods to update
 * the state of the object and insert new versions.
 */
trait BitemporalSpace[T] {
  type Update = Seq[UpdateAction[T]]

  /**
   * @return
   *   the transaction time of the last operation, or None if no operations have happened
   */
  def latestTxTime: Option[Instant]

  /**
   * @return
   *   all of the versions of the object
   */
  def all: Seq[Rectangle[T]]

  /**
   * @return
   *   all of the versions of the object valid as-on the given transaction time. The versions are returned in ascending
   *   valid time range order.
   */
  def onTxTime(t: Instant): Seq[Rectangle[T]]

  /**
   * @return
   *   all of the versions of the object whose ttFrom are before the given transaction time
   */
  def beforeTxTime(tt: Instant): Seq[Rectangle[T]]

  /**
   * @return
   *   all of the versions of the object valid as-of the given valid time. The versions are returned in ascending tx
   *   time range order.
   */
  def onValidTime(t: Instant): Seq[Rectangle[T]]

  /**
   * @return
   *   all of the versions of the object valid during the given valid time interval. The versions are returned in
   *   ascending tx time range order.
   */
  def onValidInterval(vt: ValidTimeInterval, tt: Instant): Seq[Rectangle[T]]

  protected def onValidIntervalWithMeets(vt: ValidTimeInterval): Seq[Rectangle[T]]

  /**
   * @return
   *   rectangles immediately adjacent to supplied VT (but not ones containing, must be boundary condition)
   */
  def adjacentToValidTime(vt: Instant): Seq[Rectangle[T]]

  /**
   * @return
   *   the version valid at the given dates, or None if none is found.
   */
  def at(asof: Instant, ason: Instant): Option[Rectangle[T]]

  /**
   * Update an interval. Will grow existing overlapping regions if they contain the same data.
   */
  def updateInterval(item: Option[T], vtInterval: ValidTimeInterval): Update

  final def insert(item: ValidSegment[T]): Update =
    updateInterval(Some(item.data), item.vtInterval)

  def insertPoint(
      item: Option[T],
      vt: Instant,
      filter: Rectangle[T] => Boolean = { _ =>
        false
      }): Update

  /**
   * "Correct" a previously inserted point. Moves the point and optionally updates the data contained in the rectangle.
   */
  def correctPoint(item: Option[T], oldVtf: Instant, newVtf: Instant): Update

  /**
   * Invalidate a previously inserted point. Segment starting at supplied vt continues the value (if any) of the
   * preceding segment.
   *
   * E.g.
   *
   * [0,4): A, [4,6): B invalidatePoint(4) => [0,6): A
   *
   * invItem is used to pass in the value for a tombstoned timeslice in case there is no preceding timeslice
   */
  def invalidatePoint(vt: Instant, invItem: Option[T]): Update

  def revertPoint(vt: Instant): Update

  /**
   * Temporary: Will be replaced with TimeSeries-based API
   */
  def filterCurrentTS(pred: T => Boolean): Update

  def map[B](f: T => B): BitemporalSpace[B]

  final def transform(tt: Instant, allowedUpdateAtLatestTxTime: Boolean)(
      f: BitemporalSpace[T] => Update): UpdateResult[T] = {
    performRawActions(f(this), tt, allowedUpdateAtLatestTxTime)
  }

  def updateMultiple(tt: Instant, allowedUpdateAtLatestTxTime: Boolean, mergeMultiVts: Boolean)(
      ops: Seq[BitemporalSpace[T] => Update]): UpdateResult[T]
  def updateIntervals(tt: Instant, allowedUpdateAtLatestTxTime: Boolean, mergeMultiVts: Boolean)(
      ops: Seq[(Option[T], ValidTimeInterval)])(validate: (Option[T], Seq[UpdateAction[T]]) => Unit): UpdateResult[T]

  def performRawActions(acts: Seq[UpdateAction[T]], tt: Instant, allowedUpdateAtLatestTxTime: Boolean): UpdateResult[T]

  /**
   * @return
   *   (the update bitemporal space, the affected rectangles for each action)
   */
  def execute(actions: Update, tt: Instant): (BitemporalSpace[T], Seq[Rectangle[T]])

  def maxTimeSliceCount: Int

}

trait UpdateAction[T]
final case class Insert[T](newData: ValidSegment[T]) extends UpdateAction[T]
final case class Invalidate[T](existing: Rectangle[T]) extends UpdateAction[T]
final case class AdjustVTInterval[T](existing: Rectangle[T], vtInterval: ValidTimeInterval) extends UpdateAction[T]

/**
 * The result of updating a bitemporal space.
 *
 * @param space
 *   the new bitemporal space created by the update
 * @param actions
 *   pairs of the actions taken to the bitemporal space to move into the updated state, along with the updated rectangle
 *   affected by the action
 */
class UpdateResult[T](val space: BitemporalSpace[T], val actions: Seq[UpdateAction[T]], val rects: Seq[Rectangle[T]]) {}

class BitemporalOutdatedTTException(message: String) extends BitemporalException(message)
class BitemporalException(message: String) extends Exception(message)
class BitemporalSegmentException(val conflicting: Rectangle[_], message: String) extends BitemporalException(message)
class BitemporalCoalesceRequirementFailureException(message: String) extends BitemporalException(message)
