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

import optimus.platform.dal.Discrete

/**
 * An interval [from, to)
 *
 * Do not extend except from optimus.platform.[Valid]TimeInterval.
 *
 * @param from
 *   the start of the interval, inclusive
 * @param to
 *   the end of the interval, exclusive
 */
private[optimus] abstract class Interval[T <: Comparable[T]] {
  val from: T
  val to: T

  protected def discrete: Discrete[T]
  protected def infinity: T
  protected def negInfinity: T

  def isAfter(a: T, b: T): Boolean = (a compareTo b) > 0
  def isAfterOrEqual(a: T, b: T): Boolean = (a compareTo b) >= 0
  def isBefore(a: T, b: T): Boolean = (a compareTo b) < 0
  def isBeforeOrEqual(a: T, b: T): Boolean = (a compareTo b) <= 0

  /**
   * @return
   *   true if the interval has a finite end
   */
  final def isFinite: Boolean = to != infinity

  /**
   * @return
   *   true if the interval starts after negative infinity
   */
  final def isBoundedFrom: Boolean = (from != negInfinity)

  /**
   * @return
   *   true if the interval ends before infinity
   */
  final def isBoundedTo: Boolean = isFinite

  /**
   * @return
   *   true if the interval starts after negative infinity or ends before infinity
   */
  final def isBounded: Boolean = (isBoundedFrom || isBoundedTo)

  /**
   * @return
   *   true if to <= o.from
   */
  final def precedes(o: Interval[T]): Boolean = isBeforeOrEqual(to, o.from) // ordering.lteq(to, o.from)

  /**
   * @return
   *   true if to <= t
   */
  final def precedes(t: T): Boolean = isBeforeOrEqual(to, t) // ordering.lteq(to, t)

  /**
   * @return
   *   true if from >= o.to
   */
  final def succeeds(o: Interval[T]): Boolean = isAfterOrEqual(from, o.to) // ordering.gteq(from, o.to)

  /**
   * @return
   *   true if any part of o overlaps
   */
  def overlaps(o: Interval[T]): Boolean =
    contains(o.from) ||
      (isBefore(from, o.to) && isAfterOrEqual(to, o.to)) ||
      (isAfter(from, o.from) && isBeforeOrEqual(to, o.to))

  /**
   * @return
   *   true if any part of o cross the boundary
   */
  def overlapsExcludingSharedBoundary(o: Interval[T]): Boolean =
    ((to == infinity && o.from == infinity) || (isBefore(from, o.from) && isAfter(to, o.from))) ||
      (isBefore(from, o.to) && isAfter(to, o.to)) ||
      (isAfter(from, o.from) && isBefore(to, o.to))

  /**
   * return true if from <= o.from && to >= o.to
   */
  def contains(o: Interval[T]): Boolean = isBeforeOrEqual(from, o.from) && isAfterOrEqual(to, o.to)

  /**
   * @return
   *   true if from <= t < to
   */
  final def contains(t: T): Boolean = {
    (to == infinity && t == infinity) || (isBeforeOrEqual(from, t) && isAfter(to, t))
  }

  /**
   * Note this is NOT the Allen operator meets Its instead (X m Y) || (X mi Y)
   * @return
   *   true if the intervals are adjacent
   */
  def meets(o: Interval[T]): Boolean = from == o.to || to == o.from

  /**
   * Allen meets (m) operator
   * @return
   *   true if the interval is adjacent to and precedes o
   */
  def meetsBefore(o: Interval[T]): Boolean = to == o.from

  /**
   * Allen inverse meets (mi) operator
   * @return
   *   true if the interval is adjacent to and succeeds o
   */
  def meetsAfter(o: Interval[T]): Boolean = from == o.to

  final override def hashCode: Int = from.hashCode * 31 + to.hashCode

  final override def toString = {
    val toStr = if (isFinite) to else "infinity"
    val fromStr = if (from != negInfinity) "[" + from else "(-infinity"
    fromStr + ", " + toStr + ")"
  }

  /**
   * Check whether the interval only contains a single discrete point.
   *
   * This cannot be the case if the interval starts at [[infinity]].
   *
   * @return
   *   `true` iff the interval contains exactly one discrete point
   */
  final def isPoint: Boolean =
    from != infinity && to == discrete.successor(from)

  /**
   * Check whether the interval contains any discrete points.
   *
   * Strictly, this compares the start and end of the interval using `[[Comparable]]#compareTo` and ensures that
   * [[from]] is smaller than [[to]].
   *
   * @return
   *   `true` iff the interval contains no discrete points.
   */
  final def isEmpty: Boolean =
    from.compareTo(to) >= 0
}
