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

import optimus.platform.{TimeInterval, ValidTimeInterval}

/**
 * A base trait for bitemporal objects.
 */
trait Bitemporal {

  /**
   * The transaction time interval during which the object is valid
   */
  def ttInterval: TimeInterval

  /**
   * The valid time interval during which the object is valid
   */
  def vtInterval: ValidTimeInterval

  def overlaps(o: Bitemporal): Boolean =
    vtInterval.overlaps(o.vtInterval) &&
      ttInterval.overlaps(o.ttInterval)

  override def toString = s"(vt=${vtInterval}, tt=${ttInterval})"
}

/**
 * A wrapper to add a bitemporal context around data.
 *
 * @param data
 *   the data valid during the bitemporal context
 * @param ttInterval
 *   the transaction time interval
 * @param vtInterval
 *   the valid time interval
 */
class Rectangle[T](val data: T, val ttInterval: TimeInterval, val vtInterval: ValidTimeInterval, val index: Int)
    extends Bitemporal
    with Comparable[Rectangle[T]] {
  def this(segment: ValidSegment[T], ttInterval: TimeInterval, index: Int) =
    this(segment.data, ttInterval, segment.vtInterval, index)

  if (index <= 0)
    throw new IllegalArgumentException("requirement failed: Needed index > 0")

  override def toString = s"(vt=${vtInterval}, tt=${ttInterval}, data=${data}, index=${index})"

  override def equals(other: Any) = other match {
    case other: Rectangle[_] =>
      ttInterval == other.ttInterval &&
      vtInterval == other.vtInterval &&
      data == other.data
    case _ => false
  }

  def map[B](f: T => B): Rectangle[B] = {
    new Rectangle(f(data), ttInterval, vtInterval, index)
  }

  override def hashCode = ttInterval.hashCode * 31 + vtInterval.hashCode * 31 + data.hashCode

  def copy(
      data: T = data,
      ttInterval: TimeInterval = ttInterval,
      vtInterval: ValidTimeInterval = vtInterval,
      index: Int = index) = new Rectangle(data, ttInterval, vtInterval, index)

  override def compareTo(o: Rectangle[T]): Int = ttInterval.from.compareTo(o.ttInterval.from) match {
    case 0 =>
      ttInterval.to.compareTo(o.ttInterval.to) match {
        case 0 =>
          vtInterval.from.compareTo(o.vtInterval.from) match {
            case 0 => vtInterval.to.compareTo(o.vtInterval.to)
            case x => x
          }
        case x => x
      }
    case x => x
  }
}

/**
 * A wrapper around temporal data that is valid during some interval.
 *
 * @param data
 *   the data
 * @param interval
 *   the interval during which the data is valid
 */
final case class Segment[T](val data: T, val interval: TimeInterval)

/**
 * A wrapper around temporal data that is valid during some valid time interval
 *
 * @param data
 *   the data
 * @param vtInterval
 *   the valid time interval
 */
final case class ValidSegment[+T](val data: T, val vtInterval: ValidTimeInterval)
