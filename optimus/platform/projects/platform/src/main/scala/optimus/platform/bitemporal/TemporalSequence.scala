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

import optimus.platform.TimeInterval
import java.time.Instant

/**
 * A sequence of items, each of which is valid for some valid time interval.
 */
class TemporalSequence[T] private (val items: List[Segment[T]]) {
  def this() = this(Nil)

  lazy val current: Option[Segment[T]] = items.headOption flatMap {
    _.interval.isFinite match {
      case true  => None
      case false => Some(items.head)
    }
  }

  def ason(on: Instant): Option[Segment[T]] = items.find(_.interval contains on)

  def invalidate(on: Instant): TemporalSequence[T] = {
    require(current.isDefined)
    val c = current.get
    require(c.interval.from.compareTo(on) < 0)

    val newItems = new Segment[T](c.data, TimeInterval(c.interval.from, on)) :: items.tail
    new TemporalSequence[T](newItems)
  }

  def insert(item: T, on: Instant): TemporalSequence[T] = {
    require(!current.isDefined)
    items.headOption foreach { latest =>
      require(latest.interval precedes on)
    }

    val newItems = new Segment[T](item, TimeInterval(on)) :: items
    new TemporalSequence[T](newItems)
  }

  def isEmpty = items.isEmpty

  override def equals(o: Any) = o match {
    case ts: TemporalSequence[_] => items == ts.items
    case _                       => false
  }

  override def hashCode: Int = items.hashCode

  override def toString: String = "[" + (items.reverse map (_.toString) mkString (",")) + "]"
}

object TemporalSequence {
  def apply[T](items: Seq[Segment[T]]): TemporalSequence[T] = {
    val ordered = items.sortWith((l, r) => l.interval succeeds r.interval).toList
    ordered.sliding(2) foreach { w =>
      w.tail.headOption foreach { t =>
        require(!(w.head.interval overlaps t.interval))
      }
    }
    new TemporalSequence[T](ordered.toList)
  }
}
