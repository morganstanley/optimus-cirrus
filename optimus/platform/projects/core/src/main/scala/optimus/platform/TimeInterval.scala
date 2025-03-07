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
package optimus.platform

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.dal.DalChronon
import optimus.platform.dal.Discrete

import TimeInterval.instantDiscrete

final case class TimeInterval(from: Instant, to: Instant) extends bitemporal.Interval[Instant] {
  import TimeInterval.log
  if (!isBeforeOrEqual(from, to)) {
    val msg = s"requirement failed: tt from ($from) should not be after tt to ($to)"
    val ex = new IllegalArgumentException(msg)
    log.error(msg, ex)
    throw ex
  }

  override def infinity: Instant = TimeInterval.Infinity
  override def negInfinity: Instant = TimeInterval.NegInfinity

  override protected def discrete: Discrete[Instant] = instantDiscrete

  final override def equals(o: Any): Boolean = o match {
    case that: TimeInterval =>
      from == that.from &&
      to == that.to
    case _ => false
  }
}

object TimeInterval {
  val log = getLogger[TimeInterval]

  /**
   * Creates a bounded interval of [from, to)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: ZonedDateTime, to: ZonedDateTime): TimeInterval =
    new TimeInterval(from.toInstant, to.toInstant)

  /**
   * Creates a bounded interval of [from, to)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: OffsetDateTime, to: OffsetDateTime): TimeInterval =
    new TimeInterval(from.toInstant, to.toInstant)

  /**
   * Creates a bounded interval of [from, to)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: Instant, to: Instant): TimeInterval =
    new TimeInterval(from, to)

  /**
   * Creates an unbounded interval of [from, infinity)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: ZonedDateTime): TimeInterval =
    new TimeInterval(from.toInstant, Infinity)

  /**
   * Creates an unbounded interval of [from, infinity)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: OffsetDateTime): TimeInterval =
    new TimeInterval(from.toInstant, Infinity)

  /**
   * Creates an unbounded interval of [from, infinity)
   *
   * @param from
   *   the start of the interval, inclusive
   */
  def apply(from: Instant): TimeInterval =
    new TimeInterval(from, Infinity)

  /**
   * Creates an interval representing [from, from + 1 chronon). (A chronon is the smallest discrete unit of time the
   * system is designed to deal with)
   */
  def point(from: Instant): TimeInterval = {
    apply(from, instantDiscrete.successor(from))
  }

  val Infinity: Instant = Instant.ofEpochMilli(Long.MaxValue)
  val NegInfinity: Instant = Instant.ofEpochMilli(Long.MinValue)

  val max = new TimeInterval(NegInfinity, Infinity)

  implicit val instantDiscrete: Discrete[Instant] = new Discrete[Instant] {
    override def successor(t: Instant): Instant = {
      require(t != Infinity, "Cannot get successor instant of Infinity")
      t.plusNanos(DalChronon)
    }

    override def predecessor(t: Instant): Instant = {
      require(t != NegInfinity, "Cannot get predecessor instant of NegInfinity")
      t.minusNanos(DalChronon)
    }
  }
}

final case class ValidTimeInterval(from: Instant, to: Instant) extends bitemporal.Interval[Instant] {
  require(isBeforeOrEqual(from, to), s"vt from ${from} is greater than vt to ${to}")

  private[this] def ordering = implicitly[Ordering[Instant]]

  def this(int: TimeInterval) = this(int.from, int.to)

  override def infinity: Instant = TimeInterval.Infinity
  override def negInfinity: Instant = TimeInterval.NegInfinity

  override protected def discrete: Discrete[Instant] = instantDiscrete

  def union(o: ValidTimeInterval) = {
    require((this meets o) || (this overlaps o))
    ValidTimeInterval(ordering.min(from, o.from), ordering.max(to, o.to))
  }

  def intersect(o: ValidTimeInterval) = {
    require(this overlaps o)
    ValidTimeInterval(ordering.max(from, o.from), ordering.min(to, o.to))
  }

  def extend(o: ValidTimeInterval) = {
    ValidTimeInterval(ordering.min(from, o.from), ordering.max(to, o.to))
  }

  def close(t: Instant) = {
    require(ordering.gt(t, from))
    require(ordering.lteq(t, to))
    ValidTimeInterval(from, t)
  }

  final override def equals(o: Any): Boolean = o match {
    case that: ValidTimeInterval =>
      from == that.from &&
      to == that.to
    case _ => false
  }
}

object ValidTimeInterval {
  def from(from: ZonedDateTime) = ValidTimeInterval(from.toInstant, TimeInterval.Infinity)
  def from(from: OffsetDateTime) = ValidTimeInterval(from.toInstant, TimeInterval.Infinity)
  def from(from: Instant) = ValidTimeInterval(from, TimeInterval.Infinity)

  def point(from: Instant): ValidTimeInterval = {
    apply(from, instantDiscrete.successor(from))
  }

  val max = new ValidTimeInterval(TimeInterval.max)
}
