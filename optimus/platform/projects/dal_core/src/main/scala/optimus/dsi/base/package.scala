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
package optimus.dsi

import java.time.Instant

import optimus.dsi.base.actions.{AddBlobAction, AddEntityBlob, AddEventBlob}
import optimus.platform.dal.DalChronon
import optimus.platform.dsi.bitemporal.GetProtoFileProperties
import optimus.platform.dsi.bitemporal.Heartbeat
import optimus.platform.dsi.bitemporal.RoleMembershipQuery
import optimus.platform.dsi.bitemporal.DSIQueryTemporality._
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable._
import java.time.Duration

import optimus.platform.TimeInterval

package object base {

  // http://stackoverflow.com/questions/3431658/scala-immutable-multimap
  /**
   * Like {@link scala.collection.Traversable#groupBy} but lets you return both the key and the value for the resulting
   * Map-of-Lists, rather than just the key.
   *
   * And you use it like this:
   *
   * val xs = (1 to 10).toList groupBy2(xs) { case i => (i%2 == 0, i.toDouble) } res3: Map[Boolean,List[Double]] =
   * Map(false -> List(1.0, 3.0, 5.0, 7.0, 9.0), true -> List(2.0, 4.0, 6.0, 8.0, 10.0))
   *
   * @param in
   *   the input list
   * @param f
   *   the function that maps elements in the input list to a tuple for the output map.
   * @tparam A
   *   the type of elements in the source list
   * @tparam B
   *   the type of the first element of the tuple returned by the function; will be used as keys for the result
   * @tparam C
   *   the type of the second element of the tuple returned by the function; will be used as values for the result
   * @return
   *   a Map-of-Lists
   */
  def groupBy2[A, B, C](in: List[A])(f: PartialFunction[A, (B, C)]): Map[B, List[C]] = {

    def _groupBy2[A, B, C](in: List[A], got: Map[B, List[C]], f: PartialFunction[A, (B, C)]): Map[B, List[C]] =
      in match {
        case Nil =>
          got.map { case (k, vs) => (k, vs.reverse) }

        case x :: xs if f.isDefinedAt(x) =>
          val (b, c) = f(x)
          val appendTo = got.getOrElse(b, Nil)
          _groupBy2(xs, got.updated(b, c :: appendTo), f)

        case x :: xs =>
          _groupBy2(xs, got, f)
      }

    _groupBy2(in, Map.empty, f)
  }

  /**
   * Renaming of the above function.
   */
  def immutableMultimapOf[A, B, C](in: List[A])(f: PartialFunction[A, (B, C)]): Map[B, List[C]] =
    groupBy2(in)(f)

  /**
   * Vanilla case for taking a generated List[(A,B)] and turning them into a Map[A,List[B]]
   */
  def immutableMultimapOf[A, B](in: List[(A, B)]): Map[A, List[B]] =
    groupBy2(in) { case (a, b) => (a, b) }

  type EntityVersionHandle = VersionHandle[SerializedEntity]
  type EventVersionHandle = VersionHandle[SerializedBusinessEvent]
  sealed trait AddBlobHandle[BlobType <: SerializedStorable] {
    def blobAction: AddBlobAction
    def versionHandle: VersionHandle[BlobType]
  }
  final case class AddEntityHandle(blobAction: AddEntityBlob, user: String) extends AddBlobHandle[SerializedEntity] {
    override def versionHandle = blobAction.versionHandle(user)
  }
  final case class AddEventHandle(blobAction: AddEventBlob, user: String)
      extends AddBlobHandle[SerializedBusinessEvent] {
    override def versionHandle = blobAction.versionHandle(user)
  }
  final case class SlottedVersionedReference(vref: VersionedReference, slot: Int)

  private[dsi] val ClockIncrementNano = DalChronon

  val dalQuantum: Duration = Duration.ofNanos(ClockIncrementNano)

  def extractReadTxTime(cmd: ReadOnlyCommand): Instant = {
    def fromTemporality(temporality: DSIQueryTemporality) = temporality match {
      case All(rtt)             => rtt
      case TxTime(tx)           => tx
      case ValidTime(vt, rtt)   => rtt
      case At(vt, tt)           => tt
      case TxRange(range)       => range.to
      case b: BitempRange       => b.readTxTime
      case OpenVtTxRange(range) => range.to
    }

    cmd match {
      case wtmp: ReadOnlyCommandWithTemporality => fromTemporality(wtmp.temporality)
      case wtt: ReadOnlyCommandWithTT           => wtt.tt
      case _: GetInfo | _: ErrorCommand | VoidCommand | _: SessionTokenRequest | _: CreateNewSession |
          _: QueryEntityMetadata | _: GetSlots | _: CanPerformAction | _: IsEntitled =>
        TimeInterval.NegInfinity
      case ex: ExpressionQueryCommand    => ex.latestTT
      case CountGroupings(_, readTxTime) => readTxTime
      // a heartbeat command should never make it's way here!
      case _: Heartbeat | _: RoleMembershipQuery | _: GetProtoFileProperties =>
        throw new IllegalArgumentException(
          "Cannot extract 'tt' from Heartbeat/RoleMembershipQuery/GetProtoFileProperties commands")
    }
  }

  def extractPubSubTxTime(cmd: PubSubCommand, liveTime: Instant): Instant = {
    cmd match {
      case c: CreatePubSubStream => c.startTime.getOrElse(liveTime)
      case _: ChangeSubscription => liveTime
      case _: ClosePubSubStream  => liveTime
    }
  }

  def getTTTooFarAheadMsg(
      maxCommandRequestTxTime: Instant,
      fencepost: (String, Instant),
      maxGapInSeconds: Int): String = {
    s"$getTTTooFarAheadMsgPrefix ${maxCommandRequestTxTime} is too far ahead of the query latest safe query time ${fencepost} with tolerance of ${maxGapInSeconds} seconds."
  }

  private[optimus] val getTTTooFarAheadMsgPrefix = "Unable to complete request as max requested transaction time"
}
