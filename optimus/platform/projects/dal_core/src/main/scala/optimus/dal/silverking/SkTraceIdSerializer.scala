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
package optimus.dal.silverking

import com.ms.silverking.cloud.dht.trace.SkForwardState
import com.ms.silverking.cloud.dht.trace.SkTraceId
import optimus.dsi.trace.TraceId
import optimus.platform.dsi.bitemporal.proto.ChainedIdSerializer
import optimus.platform.dsi.bitemporal.proto.Prc.SilverKingTraceIdProto
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer

// Serializer for SilverKing's TraceId
private[optimus] object SkTraceIdSerializer extends ProtoSerializer[SkTraceId, SilverKingTraceIdProto] {
  /* private def fromProto(gpbState: ForwardState): SkForwardState = gpbState match {
    case ForwardState.NOT_FORWARDED    => SkForwardState.NotForwarded
    case ForwardState.LOCAL_FORWARDED  => SkForwardState.LocalForwarded
    case ForwardState.REMOTE_FORWARDED => SkForwardState.RemoteForwarded
  }

  private def toProto(state: SkForwardState): ForwardState = state match {
    case SkForwardState.NotForwarded    => ForwardState.NOT_FORWARDED
    case SkForwardState.LocalForwarded  => ForwardState.LOCAL_FORWARDED
    case SkForwardState.RemoteForwarded => ForwardState.REMOTE_FORWARDED
  } */

  override def serialize(skTraceId: SkTraceId): SilverKingTraceIdProto = ??? /* {
    SilverKingTraceIdProto
      .newBuilder()
      .setType(Type.TRACE_ID)
      .setForwardState(toProto(skTraceId.getForwardState))
      .setRequestUuid(skTraceId.getTraceId.requestId)
      .setChainedId(ChainedIdSerializer.serialize(skTraceId.getTraceId.chainedId))
      .build()
  } */

  override def deserialize(proto: SilverKingTraceIdProto): SkTraceId = ??? /* proto.getType match {
    case Type.TRACE_ID =>
      new SkTraceId(
        TraceId(
          proto.getRequestUuid,
          ChainedIdSerializer.deserialize(proto.getChainedId)
        ),
        fromProto(proto.getForwardState)
      )
  } */

  def traceIdToBytes(skTraceId: SkTraceId): Array[Byte] = ??? // serialize(skTraceId).toByteArray

  def bytesToTraceId(bytes: Array[Byte]): SkTraceId = ??? // deserialize(SilverKingTraceIdProto.parseFrom(bytes))
}
