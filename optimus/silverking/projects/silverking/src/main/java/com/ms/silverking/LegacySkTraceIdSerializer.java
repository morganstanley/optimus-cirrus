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
package com.ms.silverking;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ms.silverking.cloud.dht.trace.SkForwardState;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import optimus.dsi.trace.TraceId;
import com.ms.silverking.proto.Silverking.*;

public class LegacySkTraceIdSerializer {
  LegacyChainedIdSerializer legacyChainedIdSerializer = new LegacyChainedIdSerializer();

  private SkForwardState fromProto(SilverKingTraceIdProto.ForwardState gpbstate) {

    switch (gpbstate) {
      case NOT_FORWARDED:
        return SkForwardState.NotForwarded;
        // break;
      case LOCAL_FORWARDED:
        return SkForwardState.LocalForwarded;
        // break;
      case REMOTE_FORWARDED:
        return SkForwardState.RemoteForwarded;
        // break;
    }
    return null;
  }

  private SilverKingTraceIdProto.ForwardState toProto(SkForwardState state) {

    switch (state) {
      case NotForwarded:
        return SilverKingTraceIdProto.ForwardState.NOT_FORWARDED;
        // break;
      case LocalForwarded:
        return SilverKingTraceIdProto.ForwardState.LOCAL_FORWARDED;
        // break;
      case RemoteForwarded:
        return SilverKingTraceIdProto.ForwardState.REMOTE_FORWARDED;
        // break;
    }
    return null;
  }

  public SilverKingTraceIdProto serialize(SkTraceId skTraceId) {

    return SilverKingTraceIdProto.newBuilder()
        .setType(SilverKingTraceIdProto.Type.TRACE_ID)
        .setForwardState(toProto(skTraceId.getForwardState()))
        .setRequestUuid(skTraceId.getTraceId().requestId())
        .setChainedId(legacyChainedIdSerializer.serialize(skTraceId.getTraceId().chainedId()))
        .build();
  }

  public SkTraceId deserialize(SilverKingTraceIdProto proto) {

    if (proto != null) {
      return new SkTraceId(
          new TraceId(
              proto.getRequestUuid(), legacyChainedIdSerializer.deserialize(proto.getChainedId())),
          fromProto(proto.getForwardState()));
    }
    return null;
  }

  public byte[] traceIdToBytes(SkTraceId skTraceId) {
    return serialize(skTraceId).toByteArray();
  }

  public SkTraceId bytesToTraceId(byte[] bytes) throws InvalidProtocolBufferException {
    return deserialize(SilverKingTraceIdProto.parseFrom(bytes));
  }
}
