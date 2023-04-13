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
package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

import com.ms.silverking.cloud.dht.trace.SkForwardState;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.numeric.NumConversion;
import optimus.breadcrumbs.ChainedID;
import optimus.dsi.trace.TraceId;

public class ProtoSkTraceIdSerialize {
  private static Function<byte[], Optional<SkTraceId>> customDeserializer = null;

  public static void registerCustomDeserializer(Function<byte[], Optional<SkTraceId>> fn) {
    customDeserializer = fn;
  }

  public static Function<byte[], Optional<SkTraceId>> getCustomDeserializer() {
    return customDeserializer;
  }

  public static ByteBuffer serializeSkTraceId(SkTraceId skTraceId) {

    byte[] forwardStateBytes = skTraceId.getForwardState().toString().getBytes(StandardCharsets.UTF_8);
    byte[] requestUuIdBytes = skTraceId.getTraceId().requestId().getBytes(StandardCharsets.UTF_8);
    byte[] chainedIdBytes = ChainedIdSerialization.serializeChainId(skTraceId.getTraceId().chainedId()).array();

    int totalEncodeLength = (forwardStateBytes.length +
                             requestUuIdBytes.length +
                             chainedIdBytes.length +
                             NumConversion.BYTES_PER_INT * 4);
    ByteBuffer traceBuffer = ByteBuffer.allocate(totalEncodeLength);

    traceBuffer.putInt(NumConversion.BYTES_PER_INT);
    traceBuffer.putInt(serializeForwardState(skTraceId.getForwardState()));
    traceBuffer.putInt(requestUuIdBytes.length);
    traceBuffer.put(requestUuIdBytes);
    traceBuffer.putInt(chainedIdBytes.length);
    traceBuffer.put(chainedIdBytes);

    return traceBuffer;
  }

  private static int serializeForwardState(SkForwardState skForwardState) {
    int result = 0;

    switch (skForwardState) {
      case NotForwarded:
        result = 1;
        break;
      case LocalForwarded:
        result = 2;
        break;
      case RemoteForwarded:
        result = 3;
        break;
    }
    return result;
  }

  public SkTraceId backwardsCompatibleDeserialize(byte[] incomingbyte) {
    ByteBuffer copyByteBuffer;
    copyByteBuffer = ByteBuffer.wrap(incomingbyte);

    if (customDeserializer == null) {
      return deserializeSkTraceId(copyByteBuffer);
    }
    Optional<SkTraceId> result = customDeserializer.apply(incomingbyte);
    return result.orElseGet(() -> deserializeSkTraceId(copyByteBuffer));
  }

  static SkTraceId deserializeSkTraceId(ByteBuffer bytebuffer) {
    int forwardStateLenght, requestIdLength, chainedIdLength, offset = 0;
    byte[] requestUuIdBytes, chainedIdBytes;
    SkForwardState forwardState;
    bytebuffer.position(0);

    forwardStateLenght = bytebuffer.getInt(offset);
    offset += NumConversion.BYTES_PER_INT;
    bytebuffer.position(offset);
    int forwardStateValue = bytebuffer.getInt(offset);
    offset += forwardStateLenght;
    bytebuffer.position(offset);

    requestIdLength = bytebuffer.getInt(offset);
    requestUuIdBytes = new byte[requestIdLength];
    offset += NumConversion.BYTES_PER_INT;
    bytebuffer.position(offset);
    bytebuffer.get(requestUuIdBytes, 0, requestIdLength);
    String requestId = new String(requestUuIdBytes);
    offset += requestIdLength;
    bytebuffer.position(offset);

    chainedIdLength = bytebuffer.getInt(offset);
    chainedIdBytes = new byte[chainedIdLength];
    offset += NumConversion.BYTES_PER_INT;
    bytebuffer.position(offset);
    bytebuffer.get(chainedIdBytes, 0, chainedIdLength);
    ByteBuffer chainedIdbuffer = ByteBuffer.wrap(chainedIdBytes);
    ChainedID chainedID = ChainedIdSerialization.deserializeChainId(chainedIdbuffer);

    forwardState = deserializeForwardState(forwardStateValue);

    return new SkTraceId(new TraceId(requestId, chainedID), forwardState);
  }

  private static SkForwardState deserializeForwardState(int assignedvalue) {

    switch (assignedvalue) {
      case 1:
        return SkForwardState.NotForwarded;
      case 2:
        return SkForwardState.LocalForwarded;
      case 3:
        return SkForwardState.RemoteForwarded;
    }
    return null;
  }
}

