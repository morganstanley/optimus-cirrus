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

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.PutResponseMessageFormat;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;

public class ProtoPutResponseMessageGroup extends ProtoKeyedMessageGroup {
  private static final int keyBufferAdditionalBytesPerKey = 1;
  private static final int optionsBufferSize = PutResponseMessageFormat.optionBytesSize;

  // FUTURE - is there a better way to compute the 1 index difference
  private static final int shiftedOptionBufferIndex = 0; // on reception header is removed
  private static final int shiftedKeyBufferIndex = 1; // shifted to 1 if no traceID

  public ProtoPutResponseMessageGroup(UUIDBase uuid, long context, long version, int numKeys, byte[] originator,
      byte storageState, int deadlineRelativeMillis, SkTraceId maybeTraceID) {
    super(TraceIDProvider.isValidTraceID(maybeTraceID) ? MessageType.PUT_RESPONSE_TRACE : MessageType.PUT_RESPONSE,
        uuid, context, ByteBuffer.allocate(optionsBufferSize), numKeys, keyBufferAdditionalBytesPerKey, originator,
        deadlineRelativeMillis, ForwardingMode.FORWARD, maybeTraceID);
    if (hasTraceID) {
      // FIXME: This is a temp workaround to preserve parent's base offset put in bufferList (but this leads to the
      //  risk where SK's serde codes could be misused
      bufferList.add(optionsByteBuffer);
    } else {
      // This shifts parent's baseOffset in optionsByteBuffer by 1
      bufferList.add(shiftedOptionBufferIndex, optionsByteBuffer);
    }
    optionsByteBuffer.putLong(version);
    optionsByteBuffer.put(storageState);
    //System.out.printf("optionsByteBuffer\t%s %x %x\n",
    //  StringUtil.byteBufferToHexString(optionsByteBuffer), version, storageState);
  }

  public void addResult(DHTKey key, OpResult result) {
    addKey(key);
    keyByteBuffer.put((byte) result.ordinal());
  }

  public static ByteBuffer getOptionBuffer(MessageGroup mg) {
    if (TraceIDProvider.hasTraceID(mg.getMessageType())) {
      return mg.getBuffers()[ProtoKeyedMessageGroup.getOptionsByteBufferBaseOffset(true)];
    } else {
      return mg.getBuffers()[shiftedOptionBufferIndex];
    }
  }

  public static int getKeyBufferIndex(MessageGroup mg) {
    if (TraceIDProvider.hasTraceID(mg.getMessageType())) {
      // No shift, so it's default index
      return ProtoKeyedMessageGroup.keyByteBufferIndex;
    } else {
      return shiftedKeyBufferIndex;
    }
  }

  public static byte getStorageState(MessageGroup mg) {
    return getOptionBuffer(mg).get(PutResponseMessageFormat.storageStateOffset);
  }
}
