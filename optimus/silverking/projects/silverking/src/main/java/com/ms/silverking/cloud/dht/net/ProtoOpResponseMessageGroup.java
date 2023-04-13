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
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.id.UUIDBase;

public class ProtoOpResponseMessageGroup extends ProtoMessageGroup {
  private final ByteBuffer reponseByteBuffer;

  private static final int dataBufferIndex = 0;

  // room for response code
  private static final int responseBufferSize = 1;
  private static final int resultIndex = 0;

  public ProtoOpResponseMessageGroup(UUIDBase uuid, long context, OpResult result, byte[] originator,
      int deadlineRelativeMillis) {
    super(MessageType.OP_RESPONSE, uuid, context, originator, deadlineRelativeMillis, ForwardingMode.FORWARD);

    reponseByteBuffer = ByteBuffer.allocate(responseBufferSize);
    bufferList.add(reponseByteBuffer);
    reponseByteBuffer.put((byte) result.ordinal());
  }

  @Override
  public boolean isNonEmpty() {
    return true;
  }

  public static OpResult result(MessageGroup mg) {
    return OpResult.values()[mg.getBuffers()[dataBufferIndex].get(resultIndex)];
  }
}
