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
import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoSetConvergenceStateMessageGroup extends ProtoMessageGroup {
  private boolean isNonEmpty;

  private static final int dataBufferIndex = 0;
  // room for UUID + version
  private static final int uuidMSLOffset = 0;
  private static final int uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int curCPOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int targetCPOffset = curCPOffset + ConvergencePoint.serializedSizeBytes;
  private static final int ringStateOffset = targetCPOffset + ConvergencePoint.serializedSizeBytes;
  private static final int dataBufferSizeBytes = ringStateOffset + 1;

  public ProtoSetConvergenceStateMessageGroup(UUIDBase uuid, byte[] originator, int deadlineRelativeMillis,
      ConvergencePoint curCP, ConvergencePoint targetCP, RingState ringState) {
    super(MessageType.SET_CONVERGENCE_STATE, uuid, 0, originator, deadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD);

    ByteBuffer buffer;

    buffer = ByteBuffer.allocate(dataBufferSizeBytes);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getUUID().getLeastSignificantBits());

    curCP.writeToBuffer(buffer);
    targetCP.writeToBuffer(buffer);
    buffer.put((byte) ringState.ordinal());

    buffer.flip();
    bufferList.add(buffer);
  }

  public MessageGroup toMessageGroup() {
    return toMessageGroup(false);
  }

  public void addToMessageGroupList(List<MessageGroup> messageGroups) {
    super.addToMessageGroupList(messageGroups);
  }

  @Override
  public boolean isNonEmpty() {
    return isNonEmpty;
  }

  public void setNonEmpty() {
    isNonEmpty = true;
  }

  /////////////////////

  public static long getUUIDMSL(MessageGroup mg) {
    return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
  }

  public static long getUUIDLSL(MessageGroup mg) {
    return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
  }

  public static ConvergencePoint getCurCP(MessageGroup mg) {
    return ConvergencePoint.readFromBuffer(mg.getBuffers()[dataBufferIndex], curCPOffset);
  }

  public static ConvergencePoint getTargetCP(MessageGroup mg) {
    return ConvergencePoint.readFromBuffer(mg.getBuffers()[dataBufferIndex], targetCPOffset);
  }

  public static RingState getRingState(MessageGroup mg) {
    return RingState.values()[mg.getBuffers()[dataBufferIndex].get(ringStateOffset)];
  }
}
