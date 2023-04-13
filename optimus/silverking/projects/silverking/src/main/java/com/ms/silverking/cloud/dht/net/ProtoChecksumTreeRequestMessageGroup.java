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
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.numeric.NumConversion;

public class ProtoChecksumTreeRequestMessageGroup extends ProtoMessageGroup {
  private final ByteBuffer dataByteBuffer;

  private static final int dataBufferIndex = 0;
  // room for UUID + targetCP + sourceCP + region start + region end + replica + localFlag
  private static final int dataBufferSize =
      4 * NumConversion.BYTES_PER_LONG + 2 * ConvergencePoint.serializedSizeBytes + IPAndPort.SIZE_BYTES + 1;
  private static final int uuidMSLOffset = 0;
  private static final int uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;

  private static final int targetCPOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int sourceCPOffset = targetCPOffset + ConvergencePoint.serializedSizeBytes;

  private static final int regionStartOffset = sourceCPOffset + ConvergencePoint.serializedSizeBytes;
  private static final int regionEndOffset = regionStartOffset + NumConversion.BYTES_PER_LONG;

  private static final int replicaOffset = regionEndOffset + NumConversion.BYTES_PER_LONG;

  private static final int localFlagOffset = replicaOffset + IPAndPort.SIZE_BYTES;

  private static final int deadlineRelativeMillis = 25 * 60 * 1000; // For now, interpreted as a progress deadline,
  // not at absolute op deadline

  public ProtoChecksumTreeRequestMessageGroup(UUIDBase uuid, long context, ConvergencePoint targetCP,
      ConvergencePoint curCP, byte[] originator, RingRegion ringRegion, IPAndPort replica, boolean localFlag) {
    super(MessageType.CHECKSUM_TREE_REQUEST, uuid, context, originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
    dataByteBuffer = ByteBuffer.allocate(dataBufferSize);
    bufferList.add(dataByteBuffer);
    dataByteBuffer.putLong(uuid.getMostSignificantBits());
    dataByteBuffer.putLong(uuid.getLeastSignificantBits());

    targetCP.writeToBuffer(dataByteBuffer);
    curCP.writeToBuffer(dataByteBuffer);

    dataByteBuffer.putLong(ringRegion.getStart());
    dataByteBuffer.putLong(ringRegion.getEnd());

    dataByteBuffer.put(replica.toByteArray());

    dataByteBuffer.put(localFlag ? (byte) 1 : (byte) 0);
  }

  public ProtoChecksumTreeRequestMessageGroup(UUIDBase uuid, long context, ConvergencePoint targetCP,
      ConvergencePoint curCP, byte[] originator, RingRegion ringRegion, boolean localFlag) {
    this(uuid, context, targetCP, curCP, originator, ringRegion, IPAndPort.emptyIPAndPort, localFlag);
  }

  @Override
  public boolean isNonEmpty() {
    return true;
  }

  public static long getUUIDMSL(MessageGroup mg) {
    return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
  }

  public static long getUUIDLSL(MessageGroup mg) {
    return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
  }

  public static ConvergencePoint getTargetConvergencePoint(MessageGroup mg) {
    return ConvergencePoint.readFromBuffer(mg.getBuffers()[dataBufferIndex], targetCPOffset);
  }

  public static ConvergencePoint getSourceConvergencePoint(MessageGroup mg) {
    return ConvergencePoint.readFromBuffer(mg.getBuffers()[dataBufferIndex], sourceCPOffset);
  }

  public static RingRegion getRegion(MessageGroup mg) {
    long start;
    long end;

    start = mg.getBuffers()[dataBufferIndex].getLong(regionStartOffset);
    end = mg.getBuffers()[dataBufferIndex].getLong(regionEndOffset);
    return new RingRegion(start, end);
  }

  public static IPAndPort getReplica(MessageGroup mg) {
    return IPAndPort.fromByteBuffer(mg.getBuffers()[dataBufferIndex], replicaOffset);
  }

  public static boolean getLocalFlag(MessageGroup mg) {
    return mg.getBuffers()[dataBufferIndex].get(localFlagOffset) != 0;
  }
}
