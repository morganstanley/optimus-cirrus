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

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;

// Consider removing this class (if snapshots are removed)
public class ProtoSnapshotMessageGroup extends ProtoVersionedBasicOpMessageGroup {
  /*
   * FUTURE - delete this when verified/tested
   *
  private final ByteBuffer    dataByteBuffer;

  private static final int    dataBufferIndex = 0;
  // room for UUID + version
  private static final int    dataBufferSize = 3 * NumConversion.BYTES_PER_LONG;
  private static final int    uuidMSLOffset = 0;
  private static final int    uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int    versionOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
  */

  public ProtoSnapshotMessageGroup(
      MessageType type, UUIDBase uuid, long context, long version, byte[] originator) {
    super(type, uuid, context, version, originator);
    /*
    dataByteBuffer = ByteBuffer.allocate(dataBufferSize);
    bufferList.add(dataByteBuffer);
    dataByteBuffer.putLong(uuid.getMostSignificantBits());
    dataByteBuffer.putLong(uuid.getLeastSignificantBits());
    dataByteBuffer.putLong(version);
    */
  }

  @Override
  public boolean isNonEmpty() {
    return true;
  }

  /*
  public static long getUUIDMSL(MessageGroup mg) {
      return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
  }

  public static long getUUIDLSL(MessageGroup mg) {
      return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
  }

  public static long getVersion(MessageGroup mg) {
      return mg.getBuffers()[dataBufferIndex].getLong(versionOffset);
  }
  */
}
