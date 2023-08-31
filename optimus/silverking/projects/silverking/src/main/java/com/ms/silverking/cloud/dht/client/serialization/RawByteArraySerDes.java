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
package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.io.util.BufferUtil;

/** Serializer/deserializer for byte[]. No copy of source data is made for put(). */
public final class RawByteArraySerDes implements BufferSerDes<byte[]> {
  @Override
  public ByteBuffer serializeToBuffer(byte[] b) {
    return ByteBuffer.wrap(b);
  }

  @Override
  public void serializeToBuffer(byte[] b, ByteBuffer buffer) {
    buffer.put(b);
  }

  @Override
  public int estimateSerializedSize(byte[] b) {
    return b.length;
  }

  @Override
  public byte[] deserialize(ByteBuffer[] buffers) {
    int totalRemaining;
    byte[] array;
    int totalDeserialized;

    totalRemaining = BufferUtil.totalRemaining(buffers);
    array = new byte[totalRemaining];
    totalDeserialized = 0;
    for (ByteBuffer buffer : buffers) {
      int bufferRemaining;

      bufferRemaining = buffer.remaining();
      buffer.get(array, totalDeserialized, bufferRemaining);
      totalDeserialized += bufferRemaining;
    }
    return array;
  }

  @Override
  public byte[] deserialize(ByteBuffer buffer) {
    return Arrays.copyOfRange(
        buffer.array(), buffer.position(), buffer.position() + buffer.remaining());
  }

  @Override
  public byte[] emptyObject() {
    return DHTConstants.emptyByteArray;
  }
}
