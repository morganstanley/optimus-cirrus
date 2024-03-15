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

import com.ms.silverking.numeric.NumConversion;

/** Serializer/deserializer for Long */
public final class LongSerDes implements BufferSerDes<Long> {
  @Override
  public ByteBuffer serializeToBuffer(Long n) {
    return ByteBuffer.wrap(NumConversion.longToBytes(n));
  }

  @Override
  public void serializeToBuffer(Long n, ByteBuffer buffer) {
    buffer.put(NumConversion.longToBytes(n));
  }

  @Override
  public int estimateSerializedSize(Long n) {
    return NumConversion.BYTES_PER_LONG;
  }

  @Override
  public Long deserialize(ByteBuffer[] buffers) {
    byte[] def;

    def = ByteArraySerDes.deserializeBuffers(buffers);
    if (def.length != NumConversion.BYTES_PER_LONG) {
      throw new RuntimeException("Unable to deserialize Long def length: " + def.length);
    }
    return NumConversion.bytesToLong(def);
  }

  @Override
  public Long deserialize(ByteBuffer buffer) {
    return buffer.getLong(buffer.position());
  }

  @Override
  public Long emptyObject() {
    return Long.valueOf(0);
  }
}
