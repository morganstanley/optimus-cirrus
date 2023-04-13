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

/**
 * Serializer/deserializer for Short
 */
public final class ShortSerDes implements BufferSerDes<Short> {
  @Override
  public ByteBuffer serializeToBuffer(Short n) {
    return ByteBuffer.wrap(NumConversion.shortToBytes(n));
  }

  @Override
  public void serializeToBuffer(Short n, ByteBuffer buffer) {
    buffer.put(NumConversion.shortToBytes(n));
  }

  @Override
  public int estimateSerializedSize(Short n) {
    return NumConversion.BYTES_PER_SHORT;
  }

  @Override
  public Short deserialize(ByteBuffer[] buffers) {
    byte[] def;

    def = ByteArraySerDes.deserializeBuffers(buffers);
    if (def.length != NumConversion.BYTES_PER_SHORT) {
      throw new RuntimeException("Unable to deserialize Short def length: " + def.length);
    }
    return NumConversion.bytesToShort(def);
  }

  @Override
  public Short deserialize(ByteBuffer buffer) {
    return buffer.getShort(buffer.position());
  }

  @Override
  public Short emptyObject() {
    return new Short((short) 0);
  }
}
