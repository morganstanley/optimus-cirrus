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
 * Serializer/deserializer for Integer
 */
public final class IntegerSerDes implements BufferSerDes<Integer> {
  @Override
  public ByteBuffer serializeToBuffer(Integer n) {
    return ByteBuffer.wrap(NumConversion.intToBytes(n));
  }

  @Override
  public void serializeToBuffer(Integer n, ByteBuffer buffer) {
    buffer.put(NumConversion.intToBytes(n));
  }

  @Override
  public int estimateSerializedSize(Integer n) {
    return NumConversion.BYTES_PER_INT;
  }

  @Override
  public Integer deserialize(ByteBuffer[] buffers) {
    byte[] def;

    def = ByteArraySerDes.deserializeBuffers(buffers);
    if (def.length != NumConversion.BYTES_PER_INT) {
      throw new RuntimeException("Unable to deserialize integer def length: " + def.length);
    }
    return NumConversion.bytesToInt(def);
  }

  @Override
  public Integer deserialize(ByteBuffer buffer) {
    return buffer.getInt(buffer.position());
  }

  @Override
  public Integer emptyObject() {
    return new Integer(0);
  }
}
