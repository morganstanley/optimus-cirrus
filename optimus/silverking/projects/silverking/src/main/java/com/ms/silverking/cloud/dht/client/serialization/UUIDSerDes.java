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
import java.util.UUID;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.id.UUIDUtil;

/**
 * Serializer/deserializer for UUID
 */
public final class UUIDSerDes implements BufferSerDes<UUID> {
  @Override
  public ByteBuffer serializeToBuffer(UUID uuid) {
    return ByteBuffer.wrap(UUIDUtil.uuidToBytes(uuid));
  }

  @Override
  public void serializeToBuffer(UUID uuid, ByteBuffer buffer) {
    buffer.put(UUIDUtil.uuidToBytes(uuid));
  }

  @Override
  public int estimateSerializedSize(UUID uuid) {
    return UUIDUtil.BYTES_PER_UUID;
  }

  @Override
  public UUID deserialize(ByteBuffer[] buffers) {
    byte[] def;

    def = ByteArraySerDes.deserializeBuffers(buffers);
    if (def.length != UUIDUtil.BYTES_PER_UUID) {
      throw new RuntimeException("Unable to deserialize UUID def length: " + def.length);
    }
    return UUIDUtil.bytesToUUID(def);
  }

  @Override
  public UUID deserialize(ByteBuffer buffer) {
    return UUIDUtil.getUUID(buffer);
  }

  @Override
  public UUID emptyObject() {
    return new UUIDBase(0, 0).getUUID();
  }
}
