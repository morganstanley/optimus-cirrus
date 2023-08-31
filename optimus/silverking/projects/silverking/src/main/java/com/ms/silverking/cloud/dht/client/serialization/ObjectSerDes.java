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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.object.ObjectUtil;

/**
 * Native Java serializer/deserializer for generic Java objects. With this serialization,
 * interaction is limited to other Java clients.
 */
public final class ObjectSerDes implements BufferSerDes<Object> {
  @Override
  public Object deserialize(ByteBuffer[] buffers) {
    byte[] _bytes;

    _bytes = ByteArraySerDes.deserializeBuffers(buffers);
    try {
      return ObjectUtil.bytesToObj(_bytes);
    } catch (Exception e) {
      throw new RuntimeException("Unable to deserialize", e);
    }
  }

  @Override
  public Object deserialize(ByteBuffer bytes) {
    byte[] _bytes;

    // FUTURE - remove this copy
    _bytes = new byte[bytes.limit() - bytes.position()];
    bytes.get(_bytes);
    try {
      return ObjectUtil.bytesToObj(_bytes);
    } catch (Exception e) {
      throw new RuntimeException("Unable to deserialize", e);
    }
  }

  @Override
  public ByteBuffer serializeToBuffer(Object obj) {
    try {
      return ByteBuffer.wrap(ObjectUtil.objToBytes(obj));
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to serialize", ioe);
    }
  }

  @Override
  public void serializeToBuffer(Object obj, ByteBuffer buffer) {
    try {
      buffer.put(ObjectUtil.objToBytes(obj));
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to serialize", ioe);
    }
  }

  @Override
  public int estimateSerializedSize(Object obj) {
    return 0;
  }

  @Override
  public Object emptyObject() {
    return new Object();
  }
}
