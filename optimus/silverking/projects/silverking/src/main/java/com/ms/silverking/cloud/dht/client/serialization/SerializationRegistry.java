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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Registry for all implemented serializers. Allows retrieval by type.
 */
public class SerializationRegistry {
  private final Map<Class, BufferDestSerializer> serializers;
  private final Map<Class, BufferSourceDeserializer> deserializers;

  public static SerializationRegistry createEmptyRegistry() {
    return new SerializationRegistry();
  }

  public static SerializationRegistry createDefaultRegistry() {
    SerializationRegistry defaultRegistry;

    defaultRegistry = new SerializationRegistry();
    defaultRegistry.addSerDes(byte[].class, new RawByteArraySerDes());
    defaultRegistry.addSerDes(String.class, new StringSerDes());
    defaultRegistry.addSerDes(Object.class, new ObjectSerDes());
    defaultRegistry.addSerDes(Long.class, new LongSerDes());
    defaultRegistry.addSerDes(Integer.class, new IntegerSerDes());
    defaultRegistry.addSerDes(Short.class, new ShortSerDes());
    defaultRegistry.addSerDes(UUID.class, new UUIDSerDes());
    return defaultRegistry;
  }

  private SerializationRegistry() {
    serializers = new HashMap<Class, BufferDestSerializer>();
    deserializers = new HashMap<Class, BufferSourceDeserializer>();
  }

  public <T> void addSerDes(Class<T> srcClass, BufferSerDes<T> serDes) {
    addSerializer(srcClass, serDes);
    addDeserializer(srcClass, serDes);
  }

  public <T> void addSerializer(Class<T> srcClass, BufferDestSerializer<T> serializer) {
    serializers.put(srcClass, serializer);
  }

  public <T> void addDeserializer(Class<T> destClass, BufferSourceDeserializer<T> deserializer) {
    deserializers.put(destClass, deserializer);
  }

  public <T> BufferDestSerializer<T> getSerializer(Class<T> srcClass) {
    return (BufferDestSerializer<T>) serializers.get(srcClass);
  }

  public <T> BufferSourceDeserializer<T> getDeserializer(Class<T> destClass) {
    return (BufferSourceDeserializer<T>) deserializers.get(destClass);
  }

  //public <T> BufferDestSerializer<T> getBufferDestSerializer(Class<T> srcClass) {
  //    return (BufferDestSerializer<T>)serializers.get(srcClass);
  //}
}
