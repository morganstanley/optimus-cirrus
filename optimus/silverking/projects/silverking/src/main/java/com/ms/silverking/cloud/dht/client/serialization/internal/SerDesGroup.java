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
package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;

/**
 * Groups serializers and deserializers for ease-of-use.
 * <p>
 * Currently unused
 * FUTURE - consider removing this class
 */
public final class SerDesGroup<K, V> {
  //private final ArrayDestSerializer<K>        keySerializer;
  private final BufferSourceDeserializer<K> keyDeserializer;
  private final BufferDestSerializer<V> valueSerializer;
  private final BufferSourceDeserializer<V> valueDeserializer;

  public SerDesGroup(//ArrayDestSerializer<K> keySerializer,
      BufferSourceDeserializer<K> keyDeserializer, BufferDestSerializer<V> valueSerializer,
      BufferSourceDeserializer<V> valueDeserializer) {
    //this.keySerializer = keySerializer;
    this.keyDeserializer = keyDeserializer;
    this.valueSerializer = valueSerializer;
    this.valueDeserializer = valueDeserializer;
  }

  //public ArrayDestSerializer<K> getKeySerializer() {
  //    return keySerializer;
  //}

  public BufferSourceDeserializer<K> getKeyDeserializer() {
    return keyDeserializer;
  }

  public BufferDestSerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  public BufferSourceDeserializer<V> getValueDeserializer() {
    return valueDeserializer;
  }
}
