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
import com.ms.silverking.cloud.dht.common.DHTKey;

public class SerializedMD5KeyCreator<K> extends BaseKeyCreator<K> {
  // private final Map<String,DHTKey>        cachedKeys;
  private final BufferDestSerializer<K> serializer;
  private final ArrayMD5KeyCreator arrayMD5KeyCreator;

  // private static final int    cacheCapacity = 1024;
  // private static final int    cacheConcurrencyLevel = 8;

  // FUTURE think about allowing users to override

  public SerializedMD5KeyCreator(BufferDestSerializer<K> serializer) {
    super();
    this.serializer = serializer;
    arrayMD5KeyCreator = new ArrayMD5KeyCreator();
    // cachedKeys = new
    // MapMaker().concurrencyLevel(cacheConcurrencyLevel).initialCapacity(cacheCapacity).makeMap();
    // cachedKeys = null;
    // FUTURE - could consider using cached keys
  }

  @Override
  public DHTKey createKey(K key) {
    return arrayMD5KeyCreator.createKey(serializer.serializeToBuffer(key).array());
  }
}
