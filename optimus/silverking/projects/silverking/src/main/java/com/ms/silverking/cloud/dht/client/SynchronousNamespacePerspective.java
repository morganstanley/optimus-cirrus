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
package com.ms.silverking.cloud.dht.client;

import java.util.Map;

/**
 * Read/write interface to the DHT. Synchronous - all methods block until they are complete.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SynchronousNamespacePerspective<K, V>
    extends SynchronousWritableNamespacePerspective<K, V>,
        SynchronousReadableNamespacePerspective<K, V> {
  /**
   * Create a view of this namespace perspective as a java.util.Map. Operations supported are:
   * containsKey(), get(), put(), and putAll().
   *
   * @return Map view of this namespace perspective.
   */
  public Map<K, V> asMap();
}
