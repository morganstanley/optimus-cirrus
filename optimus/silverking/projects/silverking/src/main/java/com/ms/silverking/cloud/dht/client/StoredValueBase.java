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

/**
 * Bundles values and metadata retrieved from a DHT. Will contain a value and/or metadata depending
 * on what form of retrieve was used to obtain the instance.
 *
 * @param <V> value type
 */
public interface StoredValueBase<V> {
  /**
   * Get the MetaData for this stored value if it exists
   *
   * @return the MetaData for this stored value if it exists. null if no meta data was retrieved.
   */
  public MetaData getMetaData();

  /**
   * Get the value for this stored value if it exists
   *
   * @return the value for this stored value if it exists. null if no value was retrieved.
   */
  public V getValue();
}
