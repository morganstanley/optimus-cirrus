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
 * An asynchronous retrieval that supports raw value access. May be polled for partial results.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsyncValueRetrieval<K, V> extends AsyncRetrieval<K, V> {
  /**
   * Returns raw values for all successfully complete retrievals.
   *
   * @return raw values for all successfully complete retrievals
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public Map<K, V> getValues() throws RetrievalException;

  /**
   * Returns the raw value for the given key if it is present.
   *
   * @param key the given key
   * @return the raw value for the given key if it is present
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public V getValue(K key) throws RetrievalException;

  /**
   * Returns raw values for all successfully complete retrievals that
   * have completed since the last call to this method and getLatestStoredValues().
   * Each successfully retrieved value will be reported exactly once by this
   * method and getLatestStoredValues().
   * This method is unaffected by calls to either getValues() or getStoredValues().
   * Concurrent execution is permitted, but the precise values returned are
   * undefined.
   *
   * @return raw values for all successfully complete retrievals that
   * have completed since the last call to this method and getLatestStoredValues()
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public Map<K, V> getLatestValues() throws RetrievalException;

}
