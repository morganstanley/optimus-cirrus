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

import com.ms.silverking.cloud.dht.RetrievalOptions;

/**
 * An asynchronous retrieval. May be polled for partial results. Results are
 * returned as StoredValues.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsyncRetrieval<K, V> extends AsyncKeyedOperation<K> {
  /**
   * RetrievalOptions used in this operation
   *
   * @return the RetrievalOptions used in this operation
   */
  public RetrievalOptions getRetrievalOptions();

  /**
   * Block until this operation is complete.
   *
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public void waitForCompletion() throws RetrievalException;

  /**
   * Returns StoredValues for all successfully complete retrievals.
   *
   * @return StoredValues for all successfully complete retrievals
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public Map<K, ? extends StoredValue<V>> getStoredValues() throws RetrievalException;

  /**
   * Returns StoredValues for the given key if it is present.
   *
   * @param key key to query
   * @return StoredValues for the given key if it is present
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public StoredValue<V> getStoredValue(K key) throws RetrievalException;

  /**
   * Returns StoredValues for all successfully complete retrievals that
   * have completed since the last call to this method or AsyncValueRetrieval.getLatestStoredValues().
   * Each successfully retrieved value will be reported exactly once by this
   * method and AsyncValueRetrieval.getLatestStoredValues().
   * This method is unaffected by calls to getStoredValues().
   * Concurrent execution is permitted, but the precise values returned are
   * undefined.
   *
   * @return StoredValues for all successfully complete retrievals that
   * have completed since the last call to this method or AsyncValueRetrieval.getLatestStoredValues().
   * @throws RetrievalException TODO (OPTIMUS-0000): describe
   */
  public Map<K, ? extends StoredValue<V>> getLatestStoredValues() throws RetrievalException;
}
