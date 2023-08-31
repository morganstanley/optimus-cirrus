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
import java.util.Set;

import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;

/**
 * A namespace perspective that provides asynchronous write operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsynchronousWritableNamespacePerspective<K, V>
    extends BaseNamespacePerspective<K, V> {
  /**
   * Multi-value Put operation
   *
   * @param values map of key-value pairs to store
   * @param putOptions options for the Put operation
   * @return an AsyncPut object representing this operation
   */
  public AsyncPut<K> put(Map<? extends K, ? extends V> values, PutOptions putOptions);

  /**
   * Multi-value Put operation using default PutOptions.
   *
   * @param values map of key-value pairs to store
   * @return an AsyncPut object representing this operation
   */
  public default AsyncPut<K> put(Map<? extends K, ? extends V> values) {
    return put(values, getOptions().getDefaultPutOptions());
  }

  /**
   * Single-value Put operation.
   *
   * @param key key to associate the value with
   * @param value value to store
   * @param putOptions options for the Put operation
   * @return an AsyncPut object representing this operation
   */
  public AsyncPut<K> put(K key, V value, PutOptions putOptions);

  /**
   * Single-value Put operation using default PutOptions.
   *
   * @param key key to associate the value with
   * @param value value to store
   * @return an AsyncPut object representing this operation
   */
  public default AsyncPut<K> put(K key, V value) {
    return put(key, value, getOptions().getDefaultPutOptions());
  }

  /**
   * Multi-value Invalidation operation
   *
   * @param keys keys to invalidate
   * @param invalidationOptions options for the Invalidation operation
   * @return an AsyncInvalidation object representing this operation
   */
  public AsyncInvalidation<K> invalidate(
      Set<? extends K> keys, InvalidationOptions invalidationOptions);

  /**
   * Multi-value Invalidation operation using default InvalidationOptions.
   *
   * @param keys keys to invalidate
   * @return an AsyncInvalidation object representing this operation
   */
  public default AsyncInvalidation<K> invalidate(Set<? extends K> keys) {
    return invalidate(keys, getOptions().getDefaultInvalidationOptions());
  }

  /**
   * Single-value Invalidation operation.
   *
   * @param key key to invalidate
   * @param invalidationOptions options for the Invalidation operation
   * @return an AsyncInvalidation object representing this operation
   */
  public AsyncInvalidation<K> invalidate(K key, InvalidationOptions invalidationOptions);

  /**
   * Single-value Invalidation operation using default InvalidationOptions.
   *
   * @param key key to invalidate
   * @return an AsyncInvalidation object representing this operation
   */
  public default AsyncInvalidation<K> invalidate(K key) {
    return invalidate(key, getOptions().getDefaultInvalidationOptions());
  }

  /*
  public AsyncSnapshot snapshot(long version);
  public AsyncSnapshot snapshot();
  */

  /*
  public AsyncSyncRequest syncRequest(long version);
  public AsyncSyncRequest syncRequest();
  */
}
