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
 * A namespace perspective that provides synchronous write operations. All operations block until
 * completion.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SynchronousWritableNamespacePerspective<K, V>
    extends BaseNamespacePerspective<K, V> {
  /**
   * Multi-value Put operation
   *
   * @param values map of key-value pairs to store
   * @param putOptions options for the Put operation
   * @throws PutException TODO (OPTIMUS-0000): describe
   */
  public void put(Map<? extends K, ? extends V> values, PutOptions putOptions) throws PutException;

  /**
   * Multi-value Put operation using default PutOptions.
   *
   * @param values map of key-value pairs to store
   * @throws PutException TODO (OPTIMUS-0000): describe
   */
  public default void put(Map<? extends K, ? extends V> values) throws PutException {
    put(values, getOptions().getDefaultPutOptions());
  }

  /**
   * Single-value Put operation.
   *
   * @param key key to associate the value with
   * @param value value to store
   * @param putOptions options for the Put operation
   * @throws PutException TODO (OPTIMUS-0000): describe
   */
  public void put(K key, V value, PutOptions putOptions) throws PutException;

  /**
   * Single-value Put operation using default PutOptions.
   *
   * @param key key to associate the value with
   * @param value value to store
   * @throws PutException TODO (OPTIMUS-0000): describe
   */
  public default void put(K key, V value) throws PutException {
    put(key, value, getOptions().getDefaultPutOptions());
  }

  /**
   * Multi-value Invalidation operation
   *
   * @param keys keys to invalidate
   * @param invalidationOptions options for the Put operation
   * @throws InvalidationException TODO (OPTIMUS-0000): describe
   */
  public void invalidate(Set<? extends K> keys, InvalidationOptions invalidationOptions)
      throws InvalidationException;

  /**
   * Multi-value Invalidation operation using default PutOptions.
   *
   * @param keys keys to invalidate
   * @throws InvalidationException TODO (OPTIMUS-0000): describe
   */
  public default void invalidate(Set<? extends K> keys) throws InvalidationException {
    invalidate(keys, getOptions().getDefaultInvalidationOptions());
  }

  /**
   * Single-value Invalidation operation.
   *
   * @param key key to invalidate
   * @param invalidationOptions options for the Invalidation operation
   * @throws InvalidationException TODO (OPTIMUS-0000): describe
   */
  public void invalidate(K key, InvalidationOptions invalidationOptions)
      throws InvalidationException;

  /**
   * Single-value Invalidation operation using default InvalidationOptions.
   *
   * @param key key to invalidate
   * @throws InvalidationException TODO (OPTIMUS-0000): describe
   */
  public default void invalidate(K key) throws InvalidationException {
    invalidate(key, getOptions().getDefaultInvalidationOptions());
  }

  /*
   * snapshots deprecated for now
  public void snapshot(long version) throws SnapshotException;
  public void snapshot() throws SnapshotException;
  */

  /*
  // temp dev only (probably)
  public void syncRequest(long version) throws SyncRequestException;
  public void syncRequest() throws SyncRequestException;
  */
}
