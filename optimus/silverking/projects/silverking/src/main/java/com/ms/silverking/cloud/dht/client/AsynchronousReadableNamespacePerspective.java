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

import java.util.Set;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;

/**
 * <p>A namespace perspective that provides asynchronous read operations. All operations return immediately,
 * and provide an object that can be used to query the state of the asynchronous operation.</p>
 *
 * <p>All methods provided here are variations of the base retrieve() method; the same functionality can
 * be accomplished using the base retrieve method.
 * </p>
 * <p>The behavior of a retrieval operation is specified by the RetrievalOptions which control:
 * <ul>
 * <li>RetrievalType - What to return: data, metadata, both data and metadata, existence only</li>
 * <li>VersionConstraint - Which value to return if the namespace is versioned</li>
 * <li>WaitMode - Behavior if a value cannot be found that matches the key and the VersionConstraint. Can
 * specify an immediate return, or a wait.</li>
 * <li>NonexistenceResponse - Behavior for when a value is not found: return null or throw exception.</li>
 * </ul>
 * See RetrievalOptions for additional options.
 * </p>
 *
 * <p>The get() methods below provide retrieval operations with a WaitMode of GET (return immediately if value not
 * found),
 * and a return type of V - the actual value (instead of the StoredValue provided by retrieve().</p>
 *
 * <p>The waitFor() methods provided below wait for a key-value pair to become present (i.e. WaitMode is WAIT_FOR).
 * They also return a type of V - the actual value (instead of the StoredValue provided by retrieve()).
 * If all requested keys have a value associated with them, get() and waitFor() methods will behave the same.</p>
 *
 * <p>Default RetrievalOptions, GetOptions, and WaitOptions are specified in the NamespacePerspectiveOptions.</p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsynchronousReadableNamespacePerspective<K, V> extends BaseNamespacePerspective<K, V> {
  /**
   * Base retrieval operation; Gets and WaitFors are just convenience versions of this routine.
   *
   * @param keys             a set of keys to retrieve
   * @param retrievalOptions options for the retrieval
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions);

  /**
   * Base retrieval operation - with default options
   *
   * @param keys a set of keys to retrieve
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys);

  /**
   * Single-value retrieval operation
   *
   * @param key              a key to retrieve
   * @param retrievalOptions options for the retrieval
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncSingleRetrieval<K, V> retrieve(K key, RetrievalOptions retrievalOptions);

  /**
   * Single-value retrieval operation - with default options
   *
   * @param key a key to retrieve
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncSingleRetrieval<K, V> retrieve(K key);

  // get - do not wait for key-value pairs to exist
  // multi-value

  /**
   * Multiple-value Get operation.
   *
   * @param keys       a set of keys to retrieve
   * @param getOptions options for the Get operation
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncValueRetrieval<K, V> get(Set<? extends K> keys, GetOptions getOptions);

  /**
   * Multiple-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
   *
   * @param keys a set of keys to retrieve
   * @return an AsyncValueRetrieval object representing the operation
   */
  default public AsyncValueRetrieval<K, V> get(Set<? extends K> keys) {
    return (AsyncValueRetrieval<K, V>) get(keys, getOptions().getDefaultGetOptions());
  }
  // single-value

  /**
   * A single-value Get operation.
   *
   * @param key        key to retrieve
   * @param getOptions options for the Get operation
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncSingleValueRetrieval<K, V> get(K key, GetOptions getOptions);

  /**
   * Single-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
   *
   * @param key a key to retrieve
   * @return an AsyncSingleValueRetrieval object representing the operation
   */
  default public AsyncSingleValueRetrieval<K, V> get(K key) {
    return (AsyncSingleValueRetrieval<K, V>) get(key, getOptions().getDefaultGetOptions());
  }

  // waitFor - wait on non-existent key-value pairs
  // multi-value

  /**
   * Multi-value WaitFor operation.
   *
   * @param keys        a set of keys to retrieve
   * @param waitOptions options for the WaitFor operation
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys, WaitOptions waitOptions);

  /**
   * Multi-value WaitFor operation using default WaitFor operations. For retrieving values only; not StoredValues.
   *
   * @param keys a set of keys to retrieve
   * @return an AsyncValueRetrieval object representing the operation
   */
  default public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys) {
    return (AsyncValueRetrieval<K, V>) waitFor(keys, getOptions().getDefaultWaitOptions());
  }
  // single-value

  /**
   * Single-value WaitFor operation.
   *
   * @param key         key to retrieve
   * @param waitOptions options for the WaitFor operation
   * @return an AsyncRetrieval object representing the operation
   */
  public AsyncSingleValueRetrieval<K, V> waitFor(K key, WaitOptions waitOptions);

  /**
   * Single-value WaitFor operation using default WaitOptions. For retrieving values only; not StoredValues.
   *
   * @param key key to retrieve
   * @return an AsyncSingleValueRetrieval object representing the operation
   */
  default public AsyncSingleValueRetrieval<K, V> waitFor(K key) {
    return waitFor(key, getOptions().getDefaultWaitOptions());
  }
}
