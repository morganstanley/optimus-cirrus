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

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;

/**
 * <p>A namespace perspective that provides synchronous read operations. All operations block until completion.</p>
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
public interface SynchronousReadableNamespacePerspective<K, V> extends BaseNamespacePerspective<K, V> {
  /**
   * Base retrieval operation. All other operations in this interface are convenience versions of this routine.
   *
   * @param keys             a set of keys to retrieve
   * @param retrievalOptions options for the retrieval
   * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, ? extends StoredValue<V>> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions)
      throws RetrievalException;

  /**
   * Retrieve values for multiple keys - with default options
   *
   * @param keys             a set of keys to retrieve
   * @param retrievalOptions options for the retrieval
   * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, ? extends StoredValue<V>> retrieve(Set<? extends K> keys) throws RetrievalException;

  /**
   * Single value retrieval operation.
   *
   * @param key              key to retrieve
   * @param retrievalOptions options for the retrieval
   * @return a StoredValue if a the key was found to have an associated value. null otherwise unless the retrieval
   * options are set to throw an exception for non-existence
   * @throws RetrievalException
   */
  public StoredValue<V> retrieve(K key, RetrievalOptions retrievalOptions) throws RetrievalException;

  /**
   * Single value retrieval operation - with default options.
   *
   * @param key key to retrieve
   * @return a StoredValue if a the key was found to have an associated value. null otherwise unless the retrieval
   * options are set to throw an exception for non-existence
   * @throws RetrievalException
   */
  default public StoredValue<V> retrieve(K key) throws RetrievalException {
    return retrieve(key, getOptions().getDefaultGetOptions());
  }

  // get - do not wait for key-value pairs to exist

  /**
   * Multiple-value Get operation.
   * TODO (OPTIMUS-43643) - RetrievalType is not respected
   *
   * @param keys       a set of keys to retrieve
   * @param getOptions options for the Get operation. (For this operation, only RetrievalTypes of VALUE
   *                   or VALUE_AND_META_DATA are allowed.)
   * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, V> get(Set<? extends K> keys, GetOptions getOptions) throws RetrievalException;

  /**
   * Multiple-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
   *
   * @param keys a set of keys to retrieve
   * @return a Map of keys to values for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, V> get(Set<? extends K> keys) throws RetrievalException;

  /**
   * A single-value Get operation.
   * TODO (OPTIMUS-43643) - RetrievalType is not respected
   *
   * @param key        key to retrieve
   * @param getOptions options for the Get operation.  (For this operation, only RetrievalTypes of VALUE
   *                   or VALUE_AND_META_DATA are allowed.)
   * @return the StoredValue associated with the key if it exists. null otherwise unless the retrieval options
   * are set to throw and exception for non-existence.
   * @throws RetrievalException
   */
  public V get(K key, GetOptions getOptions) throws RetrievalException;

  /**
   * Single-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
   *
   * @param key key to retrieve
   * @return value associated with the key if it exists. null otherwise unless the default GetOptions
   * are set to throw and exception for non-existence.
   * @throws RetrievalException
   */
  default public V get(K key) throws RetrievalException {
    return get(key, getOptions().getDefaultGetOptions());
  }

  // waitFor - wait on non-existent key-value pairs

  /**
   * Multi-value WaitFor operation.
   *
   * @param keys        a set of keys to retrieve
   * @param waitOptions options for the WaitFor operation
   * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, V> waitFor(Set<? extends K> keys, WaitOptions waitOptions) throws RetrievalException;

  /**
   * Multi-value WaitFor operation using default WaitFor operations. For retrieving values only; not StoredValues.
   *
   * @param keys a set of keys to retrieve
   * @return a Map of keys to values for all requested keys that were found to have values associated with them
   * @throws RetrievalException
   */
  public Map<K, V> waitFor(Set<? extends K> keys) throws RetrievalException;

  /**
   * Single-value WaitFor operation.
   *
   * @param key         key to retrieve
   * @param waitOptions options for the WaitFor operation
   * @return StoredValue associated with the key if it exists. null otherwise unless the default WaitOptions
   * are set to throw and exception for non-existence.
   * @throws RetrievalException
   */
  public V waitFor(K key, WaitOptions waitOptions) throws RetrievalException;

  /**
   * Single-value WaitFor operation using default WaitOptions. For retrieving values only; not StoredValues.
   *
   * @param key key to retrieve
   * @return value associated with the key if it exists. null otherwise unless the default WaitOptions
   * are set to throw and exception for non-existence.
   * @throws RetrievalException
   */
  default public V waitFor(K key) throws RetrievalException {
    return waitFor(key, getOptions().getDefaultWaitOptions());
  }
}
