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

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.net.AddrAndPort;

/**
 * Represents a client's session with the DHT.
 * Namespaces and associated perspectives may be obtained from this session.
 *
 * @see BaseNamespacePerspective
 */
public interface DHTSession {
  /**
   * @return the server IP and port for this session
   */
  public AddrAndPort getServer();

  /**
   * Return the NamespaceCreationOptions specified for this DHT
   *
   * @return the NamespaceCreationOptions specified for this DHT
   */
  public NamespaceCreationOptions getNamespaceCreationOptions();

  /**
   * Return the default NamespaceOptions for this SK instance
   *
   * @return the default NamespaceOptions for this SK instance
   */
  public NamespaceOptions getDefaultNamespaceOptions();

  /**
   * Shortcut for getDefaultNamespaceOptions().getDefaultPutOptions()
   *
   * @return default PutOptions
   */
  public PutOptions getDefaultPutOptions();

  /**
   * Shortcut for getDefaultNamespaceOptions().getDefaultGetOptions()
   *
   * @return default GetOptions
   */
  public GetOptions getDefaultGetOptions();

  /**
   * Shortcut for getDefaultNamespaceOptions().getDefaultWaitOptions()
   *
   * @return default WaitOptions
   */
  public WaitOptions getDefaultWaitOptions();

  /**
   * @return if connected server has trace feature enabled
   */
  public boolean isServerTraceEnabled();

  /**
   * Create a namespace with default NamespaceOptions
   *
   * @param namespace name of the namespace
   * @return newly created namespace
   * @throws NamespaceCreationException if the namespace already exists
   */
  public Namespace createNamespace(String namespace) throws NamespaceCreationException;

  /**
   * Create a namespace with the specified options
   *
   * @param namespace name of the namespace
   * @param nsOptions NamespaceOptions used for creating this namespace.
   *                  If null, default this instance's NamespaceOptions will be used
   * @return newly created namespace
   * @throws NamespaceCreationException if the namespace already exists
   */
  public Namespace createNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceCreationException;

  /**
   * Get a previously created Namespace
   *
   * @param namespace name of the namespace
   * @return namespace that matches name
   */
  public Namespace getNamespace(String namespace);

  /**
   * Open an AsynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param namespace  name of the namespace
   * @param nspOptions options for NamespacePerspective
   * @return the opened AsynchronousNamespacePerspective
   */
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace,
      NamespacePerspectiveOptions<K, V> nspOptions);

  /**
   * Open an AsynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param namespace  name of the namespace
   * @param keyClass   class of keys
   * @param valueClass class of values
   * @return the opened AsynchronousNamespacePerspective
   */
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace,
      Class<K> keyClass, Class<V> valueClass);

  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace);

  /**
   * Open a SynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param namespace  name of the namespace
   * @param nspOptions options for NamespacePerspective
   * @return the opened SynchronousNamespacePerspective
   */
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace,
      NamespacePerspectiveOptions<K, V> nspOptions);

  /**
   * Open a SynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param namespace  name of the namespace
   * @param keyClass   class of keys
   * @param valueClass class of values
   * @return the opened SynchronousNamespacePerspective
   */
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace, Class<K> keyClass,
      Class<V> valueClass);

  public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace);

  /**
   * Deletes an entire namespace. This causes the data in the namespace to become inaccessible but not necessarily to
   * be deleted from disk.
   *
   * @param namespace name of the namespace
   * @throws NamespaceDeletionException TODO (OPTIMUS-0000): describe
   */
  public void deleteNamespace(String namespace) throws NamespaceDeletionException;

  /**
   * Attempts recovery of a deleted namespace. This is only possible if namespace data has not been deleted from disk.
   *
   * @param namespace name of the namespace
   * @throws NamespaceRecoverException TODO (OPTIMUS-0000): describe
   */
  public void recoverNamespace(String namespace) throws NamespaceRecoverException;

  /**
   * Close the session
   */
  public void close();
}
