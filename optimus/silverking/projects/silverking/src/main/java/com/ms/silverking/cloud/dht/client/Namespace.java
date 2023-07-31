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

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;

/**
 * A namespace for storing values associated with keys. All associations in this namespace are distinct with
 * respect to associations in other namespaces. NamespaceOptions are used to specify how values are stored.
 * <p>
 * All values stored in a namespace are stored as sequences of bytes. NamespacePerspectives are
 * used to map types to bytes and vice versa as well as specifying the compression and checksum used.
 */
public interface Namespace {
  /**
   * Return the name of this namespace
   *
   * @return the name of this namespace
   */
  public String getName();

  /**
   * Get the options associated with this namespace
   *
   * @return the options associated with this namespace
   */
  public NamespaceOptions getOptions();

  /**
   * @return if trace feature is enabled
   */
  public boolean isServerTraceEnabled();

  /**
   * Get the default NamespacePerspectiveOptions for this namespace
   *
   * @param <K>        type of the keyClass
   * @param <V>        type of the valueClass
   * @param keyClass   class of keys
   * @param valueClass class of values
   * @return the default NamespacePerspectiveOptions for this namespace
   */
  public <K, V> NamespacePerspectiveOptions<K, V> getDefaultNSPOptions(Class<K> keyClass, Class<V> valueClass);

  /**
   * Open an AsynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param nspOptions options for the NamespacePerspective
   * @return the opened AsynchronousNamespacePerspective
   */
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(
      NamespacePerspectiveOptions<K, V> nspOptions);

  /**
   * Open an AsynchronousNamespacePerspective for the given key, value types.
   * Use default NamespacePerspectiveOptions for this Namespace
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param keyClass   class of keys
   * @param valueClass class of values
   * @return the opened AsynchronousNamespacePerspective
   */
  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(Class<K> keyClass, Class<V> valueClass);

  public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective();

  /**
   * Open an SynchronousNamespacePerspective for the given key, value types
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param nspOptions options for the NamespacePerspective
   * @return the opened SynchronousNamespacePerspective
   */
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(NamespacePerspectiveOptions<K, V> nspOptions);

  /**
   * Open an SynchronousNamespacePerspective for the given key, value types
   * Use default NamespacePerspectiveOptions for this Namespace
   *
   * @param <K>        key type of perspective to open
   * @param <V>        value type of perspective to open
   * @param keyClass   class of keys
   * @param valueClass class of values
   * @return the opened SynchronousNamespacePerspective
   */
  public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(Class<K> keyClass, Class<V> valueClass);

  public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective();

  /**
   * Create a clone of this namespace. For user-defined versioned namespaces,
   * the version of this method that accepts a version must be used.
   *
   * @param name name of the child namespace
   * @return the child namespace
   * @throws NamespaceCreationException TODO (OPTIMUS-0000): describe
   */
  public Namespace clone(String name) throws NamespaceCreationException;

  /**
   * Create a clone of this namespace. This version of this method is for use with
   * user-defined versioning only.
   *
   * @param name    name of the child namespace
   * @param version the version at which to create the clone. Must be greater than
   *                any version of any value currently stored in the namespace.
   * @return the child namespace
   * @throws NamespaceCreationException TODO (OPTIMUS-0000): describe
   */
  public Namespace clone(String name, long version) throws NamespaceCreationException;

  /**
   * <p><b>WARNING: This method is provided for backwards compatibility to SilverRails.
   * Use of this method is not recommended. clone() is recommended instead.</b></p>
   * <p>Link this namespace as a child of a provided target namespace. May only be used with write-once
   * versioned namespaces. This namespace must be set to allow links. The target namespace must already
   * exist and must have identical options to this namespace.</p>
   *
   * @param target the name of the target to link to
   * @throws NamespaceLinkException TODO (OPTIMUS-0000): describe
   */
  public void linkTo(String target) throws NamespaceLinkException;

  public Namespace modifyNamespace(NamespaceOptions nsOptions) throws NamespaceModificationException;

  /**
   * Name of the Replicas namespace. The Replicas namespace provides the locations of replicas given a key
   */
  public static final String replicasName = "__Replicas__";
  /**
   * Name of the Node namespace. The Node namespace provides information about the node answering the query
   * such as "bytesFree"
   */
  public static final String nodeName = "__Node__";
  /**
   * Name of the System namespace. The System namespace provides information about the overall SK instance.
   */
  public static final String systemName = "__System__";
  /**
   * Base name of NamespaceMetrics namespaces.
   */
  public static final String namespaceMetricsBaseName = "__NamespaceMetrics__";

  public static boolean isReservedNamespace(String ns) {
    return ns.startsWith(NamespaceUtil.reservedNamespacePrefix);
  }
}
