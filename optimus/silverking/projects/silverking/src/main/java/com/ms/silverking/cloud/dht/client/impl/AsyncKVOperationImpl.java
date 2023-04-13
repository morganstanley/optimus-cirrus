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
package com.ms.silverking.cloud.dht.client.impl;

/**
 * AsyncOperationImpl that has keys and values. Stores the serDesGroup
 * used for the key and value types.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class AsyncKVOperationImpl<K, V> extends AsyncKeyedOperationImpl<K> {
  protected final NamespacePerspectiveOptionsImpl<K, V> nspoImpl;

  public AsyncKVOperationImpl(KeyedNamespaceOperation<K> operation, ClientNamespace namespace,
      NamespacePerspectiveOptionsImpl<K, V> nspoImpl, long curTime, byte[] originator) {
    super(operation, nspoImpl.getKeyCreator(), namespace, curTime, originator);
    this.nspoImpl = nspoImpl;
  }
}
