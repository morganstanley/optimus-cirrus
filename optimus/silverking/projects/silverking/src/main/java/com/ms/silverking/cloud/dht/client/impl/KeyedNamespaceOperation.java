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

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.OperationOptions;

/**
 * Operation on a namespace for a given set of keys.
 *
 * @param <K> key type
 */
abstract class KeyedNamespaceOperation<K> extends NamespaceOperation {
  private final Set<K> keys;

  KeyedNamespaceOperation(
      ClientOpType opType,
      ClientNamespace namespace,
      Collection<? extends K> keys,
      OperationOptions options) {
    super(opType, namespace, options);
    this.keys = ImmutableSet.copyOf(keys);
    // FUTURE - think about whether we can get rid of this copy
    // have a raw mode?
    // but how would we ensure the uniqueness? look for a set?
  }

  int size() {
    return keys.size();
  }

  final Set<K> getKeys() {
    return keys;
  }
}
