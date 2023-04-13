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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;

/**
 * A Put operation
 *
 * @param <K> key type
 * @param <V> value type
 */
class PutOperation<K, V> extends KeyedNamespaceOperation<K> {
  private final Map<? extends K, ? extends V> values;

  /**
   * options applied to all key/value pairs in this operation
   */

  public PutOperation(ClientNamespace namespace, Map<? extends K, ? extends V> values, PutOptions putOptions) {
    super(ClientOpType.PUT, namespace, values.keySet(), putOptions);
    // FUTURE - THINK ABOUT THIS. CONSIDER ALLOWING USERS TO GET RID OF
    // THIS COPY
    if (values instanceof HashMap) {
      this.values = new HashMap<>(values);
    } else {
      this.values = ImmutableMap.copyOf(values);
    }
  }

  public PutOptions putOptions() {
    return (PutOptions) options;
  }

  public int size() {
    return values.size();
  }

  public V getValue(K key) {
    return values.get(key);
  }

  public String debugString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Object key : values.keySet()) {
      sb.append(key);
      sb.append('\n');
    }
    return super.oidString() + "\t" + sb.toString();
  }

  @Override
  OpTimeoutController getTimeoutController() {
    return ((PutOptions) options).getOpTimeoutController();
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(':');
    sb.append(((PutOptions) options).toString());
    return sb.toString();
  }
}
