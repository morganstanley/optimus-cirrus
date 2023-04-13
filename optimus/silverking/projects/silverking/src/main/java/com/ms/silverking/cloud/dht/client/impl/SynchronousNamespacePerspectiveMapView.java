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
import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.common.OptionsHelper;

class SynchronousNamespacePerspectiveMapView<K, V> implements Map<K, V> {
  private SynchronousNamespacePerspective<K, V> snp;

  SynchronousNamespacePerspectiveMapView(SynchronousNamespacePerspective<K, V> snp) {
    this.snp = snp;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key) {
    try {
      StoredValue<V> storedValue;

      storedValue = snp.retrieve((K) key, OptionsHelper.newGetOptions(RetrievalType.EXISTENCE));
      return storedValue != null;
    } catch (RetrievalException re) {
      throw new RuntimeException(re);
    }
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object key) {
    try {
      StoredValue<V> storedValue;

      storedValue = (StoredValue<V>) snp.retrieve((K) key, OptionsHelper.newGetOptions(RetrievalType.VALUE));
      return storedValue == null ? null : storedValue.getValue();
    } catch (RetrievalException re) {
      throw new RuntimeException(re);
    }
  }

  @Override
  public V put(K key, V value) {
    try {
      snp.put(key, value);
      return null;
    } catch (PutException pe) {
      throw new RuntimeException(pe);
    }
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
