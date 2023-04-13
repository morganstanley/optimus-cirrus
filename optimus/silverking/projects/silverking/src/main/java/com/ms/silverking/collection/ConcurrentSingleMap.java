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
package com.ms.silverking.collection;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;

public class ConcurrentSingleMap<K, V> implements ConcurrentMap<K, V> {
  private final AtomicReference<KeyValuePair<K, V>> keyValuePair;

  private static class KeyValuePair<K, V> {
    final K key;
    final V value;

    KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }

  public ConcurrentSingleMap() {
    keyValuePair = new AtomicReference<>();
  }

  @Override
  public int size() {
    return isEmpty() ? 0 : 1;
  }

  @Override
  public boolean isEmpty() {
    return keyValuePair.get() == null;
  }

  @Override
  public boolean containsKey(Object key) {
    KeyValuePair<K, V> kvPair;

    kvPair = this.keyValuePair.get();
    return kvPair != null && kvPair.key.equals(key);
  }

  @Override
  public boolean containsValue(Object value) {
    KeyValuePair<K, V> kvPair;

    kvPair = this.keyValuePair.get();
    return kvPair != null && kvPair.value.equals(value);
  }

  @Override
  public V get(Object key) {
    KeyValuePair<K, V> kvPair;

    kvPair = this.keyValuePair.get();
    if (kvPair != null && kvPair.key.equals(key)) {
      return kvPair.value;
    } else {
      return null;
    }
  }

  @Override
  public V put(K key, V value) {
    if (this.keyValuePair.compareAndSet(null, new KeyValuePair<>(key, value))) {
      return null;
    } else {
      throw new RuntimeException("Changing the KeyValuePair is not allowed once set.");
    }
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    KeyValuePair<K, V> kvPair;

    kvPair = keyValuePair.get();
    if (kvPair != null) {
      return ImmutableSet.of(kvPair.key);
    } else {
      return ImmutableSet.of();
    }
  }

  @Override
  public Collection<V> values() {
    KeyValuePair<K, V> kvPair;

    kvPair = keyValuePair.get();
    if (kvPair != null) {
      return ImmutableSet.of(kvPair.value);
    } else {
      return ImmutableSet.of();
    }
  }

  private class EntryImpl implements Entry<K, V> {
    private final K key;
    private final V value;

    EntryImpl(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    KeyValuePair<K, V> kvPair;

    kvPair = keyValuePair.get();
    if (kvPair != null) {
      Entry<K, V> entry;

      entry = new EntryImpl(kvPair.key, kvPair.value);
      return ImmutableSet.of(entry);
    } else {
      return ImmutableSet.of();
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    if (this.keyValuePair.compareAndSet(null, new KeyValuePair<>(key, value))) {
      return null;
    } else {
      return keyValuePair.get().value;
    }
  }

  @Override
  public boolean remove(Object key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V replace(K key, V value) {
    throw new UnsupportedOperationException();
  }
}
