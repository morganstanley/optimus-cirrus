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
package com.ms.silverking.collection.test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class SimpleHashMap<K, V> implements Map<K, V> {
  private Entry<K, V>[] entries;
  private int size;

  private static final int defaultSize = 1024;

  static class Entry<K, V> {
    final int hash;
    final K key;
    V value;

    Entry(K key, V value) {
      this.hash = key.hashCode();
      this.key = key;
      this.value = value;
    }
  }

  public SimpleHashMap(int size) {
    this.size = size;
    entries = new Entry[size];
  }

  public SimpleHashMap() {
    this(defaultSize);
  }

  private Entry<K, V> findEntry(int hashCode, K key) {
    int index;
    int startIndex;
    Entry<K, V> entry;

    index = hashCode & (size - 1);
    //index = Math.abs(hashCode) % size;
    //index = (hashCode * hashCode) % size;
    startIndex = index;
    do {
      entry = entries[index];
      if (entry != null) {
        if (entry.hash == hashCode && entry.key.equals(key)) {
          return entry;
        } else {
          index = (index + 1) % size;
        }
      } else {
        return entry;
      }
    } while (index != startIndex);
    throw new RuntimeException("Map is full");
  }

  private int findIndex(int hashCode, K key) {
    int index;
    int startIndex;
    Entry<K, V> entry;

    index = hashCode & (size - 1);
    //index = Math.abs(hashCode) % size;
    //index = (hashCode * hashCode) % size;
    startIndex = index;
    do {
      entry = entries[index];
      if (entry != null) {
        if (entry.hash == hashCode && entry.key.equals(key)) {
          return index;
        } else {
          index++;
        }
      } else {
        return index;
      }
    } while (index != startIndex);
    throw new RuntimeException("Map is full");
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isEmpty() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public V get(Object key) {
    Entry<K, V> entry;

    entry = findEntry(key.hashCode(), (K) key);
    if (entry != null) {
      return entry.value;
    } else {
      return null;
    }
  }

  @Override
  public V put(K key, V value) {
    int index;
    Entry<K, V> entry;
    V oldValue;

    index = findIndex(key.hashCode(), key);
    entry = entries[index];
    if (entry == null) {
      entry = new Entry<>(key, value);
      entries[index] = entry;
      oldValue = null;
    } else {
      oldValue = entry.value;
      entry.value = value;
    }
    return oldValue;
  }

  @Override
  public V remove(Object key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    // TODO Auto-generated method stub

  }

  @Override
  public void clear() {
    // TODO Auto-generated method stub

  }

  @Override
  public Set<K> keySet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<V> values() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    // TODO Auto-generated method stub
    return null;
  }

  public static void main(String[] args) {
    try {
      SimpleHashMap<Integer, Integer> map;

      map = new SimpleHashMap<>();
      for (int i = 0; i < 10; i++) {
        map.put(i, i);
        System.out.println(map.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
