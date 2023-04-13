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

public final class IntHashMap<V> implements Map<Integer, V> {
  private Entry<V>[] entries;
  private int size;

  private static final int defaultSize = 1024;

  class Entry<T> {
    final int hash;
    final int key;
    final T value;

    Entry(int key, T value) {
      this.hash = key;
      this.key = key;
      this.value = value;
    }
  }

  public IntHashMap(int size) {
    this.size = size;
    entries = new Entry[size];
  }

  public IntHashMap() {
    this(defaultSize);
  }

  private int findIndex(int key) {
    return findIndex(key, key);
  }

  private int findIndex(int hashCode, int key) {
    int index;
    int startIndex;
    Entry<V> entry;

    index = hashCode & (size - 1);
    //index = Math.abs(hashCode) % size;
    //index = (hashCode * hashCode) % size;
    startIndex = index;
    do {
      entry = entries[index];
      if (entry != null) {
        if (entry.hash == hashCode && entry.key == key) {
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
  public V get(Object k) {
    int key;
    int index;
    Entry<V> entry;

    key = ((Integer) k).intValue();
    index = findIndex(key);
    entry = entries[index];
    if (entry != null) {
      return entry.value;
    } else {
      return null;
    }
  }

  @Override
  public V put(Integer key, V value) {
    int index;
    Entry<V> entry;
    V oldValue;

    index = findIndex(key);
    entry = entries[index];
    if (entry == null) {
      entry = new Entry<>(key, value);
      entries[index] = entry;
      oldValue = null;
    } else {
      oldValue = entry.value;
    }
    return oldValue;
  }

  @Override
  public V remove(Object key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends V> m) {
    // TODO Auto-generated method stub

  }

  @Override
  public void clear() {
    // TODO Auto-generated method stub

  }

  @Override
  public Set<Integer> keySet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<V> values() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<java.util.Map.Entry<Integer, V>> entrySet() {
    // TODO Auto-generated method stub
    return null;
  }

  public static void main(String[] args) {
    try {
      IntHashMap<Integer> map;

      map = new IntHashMap<>();
      for (int i = 0; i < 10; i++) {
        map.put(i, i);
        System.out.println(map.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
