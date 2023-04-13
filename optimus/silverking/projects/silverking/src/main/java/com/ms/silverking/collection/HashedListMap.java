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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public final class HashedListMap<K, V> {
  private final HashMap<K, List<V>> map;
  private final boolean fillEmptyListOnGet;

  // FUTURE - Switch below to enum
  // Make decision about keeping this class vs. Guava equivalent first
  // and same decision for HashedSetMap. Classes should match more
  // if we keep them.

  public HashedListMap(boolean fillEmptyListOnGet) {
    map = new HashMap<K, List<V>>();
    this.fillEmptyListOnGet = fillEmptyListOnGet;
  }

  public HashedListMap() {
    this(false);
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Set<V> valueSet() {
    ImmutableSet.Builder<V> builder;

    builder = ImmutableSet.builder();
    for (K key : map.keySet()) {
      List<V> list;

      list = map.get(key);
      for (V element : list) {
        builder.add(element);
      }
    }
    return builder.build();
  }

  public boolean containsValue(K key, V value) {
    List<V> list;

    list = getList(key);
    if (list == null) {
      return false;
    } else {
      return list.contains(value);
    }
  }

  public int listSize(K key) {
    List<V> list;

    list = getList(key);
    if (list != null) {
      return list.size();
    } else {
      return 0;
    }
  }

  public void addValue(K key, V value) {
    List<V> list;

    list = map.get(key);
    if (list == null) {
      list = new ArrayList<V>();
      map.put(key, list);
    }
    list.add(value);
  }

  public void addValues(K key, Collection<V> values) {
    for (V value : values) {
      addValue(key, value);
    }
  }

  public boolean removeValue(K key, V value) {
    List<V> list;

    list = map.get(key);
    if (list == null) {
      return false;
    } else {
      return list.remove(value);
    }
  }

  public V getFirstValue(K key) {
    List<V> list;

    list = map.get(key);
    if (list == null) {
      return null;
    } else {
      if (list.size() > 0) {
        return list.get(0);
      } else {
        return null;
      }
    }
  }

  public V removeFirstValue(K key) {
    List<V> list;

    list = map.get(key);
    if (list == null) {
      return null;
    } else {
      if (list.size() > 0) {
        return list.remove(0);
      } else {
        return null;
      }
    }
  }

  public List<V> getList(K key) {
    List<V> list;

    list = map.get(key);
    if (fillEmptyListOnGet && list == null) {
      list = new ArrayList<V>();
      map.put(key, list);
    }
    return list;
  }

  public List<List<V>> getLists() {
    List<List<V>> lists;
    Iterator<Map.Entry<K, List<V>>> iterator;

    lists = new ArrayList<List<V>>();
    iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      lists.add(iterator.next().getValue());
    }
    return lists;
  }

  public List<K> getKeys() {
    List<K> keys;
    Iterator<Map.Entry<K, List<V>>> iterator;

    keys = new ArrayList<K>();
    iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      keys.add(iterator.next().getKey());
    }
    return keys;
  }

  public List<V> removeList(K key) {
    List<V> list;

    list = map.remove(key);
    return list;
  }

  public int totalSize() {
    Iterator<Map.Entry<K, List<V>>> iterator;
    int size;

    iterator = map.entrySet().iterator();
    size = 0;
    while (iterator.hasNext()) {
      size += iterator.next().getValue().size();
    }
    return size;
  }
}