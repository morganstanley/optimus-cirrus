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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class HashedSetMap<K, V> implements Serializable {
  private final Map<K, Set<V>> map;

  private static final long serialVersionUID = 7891997492240035542L;

  public HashedSetMap() {
    map = new HashMap<>();
  }

  public HashedSetMap<K, V> clone() {
    HashedSetMap<K, V> map = new HashedSetMap<>();
    map.addAll(this);
    return map;
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public boolean containsValue(K key, V value) {
    Set<V> set = getSet(key);
    if (set == null) {
      return false;
    } else {
      return set.contains(value);
    }
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public int listSize(K key) {
    Set<V> list = getSet(key);
    if (list != null) {
      return list.size();
    } else {
      return 0;
    }
  }

  public void addValue(K key, V value) {
    Set<V> set = map.computeIfAbsent(key, k -> new HashSet<V>());
    set.add(value);
  }

  public void addValues(K key, Collection<V> values) {
    for (V value : values) {
      addValue(key, value);
    }
  }

  public boolean removeValue(K key, V value) {
    Set<V> list = map.get(key);
    if (list == null) {
      return false;
    } else {
      return list.remove(value);
    }
  }

  public void addAll(HashedSetMap<K, V> m) {
    for (K key : m.getKeys()) {
      addValues(key, m.getSet(key));
    }
  }

  public V getAnyValue(K key) {
    Set<V> set = map.get(key);
    if (set == null) {
      return null;
    } else {
      int rndIndex = ThreadLocalRandom.current().nextInt(set.size());
      int i = 0;
      for (V item : set) {
        if (i++ == rndIndex)
          return item;
      }
      throw new RuntimeException("panic");
    }
  }

  public Set<V> getSet(K key) {
    return map.get(key);
  }

  public List<Set<V>> getSets() {
    List<Set<V>> sets = new ArrayList<Set<V>>();
    for (Map.Entry<K, Set<V>> kSetEntry : map.entrySet()) {
      sets.add(kSetEntry.getValue());
    }
    return sets;
  }

  public List<K> getKeys() {List<K> keys = new ArrayList<K>();
    for (Map.Entry<K, Set<V>> kSetEntry : map.entrySet()) {
      keys.add(kSetEntry.getKey());
    }
    return keys;
  }

  public Set<V> removeSet(K key) {
    return map.remove(key);
  }

  public int getNumKeys() {
    return map.size();
  }
}
