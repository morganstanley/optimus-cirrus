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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWLockMap<K, V> implements Map<K, V> {
  private final Map<K, V> map;
  private final ReadWriteLock rwLock;
  private final Lock readLock;
  private final Lock writeLock;

  public RWLockMap() {
    map = new HashMap();
    rwLock = new ReentrantReadWriteLock();
    readLock = rwLock.readLock();
    writeLock = rwLock.writeLock();
  }

  @Override
  public void clear() {
    writeLock.lock();
    try {
      map.clear();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    readLock.lock();
    try {
      return map.containsKey(key);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean containsValue(Object value) {
    readLock.lock();
    try {
      return map.containsValue(value);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    readLock.lock();
    try {
      return map.entrySet();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public V get(Object key) {
    readLock.lock();
    try {
      return map.get(key);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    readLock.lock();
    try {
      return map.isEmpty();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<K> keySet() {
    readLock.lock();
    try {
      return map.keySet();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public V put(K key, V value) {
    writeLock.lock();
    try {
      return map.put(key, value);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> _map) {
    writeLock.lock();
    try {
      map.putAll(_map);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public V remove(Object key) {
    writeLock.lock();
    try {
      return map.remove(key);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public int size() {
    readLock.lock();
    try {
      return map.size();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Collection<V> values() {
    readLock.lock();
    try {
      return map.values();
    } finally {
      readLock.unlock();
    }
  }
}
