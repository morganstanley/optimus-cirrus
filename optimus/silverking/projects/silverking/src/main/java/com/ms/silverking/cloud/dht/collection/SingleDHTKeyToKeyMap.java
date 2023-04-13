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
package com.ms.silverking.cloud.dht.collection;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;

public class SingleDHTKeyToKeyMap<K> implements Map<DHTKey, K> {
  private DHTKey dhtKey;
  private K key;

  public SingleDHTKeyToKeyMap() {
  }

  @Override
  public int size() {
    return isEmpty() ? 0 : 1;
  }

  @Override
  public boolean isEmpty() {
    return dhtKey == null;
  }

  @Override
  public boolean containsKey(Object dhtKey) {
    return this.dhtKey != null && KeyUtil.equal(this.dhtKey, (DHTKey) dhtKey);
  }

  @Override
  public boolean containsValue(Object key) {
    return this.key != null && key.equals(this.key);
  }

  @Override
  public K get(Object dhtKey) {
    if (this.dhtKey != null && KeyUtil.equal(this.dhtKey, (DHTKey) dhtKey)) {
      return this.key;
    } else {
      return null;
    }
  }

  @Override
  public K put(DHTKey dhtKey, K key) {
    if (this.dhtKey == null || KeyUtil.equal(this.dhtKey, (DHTKey) dhtKey)) {
      K _key;

      _key = this.key;
      this.dhtKey = dhtKey;
      this.key = key;
      return _key;
    } else {
      throw new RuntimeException(
          "This map can only hold one key-value pair " + KeyUtil.keyToString(this.dhtKey) + " " + KeyUtil.keyToString(
              dhtKey));
    }
  }

  @Override
  public K remove(Object key) {
    if (this.dhtKey != null && KeyUtil.equal((DHTKey) dhtKey, this.dhtKey)) {
      K _key;

      _key = this.key;
      this.dhtKey = null;
      this.key = null;
      return _key;
    } else {
      return null;
    }
  }

  @Override
  public void putAll(Map<? extends DHTKey, ? extends K> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    this.dhtKey = null;
    this.key = null;
  }

  @Override
  public Set<DHTKey> keySet() {
    return ImmutableSet.of(dhtKey);
  }

  @Override
  public Collection<K> values() {
    return ImmutableSet.of(key);
  }

  @Override
  public Set<Entry<DHTKey, K>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
