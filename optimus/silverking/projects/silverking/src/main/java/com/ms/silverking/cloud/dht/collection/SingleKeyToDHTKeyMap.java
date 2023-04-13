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

public class SingleKeyToDHTKeyMap<K> implements Map<K, DHTKey> {
  private K key;
  private DHTKey dhtKey;

  public SingleKeyToDHTKeyMap() {
  }

  @Override
  public int size() {
    return isEmpty() ? 0 : 1;
  }

  @Override
  public boolean isEmpty() {
    return key == null;
  }

  @Override
  public boolean containsKey(Object key) {
    return this.key != null && key.equals(this.key);
  }

  @Override
  public boolean containsValue(Object dhtKey) {
    return this.dhtKey != null && KeyUtil.equal(this.dhtKey, (DHTKey) dhtKey);
  }

  @Override
  public DHTKey get(Object dhtKey) {
    if (this.key != null && key.equals(this.key)) {
      return this.dhtKey;
    } else {
      return null;
    }
  }

  @Override
  public DHTKey put(K key, DHTKey dhtKey) {
    if (this.key == null || key.equals(this.key)) {
      DHTKey _dhtKey;

      _dhtKey = this.dhtKey;
      this.key = key;
      this.dhtKey = dhtKey;
      return _dhtKey;
    } else {
      throw new RuntimeException("This map can only hold one key-value pair " + this.key + " " + key);
    }
  }

  @Override
  public DHTKey remove(Object key) {
    if (this.key != null && key.equals(this.key)) {
      DHTKey _dhtKey;

      _dhtKey = this.dhtKey;
      this.key = null;
      this.dhtKey = null;
      return _dhtKey;
    } else {
      return null;
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends DHTKey> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    this.dhtKey = null;
    this.key = null;
  }

  @Override
  public Set<K> keySet() {
    return ImmutableSet.of(key);
  }

  @Override
  public Collection<DHTKey> values() {
    return ImmutableSet.of(dhtKey);
  }

  @Override
  public Set<Entry<K, DHTKey>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
