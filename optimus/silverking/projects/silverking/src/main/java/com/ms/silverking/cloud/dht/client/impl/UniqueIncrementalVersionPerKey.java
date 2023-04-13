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
import java.util.Set;

class UniqueIncrementalVersionPerKey<K> {
  private final long initialVersion;
  private final Map<K, Long> versionMap;

  UniqueIncrementalVersionPerKey(long initialVersion) {
    this.initialVersion = initialVersion;
    versionMap = new HashMap<>();
  }

  long getVersion(Set<K> keys) {
    synchronized (versionMap) {
      long maxVersion;
      long newVersion;

      maxVersion = Long.MIN_VALUE;
      for (K key : keys) {
        Long _keyVersion;
        long keyVersion;

        _keyVersion = versionMap.get(key);
        if (_keyVersion != null) {
          keyVersion = _keyVersion.longValue();
        } else {
          keyVersion = initialVersion - 1;
        }
        if (keyVersion > maxVersion) {
          maxVersion = keyVersion;
        }
      }
      newVersion = maxVersion + 1;
      for (K key : keys) {
        versionMap.put(key, newVersion);
      }
      return newVersion;
    }
  }
}
