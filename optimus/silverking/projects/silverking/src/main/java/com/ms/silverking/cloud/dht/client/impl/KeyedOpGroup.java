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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

class KeyedOpGroup {
  private final KeyedOpGroup parent;
  private final DHTKey parentKey;
  //private final Set<DHTKey>   keys;
  private AtomicInteger minIncomplete;
  private final ConcurrentMap<DHTKey, OpResult> results;

  KeyedOpGroup(KeyedOpGroup parent, DHTKey parentKey, Set<DHTKey> keys) {
    this.parent = parent;
    this.parentKey = parentKey;
    //this.keys = keys;
    minIncomplete = new AtomicInteger(keys.size());
    results = new ConcurrentHashMap<>(keys.size());
    for (DHTKey key : keys) {
      results.put(key, OpResult.INCOMPLETE);
    }
  }

  void update(DHTKey key, OpResult result) {
    OpResult oldResult;

    oldResult = results.put(key, result);
    if (oldResult == null) {
      throw new RuntimeException("Unexpected key for update");
    } else {
      if (oldResult.supercedes(result)) {
        // Note - this accounting is a little loose since
        // while a complete result has been removed,
        // it's possible for multiple final results to come in here,
        // so the minIncomplete counter may be too low.
        // In such a case, we wind up with an arbitrary one
        // of the multiple complete results.
        results.put(key, oldResult);
      } else {
        if (result.isComplete()) {
          if (minIncomplete.decrementAndGet() <= 0) {
            checkForCompletion();
          }
        }
      }
    }
  }

  /*
   * If this class is to used in the future, this
   * method must take the NonExistenceResonse into
   * consideration when calling hasFailed
   */
  private void checkForCompletion() {
    OpResult groupResult;

    groupResult = null;
    for (OpResult result : results.values()) {
      if (!result.isComplete()) {
        break;
      } else {
        if (groupResult == null) {
          groupResult = result;
        } else {
          if (groupResult.hasFailed()) {
            if (result.hasFailed()) {
              if (groupResult != result) {
                groupResult = OpResult.MULTIPLE;
              }
            } else {
              groupResult = OpResult.MULTIPLE;
            }
          } else {
            if (result.hasFailed()) {
              groupResult = OpResult.MULTIPLE;
            }
          }
        }
      }
    }
    if (groupResult.isComplete()) {
      parent.update(parentKey, groupResult);
    }
  }
}
