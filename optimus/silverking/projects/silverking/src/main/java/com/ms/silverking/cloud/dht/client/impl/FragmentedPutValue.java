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

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

class FragmentedPutValue extends FragmentedValue<OpResult> {
  FragmentedPutValue(DHTKey[] keys, DHTKey relayKey, ActiveKeyedOperationResultListener<OpResult> parent) {
    super(keys, relayKey, parent, true);
  }

  private OpResult getResult(DHTKey key) {
    OpResult result;

    result = results.get(key);
    return result == null ? OpResult.INCOMPLETE : result;
  }

  @Override
  protected void checkForCompletion() {
    OpResult result;

    result = OpResult.SUCCEEDED;
    for (DHTKey key : keys) {
      if (getResult(key) != OpResult.SUCCEEDED) {
        result = getResult(key);
      }
    }
    if (getResult(relayKey) != OpResult.SUCCEEDED) {
      result = getResult(relayKey);
    }
    parent.resultReceived(relayKey, result);
  }
}
