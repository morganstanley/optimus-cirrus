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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

abstract class FragmentedValue<R> implements ActiveKeyedOperationResultListener<R> {
  protected final DHTKey[] keys;
  protected final AtomicInteger resultsReceived;
  protected final Map<DHTKey, R> results;
  protected final DHTKey relayKey;
  protected ActiveKeyedOperationResultListener<R> parent;
  protected final int trackRelayKeyCompletion;

  private static Logger log = LoggerFactory.getLogger(FragmentedValue.class);

  FragmentedValue(
      DHTKey[] keys,
      DHTKey relayKey,
      ActiveKeyedOperationResultListener<R> parent,
      boolean trackRelayKeyCompletion) {
    this.keys = keys;
    this.parent = parent;
    resultsReceived = new AtomicInteger();
    this.relayKey = relayKey;
    this.trackRelayKeyCompletion = trackRelayKeyCompletion ? 1 : 0;
    results =
        new ConcurrentHashMap<>(
            keys.length + this.trackRelayKeyCompletion); // subkeys + the relay key
  }

  @Override
  public void resultReceived(DHTKey key, R result) {
    R prevResult;

    prevResult = results.put(key, result);
    if (prevResult != null) {
      // TODO (OPTIMUS-0000): to be completed
    } else {
      resultsReceived.incrementAndGet();
    }
    if (resultsReceived.get() >= keys.length + trackRelayKeyCompletion) { // all keys + relay key
      checkForCompletion();
    }
  }

  protected abstract void checkForCompletion();
}
