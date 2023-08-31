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

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.AsyncKeyedOperation;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.collection.SingleDHTKeyToKeyMap;
import com.ms.silverking.cloud.dht.collection.SingleKeyToDHTKeyMap;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.util.concurrent.locks.SpinLock;

abstract class AsyncKeyedOperationImpl<K> extends AsyncNamespaceOperationImpl
    implements AsyncKeyedOperation<K> {
  private final KeyedNamespaceOperation<K> keyedNamespaceOperation;
  protected final int size;
  protected final Set<DHTKey> dhtKeys;
  protected final Map<DHTKey, K> dhtKeyToKey;
  protected final Map<K, DHTKey> keyToDHTKey;
  protected final BiMap<K, DHTKey> keyDHTKeyBiMap; // FUTURE - consider replacing the above two maps
  protected final KeyCreator<K> keyCreator;
  private final AtomicReference<Map<K, FailureCause>> failureCausesRef;
  protected final AtomicInteger resultsReceived;
  private final Lock completionCheckLock;

  protected int fragmentsCreated;

  protected static final boolean debugFragmentation = false;

  public AsyncKeyedOperationImpl(
      KeyedNamespaceOperation<K> operation,
      KeyCreator<K> keyCreator,
      ClientNamespace namespace,
      long curTime,
      byte[] originator) {
    super(operation, namespace.getContext(), curTime, originator);

    this.keyedNamespaceOperation = (KeyedNamespaceOperation<K>) operation;
    size = keyedNamespaceOperation.size();
    this.keyCreator = keyCreator;
    // dhtKeyToKey = new Object2ObjectOpenHashMap<>(size);
    // keyToDHTKey = new Object2ObjectOpenHashMap<>(size);
    // dhtKeyToKey = new HashMap<>(size);
    // keyToDHTKey = new HashMap<>(size);
    if (size != 1) {
      keyDHTKeyBiMap = HashBiMap.create(size);
      keyToDHTKey = keyDHTKeyBiMap;
      dhtKeyToKey = keyDHTKeyBiMap.inverse();
    } else {
      keyDHTKeyBiMap = null;
      keyToDHTKey = new SingleKeyToDHTKeyMap<>();
      dhtKeyToKey = new SingleDHTKeyToKeyMap<>();
    }
    dhtKeys = createKeys(keyCreator);
    failureCausesRef = new AtomicReference<>();
    resultsReceived = new AtomicInteger();
    completionCheckLock = new SpinLock();
    // completionCheckLock = new ReentrantLock();
  }

  protected Map<K, FailureCause> getFailureCauses() {
    Map<K, FailureCause> failureCauses;

    failureCauses = failureCausesRef.get();
    if (failureCauses == null) {
      failureCausesRef.compareAndSet(null, new HashMap<K, FailureCause>());
      failureCauses = failureCausesRef.get();
    }
    return failureCauses;
  }

  private Set<DHTKey> createKeys(KeyCreator<K> keyCreator) {
    ImmutableSet.Builder<DHTKey> keySetBuilder;

    keySetBuilder = ImmutableSet.builder();
    for (K key : keyedNamespaceOperation.getKeys()) {
      DHTKey dhtKey;

      dhtKey = keyCreator.createKey(key);
      keySetBuilder.add(dhtKey);
      dhtKeyToKey.put(dhtKey, key);
      keyToDHTKey.put(key, dhtKey);
    }
    return keySetBuilder.build();
  }

  @Override
  public int getNumKeys() {
    return size;
  }

  public Collection<DHTKey> getDHTKeys() {
    return dhtKeys;
  }

  @Override
  public Set<K> getKeys() {
    return keyedNamespaceOperation.getKeys();
  }

  protected int opWorkItems() {
    return size + fragmentsCreated;
  }

  @Override
  public Map<K, OperationState> getOperationStateMap() {
    Map<K, OperationState> stateMap;

    stateMap = new HashMap<>(getDHTKeys().size());
    for (K key : getKeys()) {
      stateMap.put(key, getOperationState(key));
    }
    return stateMap;
  }

  @Override
  public Map<K, OpResult> getOpResultMap() {
    Map<K, OpResult> stateMap;

    stateMap = new HashMap<>(getDHTKeys().size());
    for (K key : getKeys()) {
      stateMap.put(key, getOpResult(key));
    }
    return stateMap;
  }

  /**
   * Get the set of all incomplete keys in this operation.
   *
   * @return the set of keys in this operation
   */
  public Set<K> getIncompleteKeys() {
    Set<K> incompleteKeys;

    incompleteKeys = new HashSet<>();
    for (K key : getKeys()) {
      if (getOperationState(key) == OperationState.INCOMPLETE) {
        incompleteKeys.add(key);
      }
    }
    return incompleteKeys;
  }

  protected void setFailureCause(DHTKey dhtKey, FailureCause failureCause) {
    getFailureCauses().put(dhtKeyToKey.get(dhtKey), failureCause);
  }

  protected void failureCleanup(FailureCause failureCause) {
    for (K key : getKeys()) {
      OpResult keyResult;

      keyResult = getOpResult(key);
      if (keyResult == null || keyResult != OpResult.SUCCEEDED) {
        if (getFailureCauses().get(key) == null) {
          getFailureCauses().put(key, failureCause);
        }
      }
    }
    super.failureCleanup(failureCause);
  }

  protected void old_checkForCompletion() {
    // This method should only be called once in most cases,
    // so a lock here is OK. If it winds up being called multiple
    // times, then this approach is not appropriate.
    completionCheckLock.lock();
    try {
      // synchronized (this) {
      if (keyedNamespaceOperation.size() == 0) {
        setResult(OpResult.SUCCEEDED);
        return;
      } else {
        OpResult candidateResult;

        candidateResult = null;
        for (K key : keyedNamespaceOperation.getKeys()) {
          OpResult keyResult;

          keyResult = getOpResult(key);
          if (!keyResult.isComplete()) {
            // early exit if any result is not complete
            return;
          }
          if (candidateResult == null) {
            candidateResult = keyResult;
          } else {
            if (candidateResult != keyResult) {
              candidateResult = OpResult.MULTIPLE;
            }
          }
          if (isFailure(keyResult)) {
            getFailureCauses().put(key, keyResult.toFailureCause());
          }
        }
        // we only arrive here if all results are complete
        setResult(candidateResult);
      }
    } finally {
      completionCheckLock.unlock();
    }
  }

  protected void checkForCompletion() {
    // This method should only be called once in most cases,
    // so a lock here is OK. If it winds up being called multiple
    // times, then this approach is not appropriate.
    completionCheckLock.lock();
    try {
      // synchronized (this) {
      if (keyedNamespaceOperation.size() == 0) {
        setResult(OpResult.SUCCEEDED);
        return;
      } else {
        EnumSet<OpResult> candidateResults;

        candidateResults = null;
        for (K key : keyedNamespaceOperation.getKeys()) {
          OpResult keyResult = getOpResult(key);
          if (!keyResult.isComplete()) {
            // early exit if any result is not complete
            return;
          }
          if (candidateResults == null) {
            candidateResults = EnumSet.of(keyResult);
          } else {
            candidateResults.add(keyResult);
          }
          if (isFailure(keyResult)) {
            getFailureCauses().put(key, keyResult.toFailureCause(getNonExistenceResponse()));
          }
        }
        // we only arrive here if all results are complete
        if (candidateResults != null && candidateResults.size() > 0) {
          setResult(candidateResults);
        }
      }
    } finally {
      completionCheckLock.unlock();
    }
  }
}
