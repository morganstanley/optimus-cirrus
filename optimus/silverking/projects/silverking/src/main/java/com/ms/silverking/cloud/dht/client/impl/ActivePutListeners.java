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

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.util.Libraries;
import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps UUIDs from active messages back to active operations. The typing of the mapping is complex
 * enough that we hide it in this class.
 *
 * <p>The strategy of this implementation is to allow GC and weak references to handle the removal
 * of entries from the map. This removes the need to perform any manual deletion.
 */
class ActivePutListeners implements ActiveOperationListeners {

  private static final boolean enableMultipleOpsPerMessage = OpSender.opGroupingEnabled;
  private final ConcurrentMap<UUIDBase, ActiveKeyedOperationResultListener<OpResult>>
      activeOpListeners;
  private final Map<
          UUIDBase,
          ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>>>
      activePutListeners;

  private static Logger log = LoggerFactory.getLogger(ActivePutListeners.class);

  ActivePutListeners() {
    if (enableMultipleOpsPerMessage) {
      if (Libraries.useCustomGuava) {
        activePutListeners = new ConcurrentReferenceHashMap();
      } else {
        activePutListeners = Collections.synchronizedMap(new WeakHashMap());
      }
      this.activeOpListeners = null;
    } else {
      this.activePutListeners = null;
      this.activeOpListeners = new ConcurrentHashMap();
    }
  }

  OperationUUID newOpUUIDAndMap() {
    OperationUUID opUUID = new OperationUUID();
    if (enableMultipleOpsPerMessage) {
      activePutListeners.put(opUUID, new ConcurrentHashMap());
    }
    return opUUID;
  }

  /**
   * @param opUUID
   * @param dhtKey
   * @param listener
   * @return true if listener was added successfully, false otherwise
   * @throws NullPointerException if opUUID does not exist in activePutListeners map
   * @throws RuntimeException if enableMultipleOpsPerMessage is false, but you try to add multiple
   *     operations
   */
  boolean addListener(
      UUIDBase opUUID, DHTKey dhtKey, ActiveKeyedOperationResultListener<OpResult> listener)
      throws RuntimeException {
    if (enableMultipleOpsPerMessage) {
      return activePutListeners.get(opUUID).putIfAbsent(dhtKey, new WeakReference(listener))
          == null;
    } else {
      Object prev = activeOpListeners.putIfAbsent(opUUID, listener);
      if (prev != null && prev != listener) {
        log.info("{}", prev);
        log.info("{}", listener);
        throw new RuntimeException(
            "Attempted to add multiple ops in a message, but enableMultipleOpsPerMessage is false");
      }
      return true;
    }
  }

  Set<AsyncPutOperationImpl> currentPutSet() {
    log.debug("currentPutSet()");
    ImmutableSet.Builder<AsyncPutOperationImpl> ops = ImmutableSet.builder();
    if (enableMultipleOpsPerMessage) {
      for (ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>> map :
          activePutListeners.values()) {
        log.debug("maps: {}", map);
        for (WeakReference<ActiveKeyedOperationResultListener<OpResult>> listenerRef :
            map.values()) {
          ActiveKeyedOperationResultListener<OpResult> listener = listenerRef.get();
          log.debug("listener: {}", listener);
          if (listener instanceof AsyncPutOperationImpl) {
            ops.add((AsyncPutOperationImpl) listener);
          }
        }
      }
    } else {
      for (ActiveKeyedOperationResultListener<OpResult> listener : activeOpListeners.values()) {
        if (listener instanceof AsyncPutOperationImpl) {
          ops.add((AsyncPutOperationImpl) listener);
        }
      }
    }
    return ops.build();
  }

  @Override
  public boolean isResponsibleFor(UUIDBase messageId) {
    return (activePutListeners != null && activePutListeners.containsKey(messageId))
        || (activeOpListeners != null && activeOpListeners.containsKey(messageId));
  }

  public void receivedPutResponse(MessageGroup message) {
    if (enableMultipleOpsPerMessage) {
      ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>>
          listenerMap;
      listenerMap = activePutListeners.get(message.getUUID());
      if (listenerMap != null) {
        if (message.getMessageType() == MessageType.ERROR_RESPONSE) {
          for (DHTKey key : listenerMap.keySet()) {
            ActiveKeyedOperationResultListener<OpResult> listener = listenerMap.get(key).get();
            if (listener != null) {
              listener.resultReceived(key, OpResult.ERROR);
            } else {
              log.debug("receivedRetrievalResponse. null listenerRef.get() for entry: {}", key);
            }
          }

        } else {
          for (MessageGroupKeyOrdinalEntry entry : message.getKeyOrdinalIterator()) {
            WeakReference<ActiveKeyedOperationResultListener<OpResult>> listenerRef =
                listenerMap.get(entry.getKey());
            ActiveKeyedOperationResultListener<OpResult> listener = listenerRef.get();
            if (listener != null) {
              listener.resultReceived(entry.getKey(), EnumValues.opResult[entry.getOrdinal()]);
            } else {
              log.debug(
                  "receivedPutResponse. null listener ref for: {}   {}",
                  message.getUUID(),
                  entry.getKey());
            }
          }
        }

      } else {
        // If we're receiving the error, then it's possible that we lost the
        // reference that was stored in the weak map.
        log.info("receivedPutResponse. No listenerMap for: {}", message.getUUID());
      }

    } else {
      ActiveKeyedOperationResultListener<OpResult> listener =
          activeOpListeners.get(message.getUUID());
      if (listener == null) {
        log.debug("receivedRetrievalResponse. No listener for uuid: {}", message.getUUID());
      } else {
        for (MessageGroupKeyOrdinalEntry entry : message.getKeyOrdinalIterator()) {
          listener.resultReceived(entry.getKey(), EnumValues.opResult[entry.getOrdinal()]);
        }
      }
    }
  }

  public ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>>
      getKeyMap(OperationUUID opUUID) {
    if (enableMultipleOpsPerMessage) {
      return activePutListeners.get(opUUID);
    } else {
      return new ConcurrentHashMap();
    }
  }
}
