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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.util.Libraries;
import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps UUIDs from active messages back to active operations. This is necessary since we allow
 * multiple operations to be combined in a single message to improve performance.
 *
 * <p>The typing of the mapping is complex enough that we hide it in this class.
 *
 * <p>The strategy of this implementation is to allow GC and weak references to handle the removal
 * of entries from the map. This removes the need to perform any manual deletion.
 */
class ActiveRetrievalListeners implements ActiveOperationListeners {

  private static final boolean enableMultipleOpsPerMessage = OpSender.opGroupingEnabled;

  private final ConcurrentMap<
          UUIDBase, ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>
      activeOpListeners;

  private static Logger log = LoggerFactory.getLogger(ActiveRetrievalListeners.class);

  // UUIDBase-->
  //            DHTKey-->
  //                     List<ActiveKeyedOperationResultListener...>
  private final Map<
          UUIDBase,
          ConcurrentMap<
              DHTKey,
              List<
                  WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>>
      activeRetrievalListeners;

  private static final int mapCapacity = 8; // FUTURE - use something non-static
  private static final int keyMapConcurrencyLevel = 2;

  ActiveRetrievalListeners() {
    if (enableMultipleOpsPerMessage) {
      if (Libraries.useCustomGuava) {
        this.activeRetrievalListeners = new ConcurrentReferenceHashMap();
      } else {
        this.activeRetrievalListeners = Collections.synchronizedMap(new WeakHashMap());
      }
      this.activeOpListeners = null;
    } else {
      this.activeRetrievalListeners = null;
      this.activeOpListeners = new ConcurrentHashMap();
    }
  }

  OperationUUID newOpUUID() {
    OperationUUID opUUID = new OperationUUID();
    if (enableMultipleOpsPerMessage) {
      activeRetrievalListeners.put(
          opUUID, new ConcurrentHashMap(mapCapacity, keyMapConcurrencyLevel));
    }
    return opUUID;
  }

  /**
   * @param opUUID
   * @param dhtKey
   * @param listener
   * @return true if a new list was created for this key, false if a list already existed for this
   *     key
   */
  boolean addListener(
      UUIDBase opUUID,
      DHTKey dhtKey,
      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener) {
    if (enableMultipleOpsPerMessage) {
      List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>
          listenerList = new LinkedList();
      List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>
          existingList;
      existingList = activeRetrievalListeners.get(opUUID).putIfAbsent(dhtKey, listenerList);
      boolean newListCreated;
      if (existingList != null) {
        listenerList = existingList;
        newListCreated = false;
      } else {
        newListCreated = true;
      }
      listenerList.add(new WeakReference(listener));
      // caller should call protoMG.addKey(dhtKey) newListCreated is true
      return newListCreated;
    } else {
      Object prev = activeOpListeners.put(opUUID, listener);
      if (prev != null && prev != listener) {
        log.info("{}", prev);
        log.info("{}", listener);
        throw new RuntimeException(
            "Attempted to add multiple ops in a message, but enableMultipleOpsPerMessage is false");
      }
      return true;
    }
  }

  Set<AsyncRetrievalOperationImpl> currentRetrievalSet() {
    ImmutableSet.Builder<AsyncRetrievalOperationImpl> operationBuilder = ImmutableSet.builder();
    if (enableMultipleOpsPerMessage) {
      for (ConcurrentMap<
              DHTKey,
              List<
                  WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>
          map : activeRetrievalListeners.values()) {
        log.debug("maps: {}", map);
        // FUTURE - we can encounter concurrent modifications here
        // think about tolerating vs. preventing
        try {
          for (List<
                  WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>
              list : map.values()) {
            for (WeakReference<
                    ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>
                listenerRef : list) {
              ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener =
                  listenerRef.get();
              log.debug("listener: {}", listener);
              if (listener instanceof AsyncRetrievalOperationImpl) {
                operationBuilder.add((AsyncRetrievalOperationImpl) listener);
              }
            }
          }
        } catch (Exception exception) {
          // Currently, we tolerate concurrent modification-induced exceptions
          // Not critical if we skip a retry. In the future, however, we should probably prevent
          // this.
          if (log.isDebugEnabled()) {
            log.debug("", exception);
            log.debug("Ignoring exception during currentRetrievalSet()");
          }
        }
      }
    } else {
      for (ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener :
          activeOpListeners.values()) {
        if (listener instanceof AsyncRetrievalOperationImpl) {
          operationBuilder.add((AsyncRetrievalOperationImpl) listener);
        }
      }
    }
    return operationBuilder.build();
  }

  @Override
  public boolean isResponsibleFor(UUIDBase messageId) {
    return (activeRetrievalListeners != null && activeRetrievalListeners.containsKey(messageId))
        || (activeOpListeners != null && activeOpListeners.containsKey(messageId));
  }

  void receivedRetrievalResponse(MessageGroup message) {
    if (enableMultipleOpsPerMessage) {
      ConcurrentMap<
              DHTKey,
              List<
                  WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>
          listenerMap;
      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener;
      listenerMap = activeRetrievalListeners.get(message.getUUID());

      if (listenerMap != null) {
        List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>
            listenerList;

        if (message.getMessageType() == MessageType.ERROR_RESPONSE) {
          MessageGroupRetrievalResponseEntry errorResultAsRetrievalResult =
              message.getRetrievalResponseValueKeyIterator().iterator().next();
          for (DHTKey key : listenerMap.keySet()) {
            listenerList = listenerMap.get(key);
            if (listenerList != null) {
              for (WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>
                  listenerRef : listenerList) {
                listener = listenerRef.get();
                if (listener != null) {
                  listener.resultReceived(key, errorResultAsRetrievalResult);
                } else {
                  log.debug("receivedRetrievalResponse. null listenerRef.get() for entry: {}", key);
                }
              }
            }
          }
        } else {
          for (MessageGroupRetrievalResponseEntry entry :
              message.getRetrievalResponseValueKeyIterator()) {
            listenerList = listenerMap.get(entry);
            if (listenerList != null) {
              for (WeakReference<
                      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>
                  listenerRef : listenerList) {
                listener = listenerRef.get();
                if (listener != null) {
                  listener.resultReceived(entry, entry);
                } else {
                  log.debug(
                      "receivedRetrievalResponse. null listenerRef.get() for entry: {}", entry);
                }
              }
            } else {
              log.info("receivedRetrievalResponse. No listener for entry: {}", entry);
            }
          }
        }
      } else {
        // If we're receiving the error, then it's possible that we lost the
        // reference that was stored in the weak map.
        log.debug("receivedRetrievalResponse. No listenerMap for: {}", message.getUUID());
      }
    } else {

      ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener =
          activeOpListeners.get(message.getUUID());
      if (listener == null) {
        log.debug("receivedRetrievalResponse. No listener for uuid: {}", message.getUUID());
      } else {
        for (MessageGroupRetrievalResponseEntry entry :
            message.getRetrievalResponseValueKeyIterator()) {
          listener.resultReceived(entry, entry);
        }
      }
    }
  }
}
