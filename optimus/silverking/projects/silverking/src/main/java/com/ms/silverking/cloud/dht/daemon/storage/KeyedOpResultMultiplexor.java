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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.collection.HashedListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyedOpResultMultiplexor implements KeyedOpResultListener {
  private final HashedListMap<DHTKey, KeyedOpResultListener> listeners;
  private static Logger log = LoggerFactory.getLogger(KeyedOpResultMultiplexor.class);

  KeyedOpResultMultiplexor() {
    listeners = new HashedListMap<>();
  }

  void addListener(DHTKey key, KeyedOpResultListener listener) {
    listeners.addValue(key, listener);
  }

  @Override
  public void sendResult(DHTKey key, OpResult result) {
    List<KeyedOpResultListener> keyListeners;

    keyListeners = listeners.getList(key);
    if (keyListeners != null) {
      for (KeyedOpResultListener keyListener : keyListeners) {
        keyListener.sendResult(key, result);
      }
    } else {
      log.info("KeyedOpResultMultiplexor. No listeners for {}", KeyUtil.keyToString(key));
    }
  }
}
