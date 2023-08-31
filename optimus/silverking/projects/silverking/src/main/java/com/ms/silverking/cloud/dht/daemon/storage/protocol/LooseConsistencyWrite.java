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
package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Write operation for the LooseConsistency StorageProtocol. */
public class LooseConsistencyWrite
    extends BaseStorageOperation<SingleWriterLooseStorageEntryState> {
  private static final boolean debug = false;

  private static final int looseConsistencySuccessThreshold = 1;

  private static Logger log = LoggerFactory.getLogger(LooseConsistencyWrite.class);

  LooseConsistencyWrite(
      PutOperationContainer putOperationContainer, ForwardingMode forwardingMode, long deadline) {
    super(deadline, putOperationContainer, forwardingMode);
  }

  @Override
  public void initializeEntryState(
      DHTKey entryKey, List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
    setEntryState(entryKey, new SingleWriterLooseStorageEntryState(primaryReplicas));
  }

  @Override
  public void update(
      DHTKey key,
      IPAndPort replica,
      byte storageState,
      OpResult update,
      PutVirtualCommunicator pvComm) {
    // TODO (OPTIMUS-0000): reduce or eliminate locking here
    synchronized (this) {
      SingleWriterLooseStorageEntryState entryState;

      if (debug) {
        System.out.printf("replica %s\tupdate %s\n", replica, update);
      }
      entryState = getEntryState(key);
      if (debug) {
        System.out.printf("curOpResult %s\n", entryState.getCurOpResult());
      }
      if (entryState.getCurOpResult() == OpResult.INCOMPLETE) {
        entryState.setReplicaResult(replica, update);
        if (update.isComplete()) {
          OpResult looseResult;

          looseResult = entryState.getCurOpResult();
          if (debug) {
            System.out.printf("looseResult %s\n", looseResult);
          }
          if (looseResult.isComplete()) {
            int _completeEntries;

            pvComm.sendResult(key, looseResult);
            _completeEntries = completeEntries.incrementAndGet();
            if (_completeEntries >= numEntries) {
              setOpResult(OpResult.SUCCEEDED);
            }
          }
          if (debug) {
            System.out.printf(
                "completeEntries %d numEntries %d\n", completeEntries.get(), numEntries);
          }
        } else {
          log.info("Unexpected incomplete update: {}", update);
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Update for non-incomplete: {} {} {}", key, replica, update);
        }
      }
    }
  }

  public byte nextStorageState(byte prevStorageState) {
    return 0;
  }
}
