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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.common.JVMUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ActiveClientOperationTable {
  private final ActivePutListeners activePutListeners;
  private final ActiveRetrievalListeners activeRetrievalListeners;
  private final ActiveVersionedBasicOperations activeVersionedBasicOperations;
  private final LWTPool lwtPool;
  private final Stopwatch finalizationStopwatch;

  private static Logger log = LoggerFactory.getLogger(ActiveClientOperationTable.class);

  private static final boolean debugTimeouts = false;
  private static boolean allowFinalization = false;

  private static final double finalizationLoadThreshold = 0.1;
  private static final double finalizationForceIntervalSeconds = 10.0;

  public ActiveClientOperationTable() {
    activePutListeners = new ActivePutListeners();
    activeRetrievalListeners = new ActiveRetrievalListeners();
    activeVersionedBasicOperations = new ActiveVersionedBasicOperations();
    lwtPool = LWTPoolProvider.defaultConcurrentWorkPool;
    finalizationStopwatch = new SimpleStopwatch();
  }

  public static void disableFinalization() {
    allowFinalization = false;
  }

  public ActivePutListeners getActivePutListeners() {
    return activePutListeners;
  }

  public ActiveRetrievalListeners getActiveRetrievalListeners() {
    return activeRetrievalListeners;
  }

  public ActiveVersionedBasicOperations getActiveVersionedBasicOperations() {
    return activeVersionedBasicOperations;
  }

  public List<AsyncOperationImpl> getActiveAsyncOperations() {
    List<AsyncOperationImpl> activeOps = new LinkedList();
    activeOps.addAll(activeRetrievalListeners.currentRetrievalSet());
    activeOps.addAll(activePutListeners.currentPutSet());
    return activeOps;
  }

  public void receivedChecksumTree(MessageGroup message) {
    ChecksumNode checksumNode = ProtoChecksumTreeMessageGroup.deserialize(message);
    log.info("{}", checksumNode);
    activeVersionedBasicOperations.receivedOpResponse(message);
  }

  public void checkForTimeouts(long curTimeMillis,
                               OpSender putSender,
                               OpSender retrievalSender,
                               boolean exclusionSetHasChanged) {
    // FUTURE - think about gc and finalization, for now avoid finalization since it incurs massive lag
    if (allowFinalization &&
        (lwtPool.getLoad().getLoad() < finalizationLoadThreshold ||
         finalizationStopwatch.getSplitSeconds() > finalizationForceIntervalSeconds)) {
      System.gc();
      JVMUtil.getGlobalFinalization().forceFinalization((int) (finalizationForceIntervalSeconds * 1000.0));
      finalizationStopwatch.reset();
    }

    Set<AsyncRetrievalOperationImpl> retrievalOperationSet = activeRetrievalListeners.currentRetrievalSet();
    // for retrievals, proxy side exclusion detection and replica fai-lover are used instead of client-side
    checkOpsForTimeouts(curTimeMillis, retrievalOperationSet, retrievalSender, false);
    Set<AsyncPutOperationImpl> putOperationSet = activePutListeners.currentPutSet();
    checkOpsForTimeouts(curTimeMillis, putOperationSet, putSender, exclusionSetHasChanged);
  }

  private void checkOpsForTimeouts(long curTimeMillis,
                                   Set<? extends AsyncOperationImpl> ops,
                                   OpSender opSender,
                                   boolean exclusionSetHasChanged) {
    if (debugTimeouts) {
      log.info("ActiveClientOperationTable.checkOpsForTimeouts {}", ops.size());
    }
    for (AsyncOperationImpl op : ops) {
      checkOpForTimeouts(curTimeMillis, op, opSender, exclusionSetHasChanged);
    }
  }

  private void checkOpForTimeouts(long curTimeMillis,
                                  AsyncOperationImpl operation,
                                  OpSender opSender,
                                  boolean exclusionSetHasChanged) {
    boolean attemptHasTimedOut = operation.attemptHasTimedOut(curTimeMillis);

    // TODO (OPTIMUS-0000): rather than retrying on any exclusion, we ought to retry only if the
    //  replica to which we are communicating for a given op has been excluded
    if (attemptHasTimedOut || (exclusionSetHasChanged && operation.retryOnExclusionChange(curTimeMillis))) {
      if (operation.getState() == OperationState.INCOMPLETE) {
        if (debugTimeouts) {
          if (attemptHasTimedOut) {
            log.debug("Attempt timed out: {}", operation);
          } else {
            log.debug("Retry due to exclusion set change: {}", operation);
          }
        }
        if (!operation.opHasTimedOut(curTimeMillis)) {
          if (debugTimeouts) {
            log.debug("Resending: {}", operation);
          } else {
            log.debug("Resending: {}", operation);
          }

          if (attemptHasTimedOut) {
            // only bump up attempts if the attempt has timed out
            // not if the exclusion set has changed
            operation.newAttempt(curTimeMillis);
          }
          if (operation.newAttemptAllowed()) {
            opSender.addWork(operation);
          }

        } else {
          if (debugTimeouts) {
            log.debug("Op timed out: {}", operation);
          }
          log.debug("TIMEOUT: {}", operation);
          operation.setResult(OpResult.TIMEOUT);
        }
      }
    }
  }
}
