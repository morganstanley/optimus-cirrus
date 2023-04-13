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

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.trace.TracerFactory;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common RetrievalOperation functionality.
 */
public abstract class BaseRetrievalOperation<S extends BaseRetrievalEntryState> extends BaseOperation<S>
    implements RetrievalOperation {
  protected final RetrievalOperationContainer retrievalOperationContainer;

  protected static Logger log = LoggerFactory.getLogger(BaseRetrievalOperation.class);

  public BaseRetrievalOperation(long deadline, RetrievalOperationContainer retrievalOperationContainer,
      ForwardingMode forwardingMode) {
    super(deadline, retrievalOperationContainer, forwardingMode, BaseRetrievalEntryState.minRelTimeoutMillis,
        retrievalOperationContainer.getNumEntries());
    this.retrievalOperationContainer = retrievalOperationContainer;
  }
  
  protected abstract void noPrimaryReplicasForKey(DHTKey key, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas, OpVirtualCommunicator<DHTKey, RetrievalResult> rvComm);

  /*
   * The below processInitialMessageGroupEntry is for a typical receive protocol
   * where we only need to receive from a single replica.
   *
   * Think about moving this into a subclass that protocols pull from if they implement this behavior.
   */
  @Override
  public void processInitialMessageGroupEntry(DHTKey key, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas, OpVirtualCommunicator<DHTKey, RetrievalResult> rvComm) {
    if (primaryReplicas.size() == 0) {
      noPrimaryReplicasForKey(key, primaryReplicas, secondaryReplicas, rvComm);
    } else {
      BaseRetrievalEntryState entryState;
  
      if (debug) {
        System.out.println("forwardingMode: " + forwardingMode);
        System.out.printf("p %s s %s\n", primaryReplicas, secondaryReplicas);
      }
  
      // Note that we do not handle local retrievals here - even though we could -
      // because we want to group all local retrievals so that we can
      // handle them en masse and incur only a single lock acquisition.
      // By "forwarding" them, they will be handled in bulk
  
      entryState = initializeEntryState(key, primaryReplicas, secondaryReplicas);
      if (forwardingMode.forwards()) {
        //initializeEntryState(entry, primaryReplicas, secondaryReplicas);
        if (retrievalOperationContainer.containsLocalReplica(
            primaryReplicas) || retrievalOperationContainer.containsLocalReplica(secondaryReplicas)) {
          if (debug) {
            System.out.println("local forward");
          }
          rvComm.forwardEntry(operationContainer.localIPAndPort(), key);
        } else {
          try {
            rvComm.forwardEntry(entryState.getInitialReplica(), key);
          } catch (NoSuchElementException nsee) {
            throw new RuntimeException("No replicas for: " + key);
          }
        }
      } else {
        rvComm.forwardEntry(operationContainer.localIPAndPort(), key);
              /*
              if (retrievalOperationContainer.containsLocalReplica(primaryReplicas)) {
                  rvComm.forwardEntry(operationContainer.localIPAndPort(), entry);
              } else {
                  Log.info("Unexpected non-local non-forwarded message");
                  // should be unreachable
              }
              */
      }
    }
  }

  protected abstract S initializeEntryState(DHTKey entryKey, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas);

  protected void tryNextReplica(DHTKey key, S entryState, RetrievalVirtualCommunicator rvComm) {
    IPAndPort nextReplica;

    nextReplica = entryState.nextReplica();
    if (nextReplica != null) {
      if (debug) {
        System.out.printf("forward entry state %s %s\n", key, nextReplica);
      }
      if (TracerFactory.isInitialized()) {
        TracerFactory.getTracer().onInternalRetry(key);
      }
      rvComm.forwardEntry(nextReplica, key);
    }
  }

  public Set<IPAndPort> checkForInternalTimeouts(long curTimeMillis, RetrievalVirtualCommunicator rvComm) {
    Set<IPAndPort> timedOutReplicas;

    timedOutReplicas = new HashSet<>();
    if (getMinInternalTimeoutMillis() < curTimeMillis) {
      // before checking entries, make sure that we're past the minimum timeout
      // this also has the effect of making it less difficult to catch the operation
      // while it's being created  FUTURE - make that impossible
      try {
        for (DHTKey key : opKeys()) {
          S entryState;

          entryState = getEntryState(key);
          if (entryState.hasTimedOut(curTimeMillis)) {
            IPAndPort replica;

            replica = entryState.currentReplica();
            if (replica != null && !entryState.prevReplicaSameAsCurrent()) {
              log.info("Non-fatal replica timedOut {} {}", replica, this);
              timedOutReplicas.add(replica);
            }
            tryNextReplica(key, entryState, rvComm);
          }
        }
      } catch (ConcurrentModificationException cme) {
        // FUTURE - This may happen during object creation. Eliminate this possibility in the future.
        // For now, simply ignore. Next check should work.
        log.info("Ignoring concurrent modification in BaseRetrievalOperation.checkForInternalTimeouts()");
      }
    }
    return timedOutReplicas;
  }
}
