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
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.serverside.IntermediateResult;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retrieval operation that expects all primary replicas to be authoritative.
 * Secondary replicas are not authoritative.
 */
class SimpleRetrievalOperation extends BaseRetrievalOperation<RetrievalEntrySingleState> {

  private static Logger log = LoggerFactory.getLogger(SimpleRetrievalOperation.class);

  SimpleRetrievalOperation(long deadline, RetrievalOperationContainer retrievalOperationContainer,
      ForwardingMode forwardingMode) {
    super(deadline, retrievalOperationContainer, forwardingMode);
  }

  @Override
  protected RetrievalEntrySingleState initializeEntryState(DHTKey entryKey, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      System.out.printf("initializeEntryState %s %s %s\n", entryKey, CollectionUtil.toString(primaryReplicas),
          CollectionUtil.toString(secondaryReplicas));
    }
    entryState = new RetrievalEntrySingleState(primaryReplicas, secondaryReplicas);
    setEntryState(entryKey, entryState);
    return entryState;
  }
  
  protected void noPrimaryReplicasForKey(DHTKey key, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas, OpVirtualCommunicator<DHTKey, RetrievalResult> rvComm) {
    RetrievalEntrySingleState entryState;
    
    entryState = initializeEntryState(key, primaryReplicas, secondaryReplicas);
    entryState.updatePresentResult(OpResult.REPLICA_EXCLUDED);
    entryState.setComplete();
    synchronized (rvComm) {
      rvComm.sendResult(new RetrievalResult(key, OpResult.REPLICA_EXCLUDED, null));
    }
  }
  
  @Override
  public void update(DHTKey key, IPAndPort replica, RetrievalResult update, RetrievalVirtualCommunicator rvComm) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      log.info("SimpleRetrievalOperation.update(): {} {} {}", key, replica, update);
    }

    // Quick check the update
    if (replica == null) {
      log.debug("Ignoring update for null replica");
      return;
    }
    entryState = getEntryState(key);
    if (entryState == null) {
      log.debug("null entry state {} {} {}\n", key, replica, update);
      return;
    }
    if (!entryState.isReplica(replica)) {
      log.debug("Ignoring update for other replica {}", replica);
      return;
    }

    // Update is sane, process
    synchronized (entryState) {
      // FUTURE - need to handle potentially incomplete success
      // (generated when we get a value that we're not sure about whether a newer value is around)
      // leave out for now
      
      // process this update if it is a success from any replica, or if it is a failure from the current replica
      if (!update.getResult().hasFailed() || entryState.currentReplica().equals(replica)) {
        if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
          // Update is from this replica; forward the result
          synchronized (rvComm) {
            rvComm.sendResult(update);
          }
          entryState.setComplete();
        } else {
          boolean complete;
          
          // forwardingMode is either FORWARD or ALL
          // Update is from a remote replica
          switch (update.getResult()) {
          case CORRUPT: // fall through
          case REPLICA_EXCLUDED:
            entryState.updatePresentResult(update.getResult());
            complete = false;
            break;
          case NO_SUCH_VALUE:
            entryState.updatePresentResult(update.getResult());
            if (entryState.isPrimaryReplica(replica) && forwardingMode != ForwardingMode.ALL) {
              synchronized (rvComm) {
                rvComm.sendResult(update);
              }
              entryState.setComplete();
              complete = true;
            } else {            
              complete = false;
            }
            break;
          case SUCCEEDED:
            entryState.updatePresentResult(update.getResult());
            entryState.setComplete();
            if (entryState.isPrimaryReplica(replica)) {
              synchronized (rvComm) {
                rvComm.sendResult(update, entryState.getSecondaryReplicas());
              }
            } else {
              synchronized (rvComm) {
                rvComm.sendResult(update);
              }
            }
            complete = true;
            break;
          default:
            throw new RuntimeException("Unexpected update result: " + update.getResult());
          }       
          
          if (complete) {
            int _completeEntries;
            
            // remove from map when complete?
            _completeEntries = completeEntries.incrementAndGet();
            if (_completeEntries >= numEntries) {
              setOpResult(OpResult.SUCCEEDED); // triggers removal from MessageModule map
            }
          } else {
            // Move to next replica if any remain
            IPAndPort nextReplica;

            // The current replica has failed; move to the next replica
            nextReplica = entryState.nextReplica();
            if (debug) {
              System.out.printf("forward entry state %s %s\n", key, nextReplica);
            }
            
            if (nextReplica == null) {
              // All replicas exhausted; send the best result that we have
              // but leave operation as incomplete unless the following condition is met
              if (forwardingMode == ForwardingMode.ALL && entryState.getPresentResult() == OpResult.NO_SUCH_VALUE) {
                entryState.setComplete();
              }
              synchronized (rvComm) {
                // Exclusion changes are a general update from the node itself so do not relate to an in-flight traceId
                // However, affected operations may emit more information when they each send this result, if a Tracer is available
                log.warn("No remaining replicas for key {} - will send current result {}", KeyUtil.keyToString(key), entryState.getPresentResult());
                rvComm.sendResult(new RetrievalResult(key, entryState.getPresentResult(), null));
              }
            } else {
              synchronized (rvComm) {
                log.warn("Incomplete key {} will be forwarded to next replica {} due to result {}", KeyUtil.keyToString(key), replica, entryState.getPresentResult());
                rvComm.forwardEntry(nextReplica, key);
              }
            }
          }
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Ignoring update: {} {} {}", update.getResult().hasFailed(), entryState.currentReplica(), replica);
        }
      }
    }
  }
  
  public void replicaIncluded(IPAndPort replica, RetrievalCommunicator rComm) {
    if (forwardingMode != ForwardingMode.DO_NOT_FORWARD) {
      for (DHTKey key : opKeys()) {
        replicaIncluded(key, replica, rComm);
      }
    }
  }

  private void replicaIncluded(DHTKey key, IPAndPort replica, RetrievalCommunicator rComm) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      log.info("SimpleRetrievalOperation.replicaIncluded(): {} {}", key, replica);
    }
    
    if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
      throw new RuntimeException("Invalid replicaIncluded() invocation");
    }    

    entryState = getEntryState(key);
    if (entryState == null) {
      if (debug) {
        System.out.printf("null entry state %s %s\n", key, replica);
      }
      return;
    }

    synchronized (entryState) {
      if (entryState.isComplete() && entryState.getPresentResult() != OpResult.REPLICA_EXCLUDED) {
        return;
      } else {
        IPAndPort currentReplica;
        
        currentReplica = entryState.currentReplica();
        if (currentReplica != null && currentReplica.equals(replica)) {      
          if (log.isDebugEnabled()) {
            log.debug("forwarding: {} => {}", KeyUtil.keyToString(key), replica);
          }
          synchronized (rComm) {
            rComm.forwardEntry(replica, key);
          }
        }
      }
    }
  }
  
  public void replicaExcluded(IPAndPort replica, RetrievalCommunicator rComm) {
    if (log.isDebugEnabled()) {
      log.debug("SimpleRetrievalOperation.replicaExcluded {}", replica);
    }
    if (forwardingMode != ForwardingMode.DO_NOT_FORWARD) {
      for (DHTKey key : opKeys()) {
        replicaExcluded(key, replica, rComm);
      }
    }
  }
  
  private void replicaExcluded(DHTKey key, IPAndPort replica, RetrievalCommunicator rComm) {
    if (log.isDebugEnabled()) {
      log.debug("SimpleRetrievalOperation.replicaExcluded(): {} {}", key, replica);
    }
    
    if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
      throw new RuntimeException("Invalid replicaIncluded() invocation");
    }
    
    update(key, replica, new RetrievalResult(key, OpResult.REPLICA_EXCLUDED, null), rComm);
  }
}
