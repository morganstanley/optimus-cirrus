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

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetrievalEntrySingleState extends BaseRetrievalEntryState {
  private final List<IPAndPort> primaryReplicas;
  private final List<IPAndPort> secondaryReplicas;
  private boolean complete;
  private OpResult presentResult;
  private short replicaIndex;
  private short prevReplicaIndex;
  // 0...(secondaryReplicas.size() - 1) ==> secondary
  // secondaryReplicas.size()...((secondaryReplicas.size() + (primaryReplicas.size() - 1))) ==>
  // primary

  private static final boolean verifyReplicas = false;

  private static Logger log = LoggerFactory.getLogger(RetrievalEntrySingleState.class);

  RetrievalEntrySingleState(List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
    if (verifyReplicas) {
      Preconditions.checkArgument(!primaryReplicas.contains(null));
      Preconditions.checkArgument(!secondaryReplicas.contains(null));
    }
    this.primaryReplicas = primaryReplicas;
    this.secondaryReplicas = secondaryReplicas;
    this.presentResult = OpResult.INCOMPLETE;
    replicaIndex = -1;
    prevReplicaIndex = -1;
  }

  public List<IPAndPort> getSecondaryReplicas() {
    return secondaryReplicas;
  }

  public ReplicationType replicaType(IPAndPort ipAndPort) {
    if (primaryReplicas.contains(ipAndPort)) {
      return ReplicationType.Primary;
    } else if (secondaryReplicas.contains(ipAndPort)) {
      return ReplicationType.Secondary;
    } else {
      return null;
    }
  }

  public boolean isPrimaryReplica(IPAndPort replica) {
    return primaryReplicas.contains(replica);
  }

  public boolean isReplica(IPAndPort replica) {
    if (currentReplica().equals(replica)) {
      // optimize the common case
      return true;
    } else {
      return primaryReplicas.contains(replica) || secondaryReplicas.contains(replica);
    }
  }

  private boolean currentReplicaIsSecondary() {
    return replicaIndex < secondaryReplicas.size();
  }

  @Override
  public IPAndPort getInitialReplica() {
    replicaIndex = 0;
    if (secondaryReplicas.size() > 0) {
      return secondaryReplicas.get(0);
    } else {
      return primaryReplicas.get(0);
    }
  }

  private IPAndPort getReplica(int index) {
    if (index < 0) {
      return null;
    } else {
      if (index < secondaryReplicas.size()) {
        return secondaryReplicas.get(index);
      } else {
        return primaryReplicas.get(index - secondaryReplicas.size());
      }
    }
  }

  @Override
  public boolean prevReplicaSameAsCurrent() {
    return prevReplicaIndex == replicaIndex;
  }

  @Override
  public IPAndPort currentReplica() {
    return getReplica(Math.max(replicaIndex, 0));
  }

  @Override
  public IPAndPort nextReplica() {
    // Mutual exclusion is guaranteed externally (synchronized block in SimpleRetrievalOperation)

    // Current logic is single try on a secondary replica and then move through all replicas
    // (FUTURE - consider moving the replica logic out of here)

    // Replicas with index 0...secondaryReplicas.size() - 1 are secondary replicas
    // Replicas with index
    // secondaryReplicas.size()...primaryReplicas.size() + secondaryReplicas.size() - 1 are primary
    // replicas

    prevReplicaIndex = replicaIndex;
    if (replicaIndex < 0) {
      computeNextReplicaTimeout();
      replicaIndex = 0;
      if (secondaryReplicas.size() > 0) {
        // Try one secondary replica if any exist
        return secondaryReplicas.get(0);
      } else {
        // No secondary replicas, just go to primary
        return primaryReplicas.get(0);
      }
    } else {
      if (currentReplicaIsSecondary()) {
        // Secondaries have been tried. Now move on to first primary replica
        replicaIndex = (short) secondaryReplicas.size();
      } else {
        if (replicaIndex >= secondaryReplicas.size() + primaryReplicas.size() - 1) {
          // All replicas tried already. Return null
          return null;
        } else {
          replicaIndex++;
        }
      }
      computeNextReplicaTimeout();
      return primaryReplicas.get(replicaIndex - secondaryReplicas.size());
    }
  }

  OpResult getPresentResult() {
    return presentResult;
  }

  void updatePresentResult(OpResult candidatePresentResult) {
    switch (candidatePresentResult) {
      case REPLICA_EXCLUDED:
        if (presentResult == OpResult.INCOMPLETE) {
          presentResult = OpResult.REPLICA_EXCLUDED;
        }
        break;
      case CORRUPT:
        if (presentResult == OpResult.INCOMPLETE
            || presentResult == OpResult.NO_SUCH_VALUE
            || presentResult == OpResult.REPLICA_EXCLUDED) {
          presentResult = OpResult.CORRUPT;
        }
        break;
      case NO_SUCH_VALUE:
        if (presentResult == OpResult.CORRUPT) {
          break;
        } else {
          // fall through to SUCCEEDED case
        }
      case SUCCEEDED:
        if (presentResult == OpResult.INCOMPLETE) {
          presentResult = candidatePresentResult;
        } else {
          if (presentResult != candidatePresentResult) {
            if (complete) {
              log.info(
                  "Ignoring multiple completion {} != {}", presentResult, candidatePresentResult);
            } else {
              presentResult = candidatePresentResult;
            }
          }
        }
        break;
      default:
        throw new RuntimeException("Unexpected candidatePresentResult: " + candidatePresentResult);
    }
    this.presentResult = candidatePresentResult;
  }

  public void setComplete() {
    this.complete = true;
  }

  /**
   * Return where or not this entry has a complete state. This is not an indication of whether or
   * not the proxy or client operation is complete.
   */
  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public String toString() {
    return primaryReplicas.toString()
        + ":"
        + secondaryReplicas.toString(); // + ":" + state.toString();
  }
}
