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
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.net.IPAndPort;

class SingleWriterConsistentStorageEntryState extends StorageEntryState {
  // Replicas expected to be small enough that List is more efficient than a map
  private final List<IPAndPort> primaryReplicas;
  // Secondary replicas don't participate in consistency protocol. Just receive updates.
  private final List<IPAndPort> secondaryReplicas;
  private final TwoPhaseStorageState[] replicaStates;
  private final AtomicInteger phaseIncomplete;
  private TwoPhaseStorageState state;
  // private final List<String>      debugList;

  enum StateTransitionResult {
    NO_TRANSITION,
    REPLICA_TRANSITION,
    QUORUM_TRANSITION,
    COMPLETION
  };

  private static final boolean debug = false;

  // FUTURE - potentially optimize this

  SingleWriterConsistentStorageEntryState(
      List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
    this.primaryReplicas = primaryReplicas;
    this.secondaryReplicas = secondaryReplicas;
    replicaStates = new TwoPhaseStorageState[primaryReplicas.size()];
    this.state = TwoPhaseStorageState.INITIAL;
    phaseIncomplete = new AtomicInteger(primaryReplicas.size());
    // debugList = new ArrayList<>();
  }

  List<IPAndPort> primaryReplicas() {
    return primaryReplicas;
  }

  List<IPAndPort> secondaryReplicas() {
    return secondaryReplicas;
  }

  StateTransitionResult transitionFromState(IPAndPort replica, TwoPhaseStorageState prevState) {
    TwoPhaseStorageState newState;

    newState = TwoPhaseStorageState.nextState(prevState);
    if (newState != prevState) {
      return updateState(replica, prevState, newState);
    } else {
      // Log.info("Disallowed update: "+ state +"\t"+ newState);
      return StateTransitionResult.NO_TRANSITION;
    }
  }

  void fail(IPAndPort replica, TwoPhaseStorageState prevState) {
    updateState(replica, prevState, TwoPhaseStorageState.FAILED);
  }

  private StateTransitionResult updateState(
      IPAndPort replica, TwoPhaseStorageState prevState, TwoPhaseStorageState newState) {
    int replicaIndex;

    replicaIndex = primaryReplicas.indexOf(replica);
    if (replicaIndex < 0) {
      // Must be a secondary replica
      return StateTransitionResult.NO_TRANSITION;
    }
    // FUTURE - think about using lock-free implementation
    // if we stick with locking, then the atomicinteger isn't useful
    synchronized (this) {
      TwoPhaseStorageState replicaState;

      replicaState = replicaStates[replicaIndex];
      if (replicaState == null) {
        replicaState = TwoPhaseStorageState.INITIAL;
      }
      // debugList.add(prevState +" "+ replicaState +" "+ newState +"\t"+ phaseIncomplete +" "+
      // replica +" "+ state);
      if (replicaState != prevState) {
        if (debug) {
          System.out.printf("replicaState %s != prevState %s\n", replicaState, prevState);
          // StringUtil.display(debugList, '\n');
        }
        return StateTransitionResult.NO_TRANSITION;
      } else {
        int incomplete;

        replicaStates[replicaIndex] = newState;
        incomplete = phaseIncomplete.decrementAndGet();
        if (debug) {
          System.out.printf("replicaState %s\n", replica);
          System.out.printf("replicaState %s prevState %s\n", replicaState, prevState);
          // StringUtil.display(debugList, '\n');
        }
        if (incomplete < 0) {
          throw new RuntimeException("Panic");
        } else {
          if (debug) {
            System.out.printf("incomplete %d state %s\n", incomplete, state);
          }
          if (incomplete == 0) {
            state = newState;
            if (!state.isComplete()) {
              phaseIncomplete.set(primaryReplicas.size());
              return StateTransitionResult.QUORUM_TRANSITION;
            } else {
              return StateTransitionResult.COMPLETION;
            }
          } else {
            return StateTransitionResult.REPLICA_TRANSITION;
          }
        }
      }
    }
  }

  @Override
  OpResult getCurOpResult() {
    // locked access not required here
    // FUTURE - not currently used, think about whether interface should have this
    // whether we should leave around, or whether we should throw an unsupported exception
    return state.toOpResult();
  }
}
