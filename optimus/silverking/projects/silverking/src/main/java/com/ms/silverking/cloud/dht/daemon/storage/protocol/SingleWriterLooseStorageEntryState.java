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

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SingleWriterLooseStorageEntryState extends StorageEntryState {
  private List<IPAndPort> replicas;
  private OpResult[] replicaResults;

  private static Logger log = LoggerFactory.getLogger(SingleWriterLooseStorageEntryState.class);

  SingleWriterLooseStorageEntryState(List<IPAndPort> replicas) {
    super();
    this.replicas = replicas;
    replicaResults = new OpResult[replicas.size()];
    for (int i = 0; i < replicas.size(); i++) {
      replicaResults[i] = OpResult.INCOMPLETE;
    }
  }

  @Override
  OpResult getCurOpResult() {
    OpResult result;
    int numFailed;

    numFailed = 0;
    result = OpResult.INCOMPLETE;
    for (int i = 0; i < replicas.size(); i++) {
      OpResult replicaResult;

      replicaResult = replicaResults[i];
      if (replicaResult.isComplete()) {
        if (replicaResult.hasFailed()) {
          ++numFailed;
          if (numFailed == 1) {
            result = replicaResult;
          } else {
            if (result != replicaResult) {
              result = OpResult.MULTIPLE;
            }
          }
        } else {
          return OpResult.SUCCEEDED; // LOOSE => any replica success is grounds for op success
        }
      }
    }
    // We only reach here if no replica has succeeded
    if (numFailed < replicas.size()) {
      // LOOSE => any replica that is incomplete may succeed, so we wait
      result = OpResult.INCOMPLETE;
    } else {
      // LOOSE => if all replicas have failed, then we return the results of the failure check
      // which is stored in result already
    }

    // FUTURE - LOOSE success on a single replica may cause memory issues under sustained writes
    // could consider constraining the number of outstanding loose writes

    return result;
  }

  void setReplicaResult(IPAndPort replica, OpResult result) {
    int index;

    index = replicas.indexOf(replica);
    if (!replicaResults[index].isComplete()) {
      replicaResults[index] = result;
    } else {
      log.info("Attempted update of complete: {} {} {}", replica, replicaResults[index], result);
    }
  }
}
