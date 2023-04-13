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

import com.ms.silverking.cloud.dht.daemon.StorageReplicaProvider;
import com.ms.silverking.cloud.dht.ForwardingMode;

public class LooseConsistency implements StorageProtocol, RetrievalProtocol {
  private final StorageReplicaProvider storageReplicaProvider;

  public LooseConsistency(StorageReplicaProvider storageNodeProvider) {
    this.storageReplicaProvider = storageNodeProvider;
  }

  //@Override
  public StorageOperation createStorageOperation(long timeout, PutOperationContainer putOperationContainer,
      ForwardingMode forwardingMode) {
    return new LooseConsistencyWrite(putOperationContainer, forwardingMode, timeout);
  }

  @Override
  public RetrievalOperation createRetrievalOperation(long timeout,
      RetrievalOperationContainer retrievalOperationContainer, ForwardingMode forwardingMode) {
    return new LooseConsistencyRead(timeout, retrievalOperationContainer, forwardingMode);
  }

  @Override
  public boolean sendResultsDuringStart() {
    return true;
  }
}
