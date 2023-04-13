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

import com.ms.silverking.cloud.dht.AllReplicasExcludedResponse;
import com.ms.silverking.cloud.dht.OperationOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;

class VersionedBasicNamespaceOperation extends NamespaceOperation {
  private final long version;

  // FUTURE - get from ns etc. like put/get
  private static final OpTimeoutController opTimeoutController = new SimpleTimeoutController(5, 2 * 60 * 1000);

  VersionedBasicNamespaceOperation(ClientOpType opType, ClientNamespace namespace, long version) {
    super(opType, namespace, new OperationOptions(opTimeoutController, DHTConstants.noSecondaryTargets,
        DHTConstants.defaultTraceIDProvider, AllReplicasExcludedResponse.defaultResponse));
    this.version = version;
  }

  long getVersion() {
    return version;
  }

  @Override
  OpTimeoutController getTimeoutController() {
    return opTimeoutController;
  }
}
