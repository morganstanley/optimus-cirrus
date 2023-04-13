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

import com.ms.silverking.cloud.dht.OperationOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;

/**
 * Base operation class. An Operation is a static representation of the action
 * specified by the user and does not contain any dynamic state such as completion.
 */
abstract class Operation {
  private final OperationUUID opUUID;
  private final ClientOpType opType;
  protected final OperationOptions options;

  Operation(ClientOpType opType, OperationOptions options) {
    opUUID = new OperationUUID();
    this.opType = opType;
    this.options = options;
  }

  final ClientOpType getOpType() {
    return opType;
  }

  OperationUUID getUUID() {
    return opUUID;
  }

  protected String oidString() {
    return super.toString();
  }

  abstract OpTimeoutController getTimeoutController();

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(opType);
    sb.append(options);
    return sb.toString();
  }
}
