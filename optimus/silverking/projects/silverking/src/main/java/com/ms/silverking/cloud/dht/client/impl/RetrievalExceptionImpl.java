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

import java.util.Map;

import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;

public class RetrievalExceptionImpl extends RetrievalException {
  public RetrievalExceptionImpl(Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause,
      Map<Object, StoredValue> partialResults) {
    super(operationState, failureCause, partialResults);
  }

  public RetrievalExceptionImpl(String message, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
    super(message, operationState, failureCause, partialResults);
  }

  public RetrievalExceptionImpl(String message, Throwable cause, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
    super(message, cause, operationState, failureCause, partialResults);
  }

  public RetrievalExceptionImpl(Throwable cause, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
    super(cause, operationState, failureCause, partialResults);
  }
}
