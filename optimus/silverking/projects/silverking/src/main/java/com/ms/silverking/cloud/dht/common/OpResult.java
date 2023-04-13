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
package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationState;

/**
 * Internal representation of the result of an operation passed back by
 * methods and stored for ongoing operations.
 * <p>
 * The client API uses OperationState and FailureCause to obtain similar information.
 */
public enum OpResult {
  // incomplete
  INCOMPLETE, 
  // success
  SUCCEEDED,
  SUCCEEDED_AND_NO_SUCH_VALUE, // DEPRECATED - left for compatibility. Do not reuse this position in the enum
  // failure
  ERROR, TIMEOUT, MUTATION, NO_SUCH_VALUE, SIMULTANEOUS_PUT,
  MULTIPLE, // Multiple means multiple OpResults for keys in a batch (e.g. 1 Succeed, 1 Error). This is seen as a Failure for the whole batch
  INVALID_VERSION, NO_SUCH_NAMESPACE, CORRUPT,
  LOCKED, SESSION_CLOSED, REPLICA_EXCLUDED;

  public boolean isComplete() {
    return this != INCOMPLETE;
  }

  public boolean hasFailed(NonExistenceResponse nonExistenceResponse) {
    if (this == SUCCEEDED || this == INCOMPLETE) {
      return false;
    } else {
      if (this != NO_SUCH_VALUE) {
        return true;
      } else {
        return nonExistenceResponse == NonExistenceResponse.EXCEPTION;
      }
    }
  }

  public boolean hasFailed() {
    if (this == SUCCEEDED || this == INCOMPLETE) {
      return false;
    } else {
      if (this != NO_SUCH_VALUE) {
        return true;
      } else {
        // For a context where NO_SUCH_VALUE may exist, the NonExistenceResponse version must be used
        //throw new RuntimeException("Unexpected NO_SUCH_VALUE in hasFailed() ");
        // We allow this as setting the OpResult may occur after results are added, due to async access
        return false;
      }
    }
  }

  public OperationState toOperationState(NonExistenceResponse nonExistenceResponse) {
    switch (this) {
    case INCOMPLETE:
      return OperationState.INCOMPLETE;
    case SUCCEEDED:
      return OperationState.SUCCEEDED;
    case NO_SUCH_VALUE:
      return nonExistenceResponse == NonExistenceResponse.EXCEPTION ? OperationState.FAILED : OperationState.SUCCEEDED;
    default:
      return OperationState.FAILED;
    }
  }

  public OperationState toOperationState() {
    switch (this) {
    case INCOMPLETE:
      return OperationState.INCOMPLETE;
    case SUCCEEDED:
      return OperationState.SUCCEEDED;
    // For a context where NO_SUCH_VALUE may exist, the NonExistenceResponse version must be used
    case NO_SUCH_VALUE:
      throw new RuntimeException("Unexpected NO_SUCH_VALUE in toOperationState()");
    default:
      return OperationState.FAILED;
    }
  }

  public FailureCause toFailureCause(NonExistenceResponse nonExistenceResponse) {
    switch (this) {
    case NO_SUCH_VALUE:
      if (nonExistenceResponse == NonExistenceResponse.NULL_VALUE) {
        throw new RuntimeException(
            "toFailureCause() can't be called for " + this + " with NonExistenceResponse.NULL_VALUE");
      } else {
        return FailureCause.NO_SUCH_VALUE;
      }
    case INCOMPLETE: // fall through
    case SUCCEEDED:
      throw new RuntimeException("toFailureCause() can't be called for " + this);
    case CORRUPT:
      return FailureCause.CORRUPT;
    case ERROR:
      return FailureCause.ERROR;
    case TIMEOUT:
      return FailureCause.TIMEOUT;
    case MUTATION:
      return FailureCause.MUTATION;
    case LOCKED:
      return FailureCause.LOCKED;
    case SIMULTANEOUS_PUT:
      return FailureCause.SIMULTANEOUS_PUT;
    case MULTIPLE:
      return FailureCause.MULTIPLE;
    case INVALID_VERSION:
      return FailureCause.INVALID_VERSION;
    case NO_SUCH_NAMESPACE:
      return FailureCause.NO_SUCH_NAMESPACE;
    case SESSION_CLOSED:
      return FailureCause.SESSION_CLOSED;
    case REPLICA_EXCLUDED:
      // Note - we only return OpResult.REPLICA_EXCLUDED to the client
      // when all replicas have been excluded
      return FailureCause.ALL_REPLICAS_EXCLUDED;
    default:
      throw new RuntimeException("panic");
    }
  }

  public FailureCause toFailureCause() {
    return toFailureCause(null);
  }

  public static OpResult fromFailureCause(FailureCause failureCause) {
    switch (failureCause) {
    case CORRUPT:
      return CORRUPT;
    case ERROR:
      return ERROR;
    case TIMEOUT:
      return TIMEOUT;
    case MUTATION:
      return MUTATION;
    case LOCKED:
      return LOCKED;
    case SIMULTANEOUS_PUT:
      return SIMULTANEOUS_PUT;
    case MULTIPLE:
      return MULTIPLE;
    case INVALID_VERSION:
      return INVALID_VERSION;
    case SESSION_CLOSED:
      return SESSION_CLOSED;
    default:
      throw new RuntimeException("panic");
    }
  }

  public boolean supercedes(OpResult result) {
    return !isComplete() && result.isComplete();
  }

  public static boolean isIncompleteOrNull(OpResult opResult) {
    return opResult == null || opResult == INCOMPLETE;
  }
}
