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
package com.ms.silverking.cloud.dht.daemon.storage.convergence;

/** A single-replica's state w.r.t. a given ring */
public enum RingState {
  INITIAL,
  READY_FOR_CONVERGENCE_1,
  READY_FOR_CONVERGENCE_2,
  LOCAL_CONVERGENCE_COMPLETE_1,
  ALL_CONVERGENCE_COMPLETE_1,
  ALL_CONVERGENCE_COMPLETE_2,
  CLOSED,
  ABANDONED;

  public boolean isFinal() {
    return this == CLOSED || this == ABANDONED;
  }

  public boolean isValidTransition(RingState newRingState) {
    switch (this) {
      case INITIAL:
        return newRingState != INITIAL;
      case ABANDONED: // fall through
      case CLOSED:
        return false;
      default:
        return newRingState.ordinal() == this.ordinal() + 1 || newRingState == ABANDONED;
    }
  }

  public boolean requiresPassiveParticipation() {
    switch (this) {
      case CLOSED:
        return true;
      default:
        return false;
    }
  }

  public static RingState valueOf(byte[] b) {
    if (b != null && b.length > 0) {
      return valueOf(new String(b));
    } else {
      return null;
    }
    /*
    if (b != null && b.length == 1 && b[0] < values().length) {
        return values()[b[0]];
    } else {
        return null;
    }
    */
  }

  public boolean metBy(RingState nodeState) {
    if (nodeState == this) {
      return true;
    } else {
      switch (this) {
        case READY_FOR_CONVERGENCE_1:
          return nodeState == READY_FOR_CONVERGENCE_2
              || nodeState == LOCAL_CONVERGENCE_COMPLETE_1
              || nodeState == ALL_CONVERGENCE_COMPLETE_1
              || nodeState == ALL_CONVERGENCE_COMPLETE_2
              || nodeState == CLOSED;
        case READY_FOR_CONVERGENCE_2:
          return nodeState == LOCAL_CONVERGENCE_COMPLETE_1
              || nodeState == ALL_CONVERGENCE_COMPLETE_1
              || nodeState == ALL_CONVERGENCE_COMPLETE_2
              || nodeState == CLOSED;
        case LOCAL_CONVERGENCE_COMPLETE_1:
          return nodeState == ALL_CONVERGENCE_COMPLETE_1
              || nodeState == ALL_CONVERGENCE_COMPLETE_2
              || nodeState == CLOSED;
        case ALL_CONVERGENCE_COMPLETE_1:
          return nodeState == ALL_CONVERGENCE_COMPLETE_2 || nodeState == CLOSED;
        case ALL_CONVERGENCE_COMPLETE_2:
          return nodeState == CLOSED;
        default:
          return false;
      }
    }
  }
}
