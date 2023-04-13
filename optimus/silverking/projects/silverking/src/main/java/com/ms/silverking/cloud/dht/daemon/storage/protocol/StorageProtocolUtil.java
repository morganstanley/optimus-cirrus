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

import com.ms.silverking.cloud.dht.ConsistencyProtocol;

public class StorageProtocolUtil {
  // FUTURE - Make this protocol-specific
  public static final byte initialStorageStateOrdinal = 0;

  public static boolean storageStateValidForRead(ConsistencyProtocol consistencyProtocol, byte storageState) {
    switch (consistencyProtocol) {
    case LOOSE:
      return true;
    case TWO_PHASE_COMMIT:
      return TwoPhaseStorageState.values()[storageState].validForRead();
    default:
      throw new RuntimeException("panic");
    }
  }

  public static boolean requiresStorageStateVerification(ConsistencyProtocol cp) {
    switch (cp) {
    case LOOSE:
      return false;
    case TWO_PHASE_COMMIT:
      return true;
    default:
      throw new RuntimeException("panic");
    }
  }
}
