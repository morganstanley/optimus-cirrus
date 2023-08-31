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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;

/**
 * Verify storage state. Possibly also verify integrity. This class is designed to be called out of
 * the critical path in the uncommon instance where storage state of a previous retrieval was found
 * to be bad.
 */
class ValidityVerifier {
  private final ByteBuffer buf;
  private final ConsistencyProtocol consistencyProtocol;

  ValidityVerifier(ByteBuffer buf, ConsistencyProtocol consistencyProtocol) {
    this.buf = buf;
    this.consistencyProtocol = consistencyProtocol;
  }

  boolean isValid(int offset) {
    return StorageProtocolUtil.storageStateValidForRead(
        consistencyProtocol, MetaDataUtil.getStorageState(buf, offset));
  }
}
