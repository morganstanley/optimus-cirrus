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
package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;

public interface PutTrigger extends Trigger {
  OpResult put(
      SSNamespaceStore nsStore,
      DHTKey key,
      ByteBuffer value,
      SSPutOptions options,
      SSStorageParametersAndRequirements storageParams,
      byte[] userData,
      NamespaceVersionMode nsVersionMode);

  OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState);

  /**
   * Merges and puts in one step. writeLock is held by caller
   *
   * @param values
   * @return results of the individual puts
   */
  Map<DHTKey, OpResult> mergePut(List<StorageValueAndParameters> values);

  boolean supportsMerge();
}
