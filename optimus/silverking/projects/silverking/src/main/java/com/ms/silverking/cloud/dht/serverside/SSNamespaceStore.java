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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.KeyToReplicaResolver;
import com.ms.silverking.cloud.dht.daemon.storage.SSInvalidationParametersImpl;

public interface SSNamespaceStore {
  public long getNamespaceHash();

  public boolean isNamespace(String ns);

  public File getNamespaceSSDir();

  public NamespaceOptions getNamespaceOptions();

  public OpResult put(DHTKey key, ByteBuffer value, SSStorageParametersAndRequirements storageParams, byte[] userData,
      NamespaceVersionMode nsVersionMode);

  public default OpResult invalidate(DHTKey key, SSInvalidationParametersImpl invalidateParams, byte[] userData,
      NamespaceVersionMode nsVersionMode) {
    return put(key, DHTConstants.emptyByteBuffer, invalidateParams, userData, nsVersionMode);
  }

  public OpResult putUpdate(DHTKey key, long version, byte storageState);

  public IntermediateResult retrieve(DHTKey key, SSRetrievalOptions options);

  public IntermediateResult[] retrieve(DHTKey[] keys, SSRetrievalOptions options);

  public ReadWriteLock getReadWriteLock();

  public KeyToReplicaResolver getKeyToReplicaResolver();

  public int getHeadSegmentNumber();
}
