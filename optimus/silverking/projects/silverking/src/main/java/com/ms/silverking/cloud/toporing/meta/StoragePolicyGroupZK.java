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
package com.ms.silverking.cloud.toporing.meta;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.storagepolicy.PolicyParseException;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.io.FileUtil;
import org.apache.zookeeper.CreateMode;

public class StoragePolicyGroupZK extends MetaToolModuleBase<StoragePolicyGroup, MetaPaths> {
  public StoragePolicyGroupZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getStoragePolicyGroupPath());
  }

  @Override
  public StoragePolicyGroup readFromFile(File file, long version) throws IOException {
    return StoragePolicyGroup.parse(FileUtil.readFileAsString(file), version);
  }

  @Override
  public StoragePolicyGroup readFromZK(long version, MetaToolOptions options)
      throws KeeperException {
    try {
      return StoragePolicyGroup.parse(zk.getString(getVBase(version)), version);
    } catch (PolicyParseException ppe) {
      throw new RuntimeException("Unexpected exception", ppe);
    }
  }

  @Override
  public void writeToFile(File file, StoragePolicyGroup instance) throws IOException {
    throw new RuntimeException("writeToFile not implemented for StoragePolicyGroup");
  }

  @Override
  public String writeToZK(StoragePolicyGroup storagePolicy, MetaToolOptions options)
      throws IOException, KeeperException {
    String path;

    path = zk.createString(base + "/", storagePolicy.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return path;
  }
}
