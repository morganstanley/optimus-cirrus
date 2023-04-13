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
package com.ms.silverking.cloud.storagepolicy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.CreateMode;

public class StoragePolicyZK extends MetaToolModuleBase<StoragePolicyGroup, MetaPaths> {

  public StoragePolicyZK(MetaClientBase<MetaPaths> mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getStoragePolicyGroupPath());
  }

  @Override
  public StoragePolicyGroup readFromFile(File file, long version) throws IOException {
    return new PolicyParser().parsePolicyGroup(file, version);
  }

  @Override
  public StoragePolicyGroup readFromZK(long version, MetaToolOptions options) throws KeeperException {
    try {
      String vPath;

      vPath = getVersionPath(version);
      return new PolicyParser().parsePolicyGroup(zk.getString(vPath), version);
    } catch (PolicyParseException ppe) {
      throw new RuntimeException(ppe);
    }
  }

  @Override
  public void writeToFile(File file, StoragePolicyGroup policyGroup) throws IOException {
    BufferedWriter writer;

    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
    writer.write(policyGroup.getPolicy(policyGroup.getName()).toString());
    for (StoragePolicy policy : policyGroup.getPolicies()) {
      if (!policy.getName().equals(policyGroup.getName())) {
        writer.write(policy.toString());
      }
    }
    writer.close();
  }

  @Override
  public String writeToZK(StoragePolicyGroup policyGroup, MetaToolOptions options) throws IOException, KeeperException {
    zk.createString(base + "/", policyGroup.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return null;
  }
}
