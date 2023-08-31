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
package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.CreateMode;

public class HostGroupTableZK extends MetaToolModuleBase<HostGroupTable, MetaPaths> {
  public HostGroupTableZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getHostGroupPath());
  }

  @Override
  public HostGroupTable readFromFile(File file, long version) throws IOException {
    return HostGroupTable.parse(file, version);
  }

  @Override
  public HostGroupTable readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String base;
    String vBase;

    base = getBase();
    if (version < 0) {
      version = zk.getLatestVersion(base);
    }
    vBase = getVBase(version);
    return HostGroupTable.parse(zk.getString(vBase), version);
  }

  @Override
  public void writeToFile(File file, HostGroupTable instance) throws IOException {
    throw new RuntimeException("writeToFile not implemented for HostGroupTable");
  }

  @Override
  public String writeToZK(HostGroupTable hostGroupTable, MetaToolOptions options)
      throws IOException, KeeperException {
    String path;

    path = zk.createString(base + "/", hostGroupTable.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return path;
  }
}
