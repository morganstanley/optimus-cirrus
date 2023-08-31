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
package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.StreamParser;
import org.apache.zookeeper.CreateMode;

public class IpAliasConfigurationZk extends MetaToolModuleBase<IpAliasConfiguration, MetaPaths> {

  public IpAliasConfigurationZk(MetaClient mc) throws KeeperException {
    super(mc, MetaPaths.getIpAliasesBase());
  }

  @Override
  public IpAliasConfiguration readFromFile(File file, long version) throws IOException {
    return IpAliasConfiguration.parse(StreamParser.parseLine(file), version);
  }

  private void verifyOptions(MetaToolOptions options) {
    if (options == null) {
      throw new RuntimeException("options == null");
    } else {
      if (options.name == null) {
        throw new RuntimeException("options.name == null");
      } else {
        if (options.name.trim().length() == 0) {
          throw new RuntimeException("options.name.trim().length() == 0");
        } else {
          if (options.name.trim().endsWith("/")) {
            throw new RuntimeException("options.name.trim().endsWith(\"/\")");
          }
        }
      }
    }
  }

  @Override
  public IpAliasConfiguration readFromZK(long version, MetaToolOptions options)
      throws KeeperException {
    String base;
    String vBase;

    verifyOptions(options);
    base = getBase() + "/" + options.name;
    if (version < 0) {
      version = mc.getZooKeeper().getLatestVersion(base);
    }
    vBase = getVBase(options.name, version);
    return IpAliasConfiguration.parse(zk.getString(vBase), version);
  }

  @Override
  public void writeToFile(File file, IpAliasConfiguration instance) throws IOException {
    FileUtil.writeToFile(file, instance.toString());
  }

  @Override
  public String writeToZK(IpAliasConfiguration instance, MetaToolOptions options)
      throws IOException, KeeperException {
    verifyOptions(options);
    zk.ensureCreated(base + "/" + options.name);
    return zk.createString(
        base + "/" + options.name + "/", instance.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
  }
}
