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

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private final String dhtName;

  private static Logger log = LoggerFactory.getLogger(MetaClient.class);

  public MetaClient(String dhtName, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this(new NamedDHTConfiguration(dhtName, null), zkConfig);
  }

  public MetaClient(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(new MetaPaths(dhtConfig), zkConfig);
    this.dhtName = dhtConfig.getDHTName();
  }

  public MetaClient(ClientDHTConfiguration clientDHTConfig) throws IOException, KeeperException {
    this(clientDHTConfig.getName(), clientDHTConfig.getZKConfig());
  }

  public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    this(skGridConfig.getClientDHTConfiguration());
  }

  public String getDHTName() {
    return dhtName;
  }

  public DHTConfiguration getDHTConfiguration() throws KeeperException {
    String def;
    long version;
    String latestPath;
    long zkid;
    SilverKingZooKeeperClient zk;

    zk = getZooKeeper();
    latestPath = zk.getLatestVersionPath(getMetaPaths().getInstanceConfigPath());
    version = zk.getLatestVersionFromPath(latestPath);
    def = zk.getString(latestPath);
    zkid = zk.getStat(latestPath).getMzxid();
    return DHTConfiguration.parse(def, version, zkid);
  }

  public IpAliasConfiguration getIpAliasConfiguration(String ipAliasMapName) throws KeeperException {
    if (ipAliasMapName != null) {
      String fullPath = MetaPaths.getIpAliasMapPath(ipAliasMapName);
      log.debug("ipAliasMapName {}", ipAliasMapName);
      log.debug("fullPath {}", fullPath);
      SilverKingZooKeeperClient zk = getZooKeeper();
      if (zk.exists(MetaPaths.ipAliasesBase) && zk.exists(fullPath)) {
        String latestPath = zk.getLatestVersionPath(fullPath);
        long version = zk.getLatestVersionFromPath(latestPath);
        String def = zk.getString(latestPath);
        log.debug("def {}", def);
        return IpAliasConfiguration.parse(def, version);
      } else {
        log.debug("emptyTemplate 1");
        return IpAliasConfiguration.emptyTemplate;
      }
    } else {
      log.debug("emptyTemplate 2");
      return IpAliasConfiguration.emptyTemplate;
    }
  }
}
