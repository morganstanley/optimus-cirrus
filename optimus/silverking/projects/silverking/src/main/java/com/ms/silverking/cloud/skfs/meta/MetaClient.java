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
package com.ms.silverking.cloud.skfs.meta;

import java.io.IOException;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private final String skfsConfigName;

  public MetaClient(String skfsConfigName, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(new MetaPaths(skfsConfigName), zkConfig);
    this.skfsConfigName = skfsConfigName;
  }

  public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    this(skGridConfig.getSKFSConfigName(), skGridConfig.getClientDHTConfiguration().getZKConfig());
  }

  public String getSKFSConfigName() {
    return skfsConfigName;
  }

  public String getSKFSConfig() throws KeeperException {
    String def;
    String latestPath;
    SilverKingZooKeeperClient zk;

    zk = getZooKeeper();
    latestPath = zk.getLatestVersionPath(getMetaPaths().getConfigPath());
    def = zk.getString(latestPath);
    return def;
  }
}
