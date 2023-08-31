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

import java.io.IOException;

import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.apache.zookeeper.CreateMode;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private CloudConfiguration cloudConfiguration;

  private MetaClient(MetaPaths mp, CloudConfiguration cloudConfiguration, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(mp, zkConfig);
    this.cloudConfiguration = cloudConfiguration;
  }

  public MetaClient(NamedRingConfiguration ringConfig, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    this(
        new MetaPaths(ringConfig),
        ringConfig.getRingConfiguration().getCloudConfiguration(),
        zkConfig);
  }

  public static MetaClient createMetaClient(
      String ringName, long ringVersion, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    MetaClient _mc;
    NamedRingConfiguration ringConfig;

    _mc =
        new MetaClient(
            new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate), zkConfig);
    ringConfig =
        new NamedRingConfiguration(
            ringName, new RingConfigurationZK(_mc).readFromZK(ringVersion, null));
    return new com.ms.silverking.cloud.toporing.meta.MetaClient(ringConfig, zkConfig);
  }

  public com.ms.silverking.cloud.meta.MetaClient createCloudMC()
      throws KeeperException, IOException {
    return new com.ms.silverking.cloud.meta.MetaClient(
        cloudConfiguration, getZooKeeper().getZKConfig());
  }

  public String createConfigInstancePath(long configVersion) throws KeeperException {
    String path;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    return getZooKeeper().createString(path + "/", "", CreateMode.PERSISTENT_SEQUENTIAL);
  }

  public String getLatestConfigInstancePath(long configVersion) throws KeeperException {
    String path;
    long latestVersion;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    latestVersion = getZooKeeper().getLatestVersion(path);
    if (latestVersion >= 0) {
      return path + "/" + SilverKingZooKeeperClient.padVersion(latestVersion);
    } else {
      return null;
    }
  }

  public long getLatestConfigInstanceVersion(long configVersion) throws KeeperException {
    String path;
    long latestVersion;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    latestVersion = getZooKeeper().getLatestVersion(path);
    return latestVersion;
  }
}
