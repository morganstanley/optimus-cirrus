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

import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.net.IPAndPort;
import org.apache.zookeeper.KeeperException.NoNodeException;

/** Write/Reads state for a particular instance of a particular ring for a particular DHT. */
public class RingStateZK {
  private final MetaClient mc;
  private final DHTConfiguration dhtConfig;
  private final long ringConfigVersion;
  private final long configInstanceVersion;

  /*
   * Path format:
   * <DHTPath>/ringState/<RingName>/<RingConfigVersion>/<ConfigInstanceVersion>/<NodeID>
   */

  public RingStateZK(
      MetaClient mc, DHTConfiguration dhtConfig, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException {
    this.mc = mc;
    this.dhtConfig = dhtConfig;
    this.ringConfigVersion = ringConfigVersion;
    this.configInstanceVersion = configInstanceVersion;
    ensureBasePathExists();
  }

  public RingStateZK(MetaClient mc, DHTConfiguration dhtConfig, Pair<Long, Long> ringVersionPair)
      throws KeeperException {
    this(mc, dhtConfig, ringVersionPair.getV1(), ringVersionPair.getV2());
  }

  private void ensureBasePathExists() throws KeeperException {
    mc.ensureMetaPathsExist();
    mc.ensurePathExists(getRingStatePathBase(), true);
    mc.ensurePathExists(getRingConfigVersionPath(), true);
    mc.ensurePathExists(getRingInstanceStatePath(), true);
  }

  private String getDHTRingStatePath() {
    return mc.getMetaPaths().getInstanceRingStatePath();
  }

  private String getRingStatePathBase() {
    return getDHTRingStatePath() + "/" + dhtConfig.getRingName();
  }

  private String getRingConfigVersionPath() {
    return getRingStatePathBase() + "/" + SilverKingZooKeeperClient.padVersion(ringConfigVersion);
  }

  public String getRingInstanceStatePath() {
    return getRingConfigVersionPath()
        + "/"
        + SilverKingZooKeeperClient.padVersion(configInstanceVersion);
  }

  private String getRingStatePath(IPAndPort node) {
    return getRingInstanceStatePath() + "/" + node.toString();
  }

  public void writeState(IPAndPort node, RingState state) throws KeeperException {
    String path;

    path = getRingStatePath(node);
    if (mc.getZooKeeper().exists(path)) {
      mc.getZooKeeper().setString(path, state.toString());
    } else {
      mc.getZooKeeper().createString(path, state.toString());
    }
  }

  public RingState readState(IPAndPort node) throws KeeperException {
    try {
      return RingState.valueOf(mc.getZooKeeper().getString(getRingStatePath(node)));
    } catch (KeeperException ke) {
      if (ke.getCause() != null
          && NoNodeException.class.isAssignableFrom(ke.getCause().getClass())) {
        return null;
      } else {
        throw ke;
      }
    }
  }
}
