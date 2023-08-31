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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.IOUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/** Base functionality for both cloud-level, and dht instance specific Ws */
public abstract class ServerSetExtensionZKBase<
        W extends ServerSetExtension, M extends MetaPathsBase>
    extends MetaToolModuleBase<W, M> {
  protected String worrisomesPath;

  private static final char delimiterChar = '\n';
  private static final String delimiterString = "" + delimiterChar;

  public ServerSetExtensionZKBase(MetaClientBase<M> mc, String worrisomesPath)
      throws KeeperException {
    super(mc, worrisomesPath);
    this.worrisomesPath = worrisomesPath;
  }

  public String getLatestZKPath() throws KeeperException {
    long version;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    version = _zk.getLatestVersion(worrisomesPath);
    if (version < 0) {
      return null;
    } else {
      return getVBase(version);
    }
  }

  @Override
  public abstract W readFromFile(File file, long version) throws IOException;

  @Override
  public abstract W readFromZK(long version, MetaToolOptions options) throws KeeperException;

  public abstract W readLatestFromZK() throws KeeperException;

  protected Set<String> readNodesAsSet(String path, Stat stat) throws KeeperException {
    String[] nodes;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    nodes = _zk.getString(path, null, stat).split("\n");
    return ImmutableSet.copyOf(nodes);
  }

  public abstract W readLatestFromZK(MetaToolOptions options) throws KeeperException;

  public void writeToFile(File file, W worrisomeList) throws IOException {
    IOUtil.writeAsLines(file, worrisomeList.getServers());
  }

  public void writeToZK(W worrisomeSet) throws IOException, KeeperException {
    writeToZK(worrisomeSet, null);
  }

  public String writeToZK(W worrisomeSet, MetaToolOptions options)
      throws IOException, KeeperException {
    String vBase;
    String zkVal;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    zkVal = CollectionUtil.toString(worrisomeSet.getServers(), "", "", delimiterChar, "");
    vBase = _zk.createString(base + "/", zkVal, CreateMode.PERSISTENT_SEQUENTIAL);
    /*
    vBase = zk.createString(base +"/" , "", CreateMode.PERSISTENT_SEQUENTIAL);
    for (String entity : worrisomeList.getServers()) {
        //System.out.println(vBase +"/"+ entity);
        zk.createString(vBase +"/"+ entity, entity);
    }
    */
    return null;
  }

  public long getVersionMzxid(long version) throws KeeperException {
    Stat stat;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    stat = new Stat();
    _zk.getString(getVBase(version), null, stat);
    return stat.getMzxid();
  }

  public Map<String, Long> getStartOfCurrentWorrisome(Set<String> servers) throws KeeperException {
    Map<String, Long> esStarts;
    long latestWVersion;
    Map<String, Set<String>> worrisomeSets;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    esStarts = new HashMap<>();
    worrisomeSets = new HashMap<>();
    latestWVersion = _zk.getLatestVersion(worrisomesPath);
    for (String server : servers) {
      esStarts.put(server, getStartOfCurrentWorrisome(server, latestWVersion, worrisomeSets));
    }
    return esStarts;
  }

  private long getStartOfCurrentWorrisome(
      String server, long latestWVersion, Map<String, Set<String>> worrisomeSets)
      throws KeeperException {
    long earliestServerVersion;
    Stat stat;
    long version;

    earliestServerVersion = -1;
    version = latestWVersion;
    while (version >= 0) {
      String vBase;
      Set<String> nodes;

      vBase = getVBase(version);
      stat = new Stat();
      nodes = worrisomeSets.get(vBase);
      if (nodes == null) {
        nodes = readNodesAsSet(vBase, stat);
        worrisomeSets.put(vBase, nodes);
      }
      if (!nodes.contains(server)) {
        break;
      }
      earliestServerVersion = version;
      --version;
    }
    return earliestServerVersion;
  }
}
