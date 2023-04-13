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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Base functionality for both cloud-level, and dht instance specific SuspectSets
 */
public abstract class SuspectZKBase<M extends MetaPathsBase> extends ServerSetExtensionZKBase<SuspectSet, M> {
  private static final char suspectsDelimiter = ',';
  private static final String suspectsDelimiterString = "" + suspectsDelimiter;

  private static final String emptySetDef = "<empty>";

  public SuspectZKBase(MetaClientBase<M> mc, String worrisomesPath) throws KeeperException {
    super(mc, worrisomesPath);
    this.worrisomesPath = worrisomesPath;
  }

  @Override
  public SuspectSet readFromFile(File file, long version) throws IOException {
    return new SuspectSet(ServerSet.parse(file, version));
  }

  @Override
  public SuspectSet readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String vBase;
    //List<String>    nodes;
    String[] nodes;
    Stat stat;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    if (version == VersionedDefinition.NO_VERSION) {
      version = _zk.getLatestVersion(base);
    }
    vBase = getVBase(version);
    //nodes = zk.getChildren(vBase);
    stat = new Stat();
    nodes = _zk.getString(vBase, null, stat).split("\n");
    return new SuspectSet(ImmutableSet.copyOf(nodes), version, stat.getMzxid());
  }

  @Override
  public SuspectSet readLatestFromZK() throws KeeperException {
    return readLatestFromZK(null);
  }

  @Override
  public SuspectSet readLatestFromZK(MetaToolOptions options) throws KeeperException {
    Stat stat;
    SilverKingZooKeeperClient _zk;
    List<String> accusers;
    HashSet<String> suspectsSet;

    _zk = mc.getZooKeeper();
    stat = new Stat();

    accusers = _zk.getChildren(worrisomesPath);
    suspectsSet = new HashSet<>();
    for (String accuser : accusers) {
      String suspectsDef;

      suspectsDef = _zk.getString(worrisomesPath + "/" + accuser);
      if (!suspectsDef.equals(emptySetDef)) {
        suspectsDef = suspectsDef.trim().substring(1, suspectsDef.length() - 1);
        suspectsSet.addAll(Arrays.asList(suspectsDef.split(suspectsDelimiterString)));
      }
    }

    return new SuspectSet(suspectsSet, 0, stat.getMzxid());
  }
}
