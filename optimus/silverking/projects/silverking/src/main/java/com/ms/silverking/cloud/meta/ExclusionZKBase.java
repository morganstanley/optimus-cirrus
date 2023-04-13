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

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base functionality for both cloud-level, and dht instance specific ExclusionSets
 */
public abstract class ExclusionZKBase<M extends MetaPathsBase> extends ServerSetExtensionZKBase<ExclusionSet, M> {
  public static final int noRetainedVersionsLimit = 0;
  private static final int retainedVersions;

  private static Logger log = LoggerFactory.getLogger(ExclusionZKBase.class);

  static {
    retainedVersions = PropertiesHelper.systemHelper.getInt(DHTConstants.exclusionSetRetainedVersionsProperty,
        noRetainedVersionsLimit);
    log.info("{}  {}", DHTConstants.exclusionSetRetainedVersionsProperty, retainedVersions);
  }

  public ExclusionZKBase(MetaClientBase<M> mc, String worrisomesPath) throws KeeperException {
    super(mc, worrisomesPath);
    this.worrisomesPath = worrisomesPath;
  }

  @Override
  public ExclusionSet readFromFile(File file, long version) throws IOException {
    return new ExclusionSet(ServerSet.parse(file, version));
  }

  @Override
  public ExclusionSet readFromZK(long version, MetaToolOptions options) throws KeeperException {
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
    return new ExclusionSet(ImmutableSet.copyOf(nodes), version, stat.getMzxid());
  }

  @Override
  public ExclusionSet readLatestFromZK() throws KeeperException {
    return readLatestFromZK(null);
  }

  @Override
  public ExclusionSet readLatestFromZK(MetaToolOptions options) throws KeeperException {
    String vBase;
    long version;
    Stat stat;
    SilverKingZooKeeperClient _zk;

    _zk = mc.getZooKeeper();
    version = _zk.getLatestVersion(worrisomesPath);
    if (version >= 0) {
      vBase = getVBase(version);
      stat = new Stat();
      return new ExclusionSet(readNodesAsSet(vBase, stat), version, stat.getMzxid());
    } else {
      return ExclusionSet.emptyExclusionSet(0);
    }
  }

  @Override
  public String writeToZK(ExclusionSet exclusionSet, MetaToolOptions options) throws IOException, KeeperException {
    String rVal;

    rVal = super.writeToZK(exclusionSet, options);
    if (retainedVersions != noRetainedVersionsLimit) {
      mc.getZooKeeper().deleteVersionedChildren(base, retainedVersions);
    }
    return rVal;
  }
}
