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
package com.ms.silverking.cloud.topology;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.MetaClient;
import com.ms.silverking.cloud.meta.MetaPaths;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.CreateMode;

/** Writes/reads topologies to/from Zookeeper. */
public class TopologyZK extends MetaToolModuleBase<Topology, MetaPaths> {
  public TopologyZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getTopologyPath());
  }

  public Topology readTopologyAsBlob(long version) throws KeeperException, TopologyParseException {
    String vPath;
    String def;

    vPath = getVersionPath(version);
    def = zk.getString(vPath);
    try {
      return TopologyParser.parseVersioned(def, version);
    } catch (IOException ioe) {
      throw new TopologyParseException(ioe);
    }
  }

  /////////////////

  @Override
  public Topology readFromFile(File file, long version) throws IOException {
    return TopologyParser.parseVersioned(file, version);
  }

  @Override
  public Topology readFromZK(long version, MetaToolOptions options) throws KeeperException {
    try {
      return readTopologyAsBlob(version);
    } catch (TopologyParseException tpe) {
      throw new RuntimeException(tpe);
    }
  }

  @Override
  public void writeToFile(File file, Topology instance) throws IOException {
    throw new RuntimeException("writeToFile not yet implemented for TopologyZK");
  }

  @Override
  public String writeToZK(Topology topology, MetaToolOptions options)
      throws IOException, KeeperException {
    zk.createString(base + "/", topology.toStructuredString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return null;
  }
}
