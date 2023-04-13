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
package com.ms.silverking.cloud.management;

import java.io.IOException;

import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.meta.MetaClient;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaTool extends MetaToolBase {
  private enum Tool {Topology, Exclusions, PassiveNodes, HostGroupTables}

  ;

  public MetaTool() {
  }

  private static MetaToolModule getModule(Tool tool, MetaClient metaClient) throws KeeperException {
    switch (tool) {
    case Topology:
      return new TopologyZK(metaClient);
    case Exclusions:
      return new ServerSetExtensionZK(metaClient, metaClient.getMetaPaths().getExclusionsPath());
    //case PassiveNodes: return new ServerSetExtensionZK(metaClient, metaClient.getMetaPaths().getPassiveNodesPath());
    case PassiveNodes:
      return null;
    case HostGroupTables:
      return new HostGroupTableZK(metaClient);
    default:
      throw new RuntimeException("panic");
    }
  }

  private static CloudConfiguration cloudConfigurationFor(Tool tool, String name) {
    switch (tool) {
    case Topology:
      return CloudConfiguration.emptyTemplate.topologyName(name);
    case Exclusions:
      return CloudConfiguration.emptyTemplate.exclusionSpecsName(name);
    case HostGroupTables:
      return CloudConfiguration.emptyTemplate.hostGroupTableName(name);
    default:
      throw new RuntimeException("panic");
    }
  }

  @Override
  protected void doWork(MetaToolOptions options) throws IOException, KeeperException {
    MetaClient mc;
    Tool tool;

    tool = Tool.valueOf(options.tool);
    mc = new MetaClient(cloudConfigurationFor(tool, options.name), new ZooKeeperConfig(options.zkConfig));
    doWork(options, new MetaToolWorker(getModule(tool, mc)));
  }

  public static void main(String[] args) {
    new MetaTool().runTool(args);
  }
}
