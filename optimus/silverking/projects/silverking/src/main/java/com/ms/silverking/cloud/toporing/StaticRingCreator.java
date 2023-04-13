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
package com.ms.silverking.cloud.toporing;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ExclusionZK;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.storagepolicy.SimpleStoragePolicyCreator;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyZK;
import com.ms.silverking.cloud.topology.StaticTopologyCreator;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplifies creation of a static ring. Intended for use by simplistic DHT instances
 * (e.g. singleton instances or static DHTs.)
 */
public class StaticRingCreator {
  private static final String hostGroupName = "SimpleHostGroup";

  private static Logger log = LoggerFactory.getLogger(StaticRingCreator.class);

  public static class RingCreationResults {
    public final String exclusionSpecsName;
    public final String hostGroupTableName;
    public final HostGroupTable hostGroupTable;
    public final String hostGroupName;

    RingCreationResults(String exclusionSpecsName, String hostGroupTableName, HostGroupTable hostGroupTable,
        String hostGroupName) {
      this.exclusionSpecsName = exclusionSpecsName;
      this.hostGroupTableName = hostGroupTableName;
      this.hostGroupTable = hostGroupTable;
      this.hostGroupName = hostGroupName;
    }
  }

  /**
   * Create a static ring with primary replication only
   *
   * @param servers     set of servers
   * @param replication primary replication factor
   */
  public static RingCreationResults createStaticRing(String ringName, ZooKeeperConfig zkConfig, Set<String> servers,
      int replication) {
    return createStaticRing(ringName, zkConfig, servers, replication, new UUIDBase(false));
  }

  public static RingCreationResults createStaticRing(String ringName, ZooKeeperConfig zkConfig, Set<String> servers,
      int replication, UUIDBase myID) {
    try {
      MetaClient mc;
      com.ms.silverking.cloud.meta.MetaClient cloudMC;
      NamedRingConfiguration namedRingConfig;
      RingConfiguration ringConfig;
      RingTreeRecipe recipe;
      Topology topology;
      WeightSpecifications weightSpecs;
      ExclusionSet exclusionSet;
      StoragePolicyGroup storagePolicyGroup;
      HostGroupTable hostGroupTable;
      long ringConfigVersion;

      CloudConfiguration cloudConfig;
      String exclusionSpecsName;
      String hostGroupTableName;

      topology = StaticTopologyCreator.createTopology("topology." + myID, servers);
      exclusionSpecsName = "exclusionSpecs." + myID;
      hostGroupTableName = "hostGroupTable." + myID;
      cloudConfig = new CloudConfiguration(topology.getName(), exclusionSpecsName, hostGroupTableName);
      cloudMC = new com.ms.silverking.cloud.meta.MetaClient(cloudConfig, zkConfig);
      new TopologyZK(cloudMC).writeToZK(topology, null);
      new ExclusionZK(cloudMC).writeToZK(ExclusionSet.emptyExclusionSet(0));

      mc = new MetaClient(NamedRingConfiguration.emptyTemplate.ringName(ringName), zkConfig);
      ringConfig = new RingConfiguration(cloudConfig, "weightSpecsName", StaticTopologyCreator.parentID,
          SimpleStoragePolicyCreator.storagePolicyGroupName, SimpleStoragePolicyCreator.storagePolicyName, null);
      new RingConfigurationZK(mc).writeToZK(ringConfig, null);

      namedRingConfig = new NamedRingConfiguration(ringName, ringConfig);
      mc = new MetaClient(namedRingConfig, zkConfig);
      log.info("{}",zkConfig);

      ringConfigVersion = 0;
      weightSpecs = new WeightSpecifications(0);
      exclusionSet = ExclusionSet.emptyExclusionSet(0);

      storagePolicyGroup = SimpleStoragePolicyCreator.createStoragePolicyGroup(replication);
      new StoragePolicyZK(mc).writeToZK(storagePolicyGroup, null);

      hostGroupTable = HostGroupTable.createHostGroupTable(servers, hostGroupName);
      new HostGroupTableZK(cloudMC).writeToZK(hostGroupTable, null);

      recipe = new RingTreeRecipe(topology, StaticTopologyCreator.parentID, weightSpecs, exclusionSet,
          storagePolicyGroup, SimpleStoragePolicyCreator.storagePolicyName, hostGroupTable,
          ImmutableSet.of(hostGroupName), ringConfigVersion, DHTUtil.currentTimeMillis());
      log.info("Recipe.ringParent: {}" , recipe.ringParent);

      RingTree ringTree;
      String newInstancePath;

      ringTree = RingTreeBuilder.create(recipe, null);
      newInstancePath = mc.createConfigInstancePath(ringConfigVersion);
      SingleRingZK.writeTree(mc, topology, newInstancePath, ringTree);

      if (TopoRingConstants.verbose) {
        log.info("{}",ringTree);
        log.info("Building complete");
      }
      return new RingCreationResults(exclusionSpecsName, hostGroupTableName, hostGroupTable, hostGroupName);
    } catch (IOException ioe) {
      log.error("",ioe);
      return null;
    } catch (KeeperException ke) {
      log.error("",ke);
      return null;
    }

  }

  public static void main(String[] args) {
    if (args.length != 4) {
      log.info("args: <ringName> <zkConfig> <servers> <replication>");
    } else {
      try {
        String ringName;
        ZooKeeperConfig zkConfig;
        Set<String> servers;
        int replication;

        ringName = args[0];
        zkConfig = new ZooKeeperConfig(args[1]);
        servers = ImmutableSet.copyOf(StringUtil.splitAndTrim(args[2], ","));
        replication = Integer.parseInt(args[3]);
        createStaticRing(ringName, zkConfig, servers, replication);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
