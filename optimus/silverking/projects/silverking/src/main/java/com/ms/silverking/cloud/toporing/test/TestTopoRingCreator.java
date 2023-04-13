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
package com.ms.silverking.cloud.toporing.test;

import java.io.File;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.storagepolicy.PolicyParseException;
import com.ms.silverking.cloud.storagepolicy.PolicyParser;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyParser;
import com.ms.silverking.cloud.toporing.RingTreeRecipe;
import com.ms.silverking.cloud.toporing.TopologyRing;
import com.ms.silverking.cloud.toporing.TopologyRingCreator;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTopoRingCreator {
  private static File[] parseFiles(String fileDef) {
    String[] fileNames;
    File[] files;

    fileNames = fileDef.split(",");
    files = new File[fileNames.length];
    for (int i = 0; i < fileNames.length; i++) {
      files[i] = new File(fileNames[i]);
    }
    return files;
  }

  private static Logger log = LoggerFactory.getLogger(TestTopoRingCreator.class);

  private static StoragePolicyGroup[] parseStoragePolicyGroups(String fileDef) throws PolicyParseException {
    StoragePolicyGroup[] spGroups;
    File[] files;

    files = parseFiles(fileDef);
    spGroups = new StoragePolicyGroup[files.length];
    for (int i = 0; i < spGroups.length; i++) {
      spGroups[i] = new PolicyParser().parsePolicyGroup(files[i], VersionedDefinition.NO_VERSION);
    }
    return spGroups;
  }

  public static void main(String[] args) {
    try {
      if (args.length != 8) {
        System.out.println(
            "<topologyFile> <weightSpecsFile> <exclusionList> <nodeID> <storagePolicyGroup1,...> <policyID> " +
                "<HostGroupTableFile> <HostGroup,...>");
      } else {
        TopologyRingCreator topologyRingCreator;
        Topology topology;
        File topologyFile;
        File weightSpecsFile;
        File exclusionFile;
        String nodeID;
        String policyID;
        //Replication         replication;
        StoragePolicyGroup[] storagePolicyGroups;
        TopologyRing lastRing;
        HostGroupTable hostGroupTable;
        Set<String> hostGroups;

        //Log.setLevelAll();
        topologyFile = new File(args[0]);
        weightSpecsFile = new File(args[1]);
        exclusionFile = new File(args[2]);
        nodeID = args[3];
        storagePolicyGroups = parseStoragePolicyGroups(args[4]);
        policyID = args[5];
        topology = TopologyParser.parse(topologyFile);
        topologyRingCreator = new TopologyRingCreator();

        hostGroupTable = HostGroupTable.parse(args[6], VersionedDefinition.NO_VERSION);
        hostGroups = ImmutableSet.copyOf(args[7].split(","));

        lastRing = null;

        for (StoragePolicyGroup spGroup : storagePolicyGroups) {
          RingTreeRecipe recipe;
          TopologyRing topologyRing;

          recipe = new RingTreeRecipe(topology, topology.getNodeByID(nodeID),
              new WeightSpecifications(VersionedDefinition.NO_VERSION).parse(weightSpecsFile),
              ExclusionSet.parse(exclusionFile), spGroup, policyID, hostGroupTable, hostGroups, 0L,
              DHTUtil.currentTimeMillis());
          System.out.println("********************************");
          System.out.println("RingTreeRecipe: " + recipe);
          topologyRing = topologyRingCreator.create(recipe, nodeID);
          System.out.println("\n\nTopologyRing:\n");
          System.out.println(topologyRing);
          System.out.println("\n\n");
          if (lastRing != null) {
            topologyRing = topologyRingCreator.reduceDataMovement(lastRing, topologyRing, recipe);
            System.out.println(topologyRing);
          }
          lastRing = topologyRing;
        }
      }
    } catch (Exception e) {
      log.error("",e);
    }
  }

}
