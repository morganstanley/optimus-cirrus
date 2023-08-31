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

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.storagepolicy.PolicyParser;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.topology.GenericNode;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyParser;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.collection.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingTreeBuilder {
  private static final long maxMagnitudeTolerance =
      TopologyRingCreator.defaultMagnitudeTolerance * 10000;

  private static Logger log = LoggerFactory.getLogger(RingTreeBuilder.class);

  public static RingTree create(RingTreeRecipe recipe, RingTree sourceTree) {
    Map<String, TopologyRing> maps;

    maps = new HashMap<>();
    log.info("Create called with parent: {}", recipe.ringParent);
    if (sourceTree != null) {
      log.info("Source tree non null");
      buildMaps(maps, recipe.ringParent, recipe, sourceTree);
    } else {
      log.info("Source tree is null");
      buildMaps(maps, recipe.ringParent, recipe);
    }
    return new RingTree(recipe.topology, maps, recipe.ringConfigVersion, recipe.ringCreationTime);
  }

  private static void buildMaps(
      Map<String, TopologyRing> maps, Node node, RingTreeRecipe recipe, RingTree sourceTree) {
    log.info("buildMaps {}", node.getIDString());
    if (node.getChildren().size() > 0 && recipe.hasDescendantInHostGroups(node)) {
      buildNodeMap(maps, node, recipe, sourceTree);
      for (Node child : node.getChildren()) {
        log.info("Child of {} is {}", node.getIDString(), child.getIDString());
        if (child.getNodeClass() != NodeClass.server) {
          for (String subPolicyName :
              recipe.storagePolicy.getSubPolicyNamesForNodeClass(child.getNodeClass(), child)) {
            RingTreeRecipe _recipe;

            // need to change the storagePolicy for the below recipe
            _recipe = recipe.newParentAndStoragePolicy(child, subPolicyName);
            buildMaps(maps, child, _recipe, sourceTree);
          }
        }
      }
    }
  }

  private static void buildNodeMap(
      Map<String, TopologyRing> maps, Node node, RingTreeRecipe recipe, RingTree sourceTree) {
    TopologyRingCreator ringCreator;
    TopologyRing ring;
    SingleRing sourceRing;
    boolean built;

    log.info("buildNodeMap: {} {}", node.getIDString(), recipe.storagePolicy.getName());
    sourceRing = (SingleRing) sourceTree.getMap(node.getIDString());
    if (sourceRing == null) {
      throw new RuntimeException("Can't find node for: " + node.getIDString());
    }
    ringCreator = new TopologyRingCreator();
    built = false;
    while (!built && ringCreator.getMagnitudeTolerance() < maxMagnitudeTolerance) {
      try {
        log.info("Building with magnitude tolerance: {}", ringCreator.getMagnitudeTolerance());
        ring = ringCreator.create(recipe, node.getIDString(), sourceRing);
        maps.put(node.getIDString(), ring);
        built = true;
        log.info("{} built successfully", node);
      } catch (Exception e) {
        log.error("", e);
        ringCreator = new TopologyRingCreator(ringCreator.getMagnitudeTolerance() * 10);
      }
    }
    if (!built) {
      log.info("Unable to build. maxMagnitudeTolerance exceeded");
      System.exit(-1);
    }
  }

  /////////////////////////////////////////

  public static RingTree create(RingTreeRecipe recipe) {
    Map<String, TopologyRing> maps;

    maps = new HashMap<>();
    log.info("Create called with parent: ", recipe.ringParent);
    buildMaps(maps, recipe.ringParent, recipe);
    return new RingTree(recipe.topology, maps, recipe.ringConfigVersion, recipe.ringCreationTime);
  }

  private static void buildMaps(Map<String, TopologyRing> maps, Node node, RingTreeRecipe recipe) {
    log.info(
        "buildMaps: {} {} {}",
        node,
        node.getChildren().size() > 0,
        recipe.hasDescendantInHostGroups(node));
    log.info("hostGroups: {}", CollectionUtil.toString(recipe.hostGroups));
    if (node.getChildren().size() > 0 && recipe.hasDescendantInHostGroups(node)) {
      log.info(
          "node {} children {}", node.getIDString(), CollectionUtil.toString(node.getChildren()));
      buildNodeMap(maps, node, recipe);
      for (Node child : node.getChildren()) {
        if (recipe.hasDescendantInHostGroups(child)) {
          if (child.getNodeClass() != NodeClass.server) {
            boolean foundSubPolicy;

            foundSubPolicy = false;
            for (String subPolicyName :
                recipe.storagePolicy.getSubPolicyNamesForNodeClass(child.getNodeClass(), child)) {
              RingTreeRecipe _recipe;

              foundSubPolicy = true;
              // need to change the storagePolicy for the below recipe
              _recipe = recipe.newParentAndStoragePolicy(child, subPolicyName);
              buildMaps(maps, child, _recipe);
            }
            if (!foundSubPolicy) {
              log.info("No subPolicy {} {}", child.getIDString(), child.getNodeClass());
              throw new RuntimeException(
                  String.format("No subPolicy %s %s\n", child.getIDString(), child.getNodeClass()));
            }
          }
        } else {
          log.info(
              "child {} has no descendant in host groups {}",
              node.getIDString(),
              CollectionUtil.toString(recipe.hostGroups));
        }
      }
    } else {
      if (!recipe.hasDescendantInHostGroups(node)) {
        log.info(
            "{} has no descendant in host groups {}",
            node.getIDString(),
            CollectionUtil.toString(recipe.hostGroups));
      }
    }
  }

  private static void buildNodeMap(
      Map<String, TopologyRing> maps, Node node, RingTreeRecipe recipe) {
    TopologyRingCreator ringCreator;
    TopologyRing ring;
    boolean built;

    log.info("buildNodeMap: {} {}", node.getIDString(), recipe.storagePolicy.getName());
    ringCreator = new TopologyRingCreator();
    built = false;
    while (!built && ringCreator.getMagnitudeTolerance() < maxMagnitudeTolerance) {
      try {
        log.info("Building with magnitude tolerance: {}", ringCreator.getMagnitudeTolerance());
        ring = ringCreator.create(recipe, node.getIDString());
        maps.put(node.getIDString(), ring);
        built = true;
        log.info("{} built successfully", node);
      } catch (Exception e) {
        log.error("", e);
        ringCreator = new TopologyRingCreator(ringCreator.getMagnitudeTolerance() * 10);
      }
    }
    if (!built) {
      log.info("Unable to build. maxMagnitudeTolerance exceeded");
      System.exit(-1);
    }
  }

  public static RingTree removeExcludedNodes(RingTree oldRingTree, Set<Node> excludedNodes)
      throws InvalidRingException {
    Map<String, TopologyRing> maps;

    maps = new HashMap<>();
    for (Map.Entry<String, TopologyRing> entry : oldRingTree.getMaps().entrySet()) {
      TopologyRing topoRing;

      topoRing = TopologyRingCreator.removeExcludedMembers(entry.getValue(), excludedNodes);
      maps.put(entry.getKey(), topoRing);
    }
    return new RingTree(
        oldRingTree.getTopology(),
        maps,
        oldRingTree.getRingConfigVersion(),
        DHTUtil.currentTimeMillis());
  }

  public static RingTree removeExcludedNodes(RingTree oldRingTree, ExclusionSet excludedNodes)
      throws InvalidRingException {
    return removeExcludedNodes(oldRingTree, sampleSetNodes(excludedNodes.getServers()));
  }

  private static Set<Node> sampleSetNodes(Set<String> nodeIDs) {
    Set<Node> sampleNodes;

    sampleNodes = new HashSet<>();
    for (String nodeID : nodeIDs) {
      sampleNodes.add(new GenericNode(NodeClass.server, nodeID));
    }
    return sampleNodes;
  }

  public static void main(String[] args) {
    try {
      if (args.length != 8) {
        log.info(
            "<topology> <weights> <exclusionList> <ringParentID> <storagePolicyGroup> <storagePolicyName> "
                + "<HostGroupTableFile> <HostGroup,...>");
      } else {
        File topoFile;
        File weightsFile;
        File exclusionFile;
        String ringParentID;
        File storagePolicyGroupFile;
        String storagePolicyName;
        RingTree ringTree;
        Topology topology;
        StoragePolicyGroup storagePolicyGroup;
        HostGroupTable hostGroupTable;
        Set<String> hostGroups;

        topoFile = new File(args[0]);
        weightsFile = new File(args[1]);
        exclusionFile = new File(args[2]);
        ringParentID = args[3];
        storagePolicyGroupFile = new File(args[4]);
        storagePolicyName = args[5];
        topology = TopologyParser.parse(topoFile);
        storagePolicyGroup =
            new PolicyParser()
                .parsePolicyGroup(storagePolicyGroupFile, VersionedDefinition.NO_VERSION);

        hostGroupTable = HostGroupTable.parse(args[6], VersionedDefinition.NO_VERSION);
        hostGroups = ImmutableSet.copyOf(args[7].split(","));

        ringTree =
            create(
                new RingTreeRecipe(
                    topology,
                    (Node) topology.getNodeByID(ringParentID),
                    new WeightSpecifications(VersionedDefinition.NO_VERSION).parse(weightsFile),
                    ExclusionSet.parse(exclusionFile),
                    storagePolicyGroup,
                    storagePolicyName,
                    hostGroupTable,
                    hostGroups,
                    0L,
                    System.currentTimeMillis()));
        // TopoReplicationSpecification.parse(replicationFile)));
        log.info("{}", ringTree);
        ringTree.test("10.188.1.1");
        ringTree.testDistance("10.188.1.1", "10.188.1.1");
        ringTree.testDistance("10.188.1.1", "10.188.1.2");
        ringTree.testDistance("10.188.1.1", "10.188.2.2");
        ringTree.testDistance("10.188.2.1", "10.188.2.2");
        ringTree.testDistance("10.188.2.2", "10.188.2.2");
        ringTree.testDistance("10.188.1.1", "blah");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static boolean convergenceFeasible(
      RingTree prevRingTree,
      StoragePolicyGroup storagePolicyGroup,
      String storagePolicyName,
      String ringParentName,
      ExclusionSet exclusionSet) {
    ResolvedReplicaMap prevMap;

    prevMap = prevRingTree.getResolvedMap(ringParentName, null);
    for (RingEntry entry : prevMap.getEntries()) {
      if (!convergenceFeasible(entry, exclusionSet)) {
        return false;
      }
    }
    return true;
  }

  private static boolean convergenceFeasible(RingEntry entry, ExclusionSet exclusionSet) {
    boolean feasible;

    feasible = !exclusionSet.filter(entry.getPrimaryOwnersList()).isEmpty();
    if (!feasible) {
      log.info("Can't find viable replica for: {}", entry);
    }
    return feasible;
  }
}
