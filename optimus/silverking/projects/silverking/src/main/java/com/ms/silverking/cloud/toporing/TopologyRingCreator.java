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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.PolicyParser;
import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.storagepolicy.SubPolicy;
import com.ms.silverking.cloud.storagepolicy.SubPolicyMember;
import com.ms.silverking.cloud.topology.GenericNode;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyParser;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.numeric.RingInteger;
import com.ms.silverking.util.Mutability;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a one or more TopologyRings from a Topology and a Set of NodeClasses for which to create
 * the Topologies
 */
public class TopologyRingCreator {
  private final long magnitudeTolerance;
  private final long residualRegionThreshold;
  private final Random random;

  private static Logger log = LoggerFactory.getLogger(TopologyRingCreator.class);

  private static final boolean debug = false;

  private static final String magnitudeToleranceFactorProperty =
      TopologyRingCreator.class.getName() + ".MagnitudeToleranceFactor";
  private static final int defaultMagnitudeToleranceFactor = 1;
  private static final int magnitudeToleranceFactor;

  static {
    magnitudeToleranceFactor =
        PropertiesHelper.systemHelper.getInt(
            magnitudeToleranceFactorProperty,
            defaultMagnitudeToleranceFactor,
            ParseExceptionAction.RethrowParseException);
  }

  static final long defaultMagnitudeTolerance =
      LongRingspace.halfRingspaceErrorRoom * magnitudeToleranceFactor;

  private static final int maxShuffleAttempts = 20;

  public TopologyRingCreator(long magnitudeTolerance) {
    this.magnitudeTolerance = magnitudeTolerance;
    residualRegionThreshold = magnitudeTolerance * 10;
    random = new Random(0);
  }

  public TopologyRingCreator() {
    this(defaultMagnitudeTolerance);
  }

  public long getMagnitudeTolerance() {
    return magnitudeTolerance;
  }

  /**
   * Create a TopologyRing given a root node ID.
   *
   * @param recipe
   * @param nodeID
   * @return
   */
  public TopologyRing create(RingTreeRecipe recipe, String nodeID) {
    Node node;

    node = recipe.topology.getNodeByID(nodeID);
    if (node != null) {
      SingleRing sourceRing;
      TopologyRing _ring;

      if (debug) {
        log.debug("Create for node: {}", node);
      }
      sourceRing =
          SingleRing.emptyRing(
              recipe.topology.getNodeByID(nodeID).getChildNodeClass(),
              0,
              recipe.storagePolicy.getName());
      sourceRing.freeze(recipe.weightSpecs);
      if (debug) {
        log.debug("empty source {}", sourceRing);
      }
      _ring = _create(sourceRing, recipe, nodeID);
      _ring = _ring.simplify();
      _ring.freeze(recipe.weightSpecs);
      return _ring;
    } else {
      if (debug) {
        log.debug("Couldn't find nodeID: ", nodeID);
      }
      return null;
    }
  }

  public TopologyRing create(RingTreeRecipe recipe, String nodeID, SingleRing sourceRing) {
    Node node;

    Preconditions.checkNotNull(sourceRing);
    node = recipe.topology.getNodeByID(nodeID);
    if (node != null) {
      TopologyRing _ring;

      if (debug) {
        log.debug("Create w/ source for node: {}", node);
      }
      _ring = _create2(sourceRing, recipe, nodeID);
      _ring = _ring.simplify();
      _ring.freeze(recipe.weightSpecs);
      return _ring;
    } else {
      if (debug) {
        log.debug("Couldn't find nodeID: {}", nodeID);
      }
      return null;
    }
  }

  /**
   * Create a new ring that is as similar as possible to the ring passed in so that we minimize the
   * movement of data.
   *
   * @param sourceRing an immutable SingleRing that may or may not meet the requirements of the
   *     recipe
   * @param recipe
   * @return an immutable SingleRing (returned as a TopologyRing) that meets the requirements of the
   *     recipe
   */
  private TopologyRing _create2(SingleRing sourceRing, RingTreeRecipe recipe, String ringParentID) {
    /*
     * Single StoragePolicy applies. StoragePolicy has primary and secondary
     * SubPolicy Each SubPolicy has multiple SubPolicyMembers
     *
     * Each SubPolicyMember has a logical ring associated with it - The set
     * of nodes in each of these rings is disjoint with respect to all other
     * ring node sets - Weights must be normalized for each member
     *
     * Loop structure: For {primary, secondary} For all SubPolicyMembers For
     * all replicas
     */
    Node parent;
    NodeClass nodeClass;

    if (debug) {
      log.debug("TopologyRingCreator.create2()");
    }

    Preconditions.checkNotNull(sourceRing);
    parent = recipe.topology.getNodeByID(ringParentID);
    nodeClass = parent.getChildNodeClass();

    ProtoRegionList prList;

    prList = ProtoRegionList.createIsomorphic(sourceRing);
    for (SubPolicy subPolicy : recipe.storagePolicy.getSubPolicies()) {
      allocateSubPolicy2(sourceRing, prList, subPolicy, ringParentID, recipe);
    }

    log.info("*********************");
    log.info("{}", prList);

    return prList.toSingleRing(nodeClass, recipe);
  }

  private void allocateSubPolicy2(
      SingleRing sourceRing,
      ProtoRegionList prList,
      SubPolicy subPolicy,
      String ringParentID,
      RingTreeRecipe recipe) {
    Node parent;

    if (debug) {
      log.debug("TopologyRingCreator.allocateSubPolicy2()");
      log.debug("subPolicy: {}", subPolicy);
      System.out.flush();
    }
    parent = recipe.topology.getNodeByID(ringParentID);
    for (SubPolicyMember member : subPolicy.getMembers()) {
      allocateSubPolicyMember2(
          sourceRing, prList, member, recipe, parent, subPolicy.getReplicationType());
    }
  }

  private enum RegionAllocationMode {
    Primary,
    Secondary,
    Any
  };

  /*
   * private int nextRegion(ProtoRegionList prList, RegionAllocationMode
   * regionAllocationMode, SingleRing sourceRing, int regionIndex, Node node,
   * int maxOwners, ReplicationType rType) { int prevRegionIndex; int
   * nextRegionIndex; boolean regionIndexSearched;
   *
   * if (debug) { System.out.printf(
   * "TopologyRingCreator.nextRegion regionIndex %d maxOwners %d\n",
   * regionIndex, maxOwners); System.out.printf("regionAllocationMode %s\n",
   * regionAllocationMode); } prevRegionIndex = regionIndex - 1;
   * nextRegionIndex = Integer.MAX_VALUE; regionIndexSearched = false; while
   * (true) { //while (nextRegionIndex >= 0 && nextRegionIndex != regionIndex)
   * { ProtoRegion candidateRegion; boolean valid; int searchStartIndex;
   *
   * // Find a region not containing this node searchStartIndex =
   * (prevRegionIndex + 1) % prList.size(); if (debug) { System.out.printf(
   * "TopologyRingCreator.nextRegion loop prevRegionIndex %d nextRegionIndex %d searchStartIndex %d\n"
   * , prevRegionIndex, nextRegionIndex, searchStartIndex); } nextRegionIndex
   * = prList.nextRegion2(searchStartIndex, node, maxOwners, rType); if
   * (nextRegionIndex == prevRegionIndex) { if (debug) {
   * System.out.println("nextRegionIndex == prevRegionIndex, exiting\n"); }
   * return -1; } else { RingInterval indexProgressionInterval;
   *
   * indexProgressionInterval = new RingInterval(new RingInteger(prList.size()
   * - 1, searchStartIndex), new RingInteger(prList.size() - 1,
   * nextRegionIndex)); if (indexProgressionInterval.contains(new
   * RingInteger(prList.size() - 1, regionIndex))) { if (debug) {
   * System.out.printf
   * ("searchStartIndex %d nextRegionIndex %d regionIndex %d\n",
   * searchStartIndex, nextRegionIndex, regionIndex ); } if
   * (regionIndexSearched) { if (debug) {
   * System.out.println("regionIndexSearched is true, exiting\n"); } return
   * -1; } else { regionIndexSearched = true; } } } prevRegionIndex =
   * nextRegionIndex;
   *
   * // Now check to see if this region meets the ownership requirements of
   * the current RegionAllocationMode candidateRegion =
   * prList.get(nextRegionIndex); if (debug) {
   * System.out.printf("candidateRegion %s\n", candidateRegion); } switch
   * (regionAllocationMode) { case Primary: valid =
   * sourceRing.pointOwnedByNode(candidateRegion.getRegion().getStart(), node,
   * OwnerQueryMode.Primary); break; case Secondary: valid =
   * sourceRing.pointOwnedByNode(candidateRegion.getRegion().getStart(), node,
   * OwnerQueryMode.Secondary); break; case Any: valid = true; break; default:
   * throw new RuntimeException("panic"); } if (valid) { return
   * nextRegionIndex; } } //if (debug) { //
   * System.out.printf("nextRegionIndex %d regionIndex %d\n", nextRegionIndex,
   * regionIndex); // System.out.println("end of method, exiting\n"); //}
   * //return -1; }
   */

  private static int lastCandidateValue = 0;

  private int nextRegion(
      ProtoRegionList prList,
      RegionAllocationMode regionAllocationMode,
      SingleRing sourceRing,
      int regionIndex,
      Node node,
      int maxOwners,
      ReplicationType rType,
      Set<Node> remainingNodes) {
    int minPotentialOwners;
    int bestCandidateIndex;

    if (debug) {
      log.debug(
          "TopologyRingCreator.nextRegion regionIndex {} maxOwners {}", regionIndex, maxOwners);
      log.debug("regionAllocationMode {}", regionAllocationMode);
    }
    minPotentialOwners = Integer.MAX_VALUE;
    bestCandidateIndex = -1;
    RingInteger _candidateRegionIndex = new RingInteger(prList.size() - 1, lastCandidateValue);
    for (int ii = 0; ii < prList.size(); ii++) {
      int candidateRegionIndex = _candidateRegionIndex.getValue();
      // for (int candidateRegionIndex = 0; candidateRegionIndex <
      // prList.size(); candidateRegionIndex++) {
      ProtoRegion candidateRegion;
      boolean valid;

      // Find a region not containing this node
      if (debug) {
        log.debug(
            "TopologyRingCreator.nextRegion loop candidateRegionIndex {}", candidateRegionIndex);
      }

      // Now check to see if this region meets the ownership requirements
      // of the current RegionAllocationMode
      candidateRegion = prList.get(candidateRegionIndex);
      if (!candidateRegion.contains(node) && candidateRegion.getOwners(rType).size() < maxOwners) {
        if (debug) {
          log.debug("candidateRegion {}", candidateRegion);
        }
        switch (regionAllocationMode) {
          case Primary:
            valid =
                sourceRing.pointOwnedByNode(
                    candidateRegion.getRegion().getStart(), node, OwnerQueryMode.Primary);
            break;
          case Secondary:
            valid =
                sourceRing.pointOwnedByNode(
                    candidateRegion.getRegion().getStart(), node, OwnerQueryMode.Secondary);
            break;
          case Any:
            valid = true;
            break;
          default:
            throw new RuntimeException("panic");
        }
        if (valid) {
          // return candidateRegionIndex;
          Set<Node> potentialOwnersSet;
          int potentialOwners;

          potentialOwnersSet = new HashSet<>(remainingNodes);
          potentialOwnersSet.removeAll(candidateRegion.getOwnersSet());
          potentialOwners = potentialOwnersSet.size();
          // potentialOwners = prList.size() -
          // (candidateRegion.totalOwners() + ii);
          if (potentialOwners < minPotentialOwners) {
            minPotentialOwners = potentialOwners;
            bestCandidateIndex = candidateRegionIndex;
          }
        }
      }
      _candidateRegionIndex.increment();
    }
    return bestCandidateIndex;
  }

  class SwapResult {
    final int ownedIndex;
    final int nonownedIndex;
    final long swapSize;

    SwapResult(int ownedIndex, int nonownedIndex, long swapSize) {
      this.ownedIndex = ownedIndex;
      this.nonownedIndex = nonownedIndex;
      this.swapSize = swapSize;
    }
  }

  private SwapResult swap(
      ProtoRegionList prList, Node newNode, int ownedIndex, int nonownedIndex, long maxAllocation) {
    Node existingNode;
    ProtoRegion ownedRegion;
    ProtoRegion nonownedRegion;

    if (debug) {
      log.debug(
          "in swap {} ownedIndex {} nonownedIndex {} maxAllocation {}",
          newNode,
          ownedIndex,
          nonownedIndex,
          maxAllocation);
      System.out.flush();
    }
    // First make sure that ownedRegion size <= maxAllocation
    ownedRegion = prList.get(ownedIndex);
    if (maxAllocation < ownedRegion.getRegionSize()) {
      if (debug) {
        log.debug("reducing to max allocation: {}", ownedRegion);
      }
      prList.splitProtoRegion(ownedIndex, maxAllocation);
      if (nonownedIndex > ownedIndex) {
        nonownedIndex++;
      }
    }

    // Get the regions (get owned again since it might have been split)
    ownedRegion = prList.get(ownedIndex);
    nonownedRegion = prList.get(nonownedIndex);
    if (debug) {
      log.debug("ownedRegion: {}", ownedRegion);
      log.debug("nonownedRegion: {}", nonownedRegion);
    }

    if (ownedRegion.getRegionSize() > nonownedRegion.getRegionSize()) {
      if (debug) {
        log.debug("splitting ownedRegion");
      }
      prList.splitProtoRegion(ownedIndex, nonownedRegion.getRegionSize());
      if (nonownedIndex > ownedIndex) {
        nonownedIndex++;
      }
    } else if (ownedRegion.getRegionSize() < nonownedRegion.getRegionSize()) {
      if (debug) {
        log.debug("splitting nonownedRegion");
      }
      prList.splitProtoRegion(nonownedIndex, ownedRegion.getRegionSize());
      if (ownedIndex > nonownedIndex) {
        ownedIndex++;
      }
    } else {
      // no action necessary
      if (debug) {
        log.debug("no region split necessary");
      }
    }

    // Get the regions again since they might have been split
    ownedRegion = prList.get(ownedIndex);
    nonownedRegion = prList.get(nonownedIndex);
    if (debug) {
      log.debug(" ownedRegion: {}", ownedRegion);
      log.debug(" nonownedRegion: ", nonownedRegion);
    }
    assert ownedRegion.getRegionSize() == nonownedRegion.getRegionSize();

    // Select victim
    existingNode = nonownedRegion.getLastAddedOwnerNotIn(ownedRegion.getOwnersSet());

    // Swap
    nonownedRegion.replaceOwner(existingNode, newNode);
    ownedRegion.replaceOwner(newNode, existingNode);
    if (debug) {
      log.debug("After swap");
      log.debug("ownedRegion: {}", ownedRegion);
      log.debug(" nonownedRegion: {}", nonownedRegion);
    }

    return new SwapResult(ownedIndex, nonownedIndex, ownedRegion.getRegionSize());
  }

  /**
   * Called when a region couldn't be found. This method moves allocations around so that a region
   * can be given to the node specified.
   *
   * @return
   */
  private int createNextRegion(
      ProtoRegionList prList, Node node, ReplicationType rType, int maxOwners, long swapRemaining) {
    // FUTURE - refactor this after concept is proven
    long swapped;
    int ownedRegion;
    int nonownedRegion;
    int firstSwappedRegion;

    swapped = 0;
    ownedRegion = -1;
    nonownedRegion = -1;
    firstSwappedRegion = -1;
    // while (swapped < swapRemaining) {
    // Find a non-full region that this node owns
    if (ownedRegion == -1) {
      for (int candidateRegionIndex = prList.size() - 1;
          candidateRegionIndex >= 0;
          candidateRegionIndex--) {
        ProtoRegion candidateRegion;

        candidateRegion = prList.get(candidateRegionIndex);
        if (candidateRegion.contains(node) && candidateRegion.getOwners(rType).size() < maxOwners) {
          ownedRegion = candidateRegionIndex;
        }
      }
    }
    if (ownedRegion < 0) {
      log.info("Couldn't find ownedRegion {} {}", node, prList.toString());
    }

    // Find a region that this node doesn't own (preconditions guarantee such a region is full)
    if (nonownedRegion == -1) {
      for (int candidateRegionIndex = prList.size() - 1;
          candidateRegionIndex >= 0;
          candidateRegionIndex--) {
        ProtoRegion candidateRegion;

        candidateRegion = prList.get(candidateRegionIndex);
        if (!candidateRegion.contains(node)) {
          if (candidateRegion.getOwners(rType).size() < maxOwners) {
            // this shouldn't happen if preconditions are met
            throw new RuntimeException("Unexpectedly found a free region");
          } else {
            nonownedRegion = candidateRegionIndex;
          }
        }
      }
    }
    if (nonownedRegion < 0) {
      log.info("Couldn't find nonownedRegion {} {}", node, prList.toString());
    }

    // Swap ownership
    SwapResult swapResult;

    try {
      swapResult = swap(prList, node, ownedRegion, nonownedRegion, swapRemaining);
    } catch (RuntimeException re) {
      log.info("{}", prList);
      throw re;
    }
    ownedRegion = swapResult.ownedIndex;
    swapped += swapResult.swapSize;
    if (firstSwappedRegion < 0) {
      firstSwappedRegion = swapResult.ownedIndex;
    }
    ownedRegion = -1;
    nonownedRegion = -1;
    // }
    if (firstSwappedRegion < 0) {
      log.info(prList.toString());
      throw new RuntimeException("createNextRegion failed");
    } else {
      return firstSwappedRegion;
    }
  }

  static class NASAvailableRingspaceComparator implements Comparator<NodeAllocationState> {
    NASAvailableRingspaceComparator() {}

    @Override
    public int compare(NodeAllocationState nas0, NodeAllocationState nas1) {
      return Long.compare(nas0.availableRingspace(), nas1.availableRingspace());
    }
  }

  private List<RingRegion> fullRegions;
  private static final NASAvailableRingspaceComparator nasAvailableRingspaceComparator =
      new NASAvailableRingspaceComparator();

  class NodeAllocationState {
    private final Node node;
    private final long[] allocated;
    private final long toAllocate;
    private final List<RingRegion>[] allocatedRegions;

    NodeAllocationState(Node node, int replicas, long toAllocate) {
      this.node = node;
      this.allocated = new long[replicas];
      this.toAllocate = toAllocate;
      allocatedRegions = new List[replicas];
      for (int i = 0; i < allocatedRegions.length; i++) {
        allocatedRegions[i] = new ArrayList<>();
      }
    }

    Node getNode() {
      return node;
    }

    void addAllocation(int replica, RingRegion region) {
      long allocation;

      allocation = region.getSize();
      if (allocated[replica] + allocation > NumUtil.addWithClamp(toAllocate, magnitudeTolerance)) {
        throw new RuntimeException(
            "Excessive allocation: "
                + node
                + " "
                + replica
                + " "
                + allocated[replica]
                + "\t"
                + toAllocate);
      }
      allocated[replica] += allocation;
      allocatedRegions[replica].add(region);
    }

    void addAllocation(int replica, List<RingRegion> regions) {
      long allocation;

      allocation = RingRegion.getTotalSize(regions);
      if (allocated[replica] + allocation > NumUtil.addWithClamp(toAllocate, magnitudeTolerance)) {
        throw new RuntimeException(
            "Excessive allocation: "
                + node
                + " "
                + replica
                + " allocation: "
                + allocation
                + " allocated[replica]: "
                + allocated[replica]
                + "\ttoAllocate: "
                + toAllocate
                + "\t allocated+allocation: "
                + (allocated[replica] + allocation)
                + "\t toAllocate+magnitudeTolerance "
                + (toAllocate + magnitudeTolerance)
                + "\t NumUtil.addWithClamp(toAllocate, magnitudeTolerance) "
                + NumUtil.addWithClamp(toAllocate, magnitudeTolerance));
      }
      allocated[replica] += allocation;
      allocatedRegions[replica].addAll(regions);
    }

    long getAllocated(int replica) {
      return allocated[replica];
    }

    List<RingRegion> getAllocatedRegions(int replica) {
      return allocatedRegions[replica];
    }

    long allocationRemaining(int replica) {
      return toAllocate - allocated[replica];
    }

    long availableRingspace() {
      long rVal;

      // for (RingRegion r : RingRegion.union(allocatedRegions)) {
      // System.out.printf("%s\n", r.toString());
      // }
      rVal =
          LongRingspace.size
              - RingRegion.getTotalSize(RingRegion.union(allocatedRegions, fullRegions));
      // rVal = LongRingspace.size
      //        - RingRegion.getTotalSize(RingRegion.union(
      //              RingRegion.union(allocatedRegions), fullRegions));
      // System.out.printf("RingRegion.getTotalSize %d\n",
      // RingRegion.getTotalSize(RingRegion.union(allocatedRegions)));
      // System.out.printf("%d\n%d\n%d\n",
      // rVal,
      // LongRingspace.size -
      // RingRegion.getTotalSize(RingRegion.union(allocatedRegions)),
      // LongRingspace.size,
      // RingRegion.getTotalSize(RingRegion.union(allocatedRegions)));
      return rVal;
    }

    long ringspaceToAllocate() {
      return toAllocate;
    }

    NodeAllocationState limitAllocation(long limit) {
      return new NodeAllocationState(node, allocated.length, Math.min(toAllocate, limit));
    }
  }

  private List<NodeAllocationState> createNodeAllocationStates(
      SubPolicyMember member, RingTreeRecipe recipe, Node parent, int replicas) {
    List<NodeAllocationState> nodeAllocationStates;
    List<Node> nodes;
    NodeClass nodeClass;
    List<Long> nodeRegionSizes;

    nodeClass = parent.getChildNodeClass();
    if (nodeClass == null) {
      log.debug("parent.getChildren().size(): {}", parent.getChildren().size());
      throw new RuntimeException("No nodes for parent " + parent);
    }
    nodes = member.getNodesList(parent, recipe);
    if (debug) {
      log.debug("nodes.size() ", nodes.size());
    }
    nodeRegionSizes =
        RingRegion.allRingspace.dividedRegionSizes(recipe.weightSpecs.getWeights(nodes));
    if (debug) {
      log.debug(CollectionUtil.toString(nodeRegionSizes, '\n'));
    }

    nodeAllocationStates = new ArrayList<>(nodes.size());
    for (int i = 0; i < nodes.size(); i++) {
      nodeAllocationStates.add(
          new NodeAllocationState(nodes.get(i), replicas, nodeRegionSizes.get(i)));
    }
    return nodeAllocationStates;
  }

  private List<NodeAllocationState> getInitialAllocationStates(
      List<NodeAllocationState> nodeAllocationStates_final,
      SingleRing sourceRing,
      ReplicationType rType,
      int replicas) {
    List<NodeAllocationState> nodeAllocationStates_initial;
    OwnerQueryMode oqm;

    switch (rType) {
      case Primary:
        oqm = OwnerQueryMode.Primary;
        break;
      case Secondary:
        oqm = OwnerQueryMode.Secondary;
        break;
      default:
        throw new RuntimeException("panic");
    }

    nodeAllocationStates_initial = new ArrayList<>(nodeAllocationStates_final.size());
    for (int i = 0; i < nodeAllocationStates_final.size(); i++) {
      long oldAllocation;
      long allocationLimit;

      oldAllocation =
          sourceRing.getOwnedRingspace(nodeAllocationStates_final.get(i).getNode(), oqm);
      allocationLimit = oldAllocation / replicas;
      if (debug) {
        log.debug(
            "node {}  oldAllocation {}  allocationLimit {}",
            nodeAllocationStates_final.get(i).getNode(),
            oldAllocation,
            allocationLimit);
      }
      nodeAllocationStates_initial.add(
          nodeAllocationStates_final.get(i).limitAllocation(allocationLimit));
    }
    return nodeAllocationStates_initial;
  }

  private void allocateSubPolicyMember2(
      SingleRing sourceRing,
      ProtoRegionList prList,
      SubPolicyMember member,
      RingTreeRecipe recipe,
      Node parent,
      ReplicationType rType) {
    int replicas;
    boolean allReplicasTag;
    List<NodeAllocationState> nodeAllocationStates_initial;
    List<NodeAllocationState> nodeAllocationStates_final;

    if (debug) {
      log.debug("TopologyRingCreator.allocateSubPolicyMember2()");
    }
    fullRegions = new ArrayList<>();
    replicas = member.getQuantity();
    allReplicasTag = replicas == SubPolicyMember.ALL;
    if (allReplicasTag) {
      replicas = member.getNodesList(parent, recipe).size();
    }
    nodeAllocationStates_final = createNodeAllocationStates(member, recipe, parent, replicas);
    nodeAllocationStates_initial =
        getInitialAllocationStates(nodeAllocationStates_final, sourceRing, rType, replicas);
    doAllocation(sourceRing, prList, rType, replicas, allReplicasTag, nodeAllocationStates_initial);
    for (ProtoRegion pr : prList.getRegionList()) {
      if (pr.totalOwners() >= replicas) {
        fullRegions.add(pr.getRegion());
      }
    }
    for (int i = 0; i < nodeAllocationStates_final.size(); i++) {
      for (int j = 0; j < replicas; j++) {
        nodeAllocationStates_final
            .get(i)
            .addAllocation(j, nodeAllocationStates_initial.get(i).getAllocatedRegions(j));
      }
    }
    doAllocation(sourceRing, prList, rType, replicas, allReplicasTag, nodeAllocationStates_final);
  }

  private void doAllocation(
      SingleRing sourceRing,
      ProtoRegionList prList,
      ReplicationType rType,
      int replicas,
      boolean allReplicasTag,
      List<NodeAllocationState> nodeAllocationStates) {
    if (debug) {
      log.debug("doAllocation()");
    }
    /*
     * for (ProtoRegion pr : prList.getRegionList()) { if (pr.totalOwners()
     * >= replicas) { fullRegions.add(pr.getRegion()); } }
     */
    for (RegionAllocationMode regionAllocationMode : RegionAllocationMode.values()) {
      for (int replicaIndex = 0; replicaIndex < replicas; replicaIndex++) {
        Set<Node> remainingNodes;

        remainingNodes = new HashSet<>(prList.getOwners());

        List<NodeAllocationState> unallocated;
        unallocated = new ArrayList<>(nodeAllocationStates);
        Collections.sort(unallocated, nasAvailableRingspaceComparator);

        int nodeIndex = 0;
        while (unallocated.size() > 0) {
          // for (int nodeIndex = 0; nodeIndex <
          // nodeAllocationStates.size(); nodeIndex++) {
          NodeAllocationState nas;
          Node node;
          int regionIndex;
          long ringspaceAllocated;
          boolean nodeSearchValid;

          // Collections.sort(unallocated,
          //        nasAvailableRingspaceComparator);
          // nas = nodeAllocationStates.get(nodeIndex);
          nas = unallocated.remove(0);
          node = nas.getNode();
          remainingNodes.remove(node);
          ringspaceAllocated = 0;
          if (debug) {
            log.debug(
                "regionAllocationMode {} replicaIndex {} nodeIndex {} node {} availableRingspace {}",
                regionAllocationMode,
                replicaIndex,
                nodeIndex,
                node,
                nas.availableRingspace());
          }

          if (allReplicasTag) {
            regionIndex = nodeIndex;
          } else {
            regionIndex = 0;
          }

          nodeSearchValid = true;
          while (nodeSearchValid
              && nas.allocationRemaining(replicaIndex) - magnitudeTolerance > 0) {
            ProtoRegion protoRegion;

            if (debug) {
              log.debug(
                  " ringspaceAllocated {} ringspaceToAllocate {}  remaining {}",
                  ringspaceAllocated,
                  nas.ringspaceToAllocate(),
                  nas.allocationRemaining(replicaIndex));
              log.debug("regionIndex {}", regionIndex);
            }
            // Find an available region
            if (!allReplicasTag) {
              regionIndex =
                  nextRegion(
                      prList,
                      regionAllocationMode,
                      sourceRing,
                      regionIndex,
                      node,
                      replicas,
                      rType,
                      remainingNodes);
            } else {
              regionIndex = prList.nextRegion(regionIndex, node, replicaIndex, rType);
            }
            if (debug) {
              log.debug("doAllocation() next regionIndex {}", regionIndex);
            }
            if (regionIndex < 0) {
              if (allReplicasTag) {
                break;
              } else {
                switch (regionAllocationMode) {
                  case Primary:
                  case Secondary:
                    regionIndex = 0;
                    nodeSearchValid = false;
                    continue;
                  case Any:
                    regionIndex =
                        createNextRegion(
                            prList, node, rType, replicas, nas.allocationRemaining(replicaIndex));
                    break;
                  default:
                    throw new RuntimeException("panic");
                }
              }
            }

            // Now add this node to the region found
            protoRegion = prList.get(regionIndex);
            if (debug) {
              log.debug("{}", protoRegion.getRegion());
              log.debug(
                  "{} {}",
                  protoRegion.getRegion().getSize(),
                  nas.ringspaceToAllocate() - magnitudeTolerance);
              log.debug(
                  "A nodeAllocated {} node {} ringspaceAllocated {}",
                  nas.getAllocated(replicaIndex),
                  node,
                  ringspaceAllocated);
            }
            if (protoRegion.getRegion().getSize()
                <= NumUtil.addWithClamp(
                    nas.allocationRemaining(replicaIndex), magnitudeTolerance)) {
              // The candidate region size is <=
              // ringspaceToAllocate, so we add the entire region
            } else {
              // The candidate region size is >
              // ringspaceToAllocate.
              // Split the region and add this node to the
              // appropriate portion.
              if (debug) {
                log.debug("\t***splitProtoRegion***");
              }
              prList.splitProtoRegion(regionIndex, nas.allocationRemaining(replicaIndex));
              // Need to get protoRegion from list again since the
              // previous region was split
              protoRegion = prList.get(regionIndex);
              if (protoRegion.getRegion().getSize() != nas.allocationRemaining(replicaIndex)) {
                throw new RuntimeException("panic");
              }
            }
            nas.addAllocation(replicaIndex, protoRegion.getRegion());
            ringspaceAllocated += protoRegion.getRegion().getSize();
            protoRegion.addOwner(rType, node);
            if (protoRegion.totalOwners() >= replicas) {
              if (debug) {
                log.debug("full {}  [availableRingspace]", protoRegion.getRegion());
              }
              fullRegions.add(protoRegion.getRegion());
            } else {
              if (debug) {
                log.debug("notfull {}[availableRingspace]", protoRegion.getRegion());
              }
            }

            if (ringspaceAllocated < nas.ringspaceToAllocate() - magnitudeTolerance) {
              if (debug) {
                log.debug("incrementing regionIndex {}", regionIndex);
              }
              regionIndex = (regionIndex + 1) % prList.size();
            }
            if (debug) {
              log.debug(
                  "B nodeAllocated {} node {} ringspaceAllocated {}",
                  nas.getAllocated(replicaIndex),
                  node,
                  ringspaceAllocated);
              log.debug("after regionIndex {}", regionIndex);
            }
          }
        }
      }
    }
    prList.mergeResidualRegions(residualRegionThreshold);
  }

  /**
   * Create a new ring that is as similar as possible to the ring passed in so that we minimize the
   * movement of data.
   *
   * @param sourceRing an immutable SingleRing that may or may not meet the requirements of the
   *     recipe
   * @param recipe
   * @return an immutable SingleRing (returned as a TopologyRing) that meets the requirements of the
   *     recipe
   */
  private TopologyRing _create(SingleRing sourceRing, RingTreeRecipe recipe, String ringParentID) {
    // This version ignores the source ring, _create2 takes the source into
    // account

    /*
     * Single StoragePolicy applies. StoragePolicy has primary and secondary
     * SubPolicy Each SubPolicy has multiple SubPolicyMembers
     *
     * Each SubPolicyMember has a logical ring associated with it - The set
     * of nodes in each of these rings is disjoint with respect to all other
     * ring node sets - Weights must be normalized for each member
     *
     * Loop structure: For {primary, secondary} For all SubPolicyMembers For
     * all replicas
     */
    Node parent;
    NodeClass nodeClass;

    parent = recipe.topology.getNodeByID(ringParentID);
    nodeClass = parent.getChildNodeClass();

    ProtoRegionList prList;

    prList = ProtoRegionList.createEmpty();
    for (SubPolicy subPolicy : recipe.storagePolicy.getSubPolicies()) {
      prList = allocateSubPolicy(prList, subPolicy, ringParentID, recipe);
    }

    log.info("*********************");
    log.info("{}", prList);

    return prList.toSingleRing(nodeClass, recipe);
  }

  /*
   * private SingleRing clearOldOwners(SingleRing oldRing, Set<Node>
   * includeNodes) { SingleRing newRing;
   *
   * newRing = oldRing.cloneEmpty(Mutability.Mutable); for (RingEntry oldEntry
   * : oldRing.getMembers()) {
   * newRing.addEntry(oldEntry.removeOwnersNotInSet(includeNodes)); } return
   * newRing; }
   */

  private ProtoRegionList allocateSubPolicy(
      ProtoRegionList prList, SubPolicy subPolicy, String ringParentID, RingTreeRecipe recipe) {
    Node parent;

    if (debug) {
      log.debug("subPolicy: {}", subPolicy);
      System.out.flush();
    }
    parent = recipe.topology.getNodeByID(ringParentID);
    for (SubPolicyMember member : subPolicy.getMembers()) {
      prList =
          allocateSubPolicyMember(prList, member, recipe, parent, subPolicy.getReplicationType());
    }
    return prList;
  }

  private ProtoRegionList allocateSubPolicyMember(
      ProtoRegionList prList,
      SubPolicyMember member,
      RingTreeRecipe recipe,
      Node parent,
      ReplicationType rType) {
    List<Node> nodes;
    int replicas;
    NodeClass nodeClass;
    List<Long> nodeRegionSizes;
    List<Integer> shuffle;
    boolean allReplicasTag;

    if (recipe.getWeight(parent) == 0.0) {
      log.info("Ignoring allocation for zero-weighted node {}", parent.getIDString());
      return prList;
    }
    if (debug) {
      log.debug("Incoming prList {}", prList);
    }

    nodeClass = parent.getChildNodeClass();
    if (nodeClass == null) {
      log.error("parent.getChildren().size(): {}", parent.getChildren().size());
      throw new RuntimeException("No nodes for parent " + parent);
    }
    // System.out.printf("parent %s\n", parent.getIDString());
    nodes = member.getNodesList(parent, recipe);
    if (debug) {
      log.debug("nodes.size() ", nodes.size());
    }

    replicas = member.getQuantity();
    allReplicasTag = replicas == SubPolicyMember.ALL;
    if (allReplicasTag) {
      replicas = nodes.size();
    }

    if (replicas == nodes.size()) {
      addNodesToBlankProtoRegionList(prList, nodes);
    } else {
      nodeRegionSizes =
          RingRegion.allRingspace.dividedRegionSizes(recipe.weightSpecs.getWeights(nodes));
      if (debug) {
        log.debug("nodeRegionSizes");
        log.debug(CollectionUtil.toString(nodeRegionSizes, '\n'));
      }
      shuffle = new ArrayList<>();
      for (int i = 0; i < nodes.size(); i++) {
        shuffle.add(i);
      }

      for (int replicaIndex = 0; replicaIndex < replicas; replicaIndex++) {
        ProtoRegionList _prList;
        int attemptIndex;
        boolean successful;

        if (debug) {
          log.debug("replicaIndex {}", replicaIndex);
        }
        _prList = prList.duplicate();
        attemptIndex = 0;
        successful = false;
        while (!successful && attemptIndex++ < maxShuffleAttempts) {
          Collections.shuffle(shuffle, random);
          try {
            for (int _nodeIndex = 0; _nodeIndex < nodes.size(); _nodeIndex++) {
              Node node;
              int regionIndex;
              long ringspaceToAllocate;
              long ringspaceAllocated;
              int nodeIndex;

              nodeIndex = shuffle.get(_nodeIndex);
              if (debug) {
                log.debug(
                    "replicaIndex {} nodeIndex {} _nodeIndex {} attemptIndex {}",
                    replicaIndex,
                    nodeIndex,
                    _nodeIndex,
                    attemptIndex);
                System.out.flush();
              }

              // Determine how much ringspace to allocate to this node
              node = nodes.get(nodeIndex);
              ringspaceToAllocate = nodeRegionSizes.get(nodeIndex);
              ringspaceAllocated = 0;
              quickCheckAllocations(
                  prList,
                  nodes,
                  nodeRegionSizes,
                  replicaIndex,
                  nodeIndex,
                  ringspaceToAllocate - ringspaceAllocated,
                  shuffle);

              // regionIndex = _nodeIndex;
              regionIndex = 0;

              while (ringspaceAllocated < ringspaceToAllocate - magnitudeTolerance) {
                ProtoRegion protoRegion;

                if (debug) {
                  log.debug(
                      " ringspaceAllocated {} ringspaceToAllocate {} remaining {}",
                      ringspaceAllocated,
                      ringspaceToAllocate,
                      ringspaceToAllocate - ringspaceAllocated);
                  log.debug("regionIndex {}", regionIndex);
                }
                // Find an available region
                regionIndex = prList.nextRegion(regionIndex, node, replicaIndex, rType);
                if (debug) {
                  log.debug("allocateSubPolicyMember() next regionIndex {}", regionIndex);
                }
                if (debug) {
                  quickCheckAllocations(
                      prList,
                      nodes,
                      nodeRegionSizes,
                      replicaIndex,
                      nodeIndex,
                      ringspaceToAllocate - ringspaceAllocated,
                      shuffle);
                }
                if (regionIndex < 0) {
                  if (allReplicasTag) {
                    break;
                  } else {
                    Map<String, Long> allocations;
                    allocations = prList.getAllocations();
                    for (int i = 0; i < nodes.size(); i++) {
                      long regionSize;
                      Node n;
                      long allocation;

                      n = nodes.get(i);
                      regionSize = nodeRegionSizes.get(i);
                      allocation = allocations.get(n.getIDString());
                      log.debug(
                          "{} {}  {}  {} ",
                          n.getIDString(),
                          regionSize,
                          allocation,
                          regionSize * (replicaIndex + 1) - allocation);
                    }
                    throw new RuntimeException("Couldn't find a free region");
                  }
                }

                if (debug) {
                  quickCheckAllocations(
                      prList,
                      nodes,
                      nodeRegionSizes,
                      replicaIndex,
                      nodeIndex,
                      ringspaceToAllocate - ringspaceAllocated,
                      shuffle);
                }
                // Now add this node to the region found
                protoRegion = prList.get(regionIndex);
                if (debug) {
                  log.debug("{}", protoRegion.getRegion());
                  log.debug(
                      "{} {}",
                      protoRegion.getRegion().getSize(),
                      ringspaceToAllocate - magnitudeTolerance);
                }
                if (protoRegion.getRegion().getSize()
                    <= NumUtil.addWithClamp(
                        ringspaceToAllocate - ringspaceAllocated, magnitudeTolerance)) {
                  // The candidate region size is <= ringspaceToAllocate,
                  // so we add the entire region
                  ringspaceAllocated += protoRegion.getRegion().getSize();
                  protoRegion.addOwner(rType, node);
                  if (debug)
                    quickCheckAllocations(
                        prList,
                        nodes,
                        nodeRegionSizes,
                        replicaIndex,
                        nodeIndex,
                        ringspaceToAllocate - ringspaceAllocated,
                        shuffle);
                } else {
                  // The candidate region size is > ringspaceToAllocate.
                  // Split the region and add this node to the appropriate
                  // portion.
                  if (debug) {
                    log.debug("***splitProtoRegion***");
                  }
                  if (debug)
                    quickCheckAllocations(
                        prList,
                        nodes,
                        nodeRegionSizes,
                        replicaIndex,
                        nodeIndex,
                        ringspaceToAllocate - ringspaceAllocated,
                        shuffle);
                  prList.splitProtoRegion(regionIndex, ringspaceToAllocate - ringspaceAllocated);
                  if (debug)
                    quickCheckAllocations(
                        prList,
                        nodes,
                        nodeRegionSizes,
                        replicaIndex,
                        nodeIndex,
                        ringspaceToAllocate - ringspaceAllocated,
                        shuffle);
                  // Need to get protoRegion from list again since the
                  // previous region was split
                  protoRegion = prList.get(regionIndex);
                  if (protoRegion.getRegion().getSize()
                      != ringspaceToAllocate - ringspaceAllocated) {
                    throw new RuntimeException("panic");
                  }
                  if (debug) {
                    log.debug("protoRegion {}", protoRegion);
                  }

                  // Add the entire region
                  ringspaceAllocated += protoRegion.getRegion().getSize();
                  protoRegion.addOwner(rType, node);
                  if (debug)
                    quickCheckAllocations(
                        prList,
                        nodes,
                        nodeRegionSizes,
                        replicaIndex,
                        nodeIndex,
                        ringspaceToAllocate - ringspaceAllocated,
                        shuffle);
                }

                if (debug)
                  quickCheckAllocations(
                      prList,
                      nodes,
                      nodeRegionSizes,
                      replicaIndex,
                      nodeIndex,
                      ringspaceToAllocate - ringspaceAllocated,
                      shuffle);
                if (ringspaceAllocated < ringspaceToAllocate - magnitudeTolerance) {
                  if (debug) {
                    log.debug("incrementing regionIndex {}", regionIndex);
                  }
                  regionIndex = (regionIndex + 1) % prList.size();
                }
                if (debug) {
                  log.debug("after regionIndex {}", regionIndex);
                }
              }
            }
            successful = true;
          } catch (RuntimeException re) {
            if (attemptIndex >= maxShuffleAttempts) {
              throw re;
            } else {
              log.info("Ignoring exception. Trying a new shuffle. attemptIndex {}", attemptIndex);
              prList = _prList.duplicate();
            }
          }
        }
      }
    }
    return prList;
  }

  private void addNodesToBlankProtoRegionList(ProtoRegionList prList, List<Node> nodes) {
    ProtoRegion protoRegion;

    if (prList.size() != 1) {
      throw new RuntimeException("prList.size() != 1");
    }
    protoRegion = prList.get(0);
    if (protoRegion.getOwnersSet().size() != 0) {
      throw new RuntimeException("protoRegion.getOwnersSet().size() != 0");
    }
    for (Node node : nodes) {
      protoRegion.addOwner(ReplicationType.Primary, node);
    }
  }

  private void quickCheckAllocations(
      ProtoRegionList prList,
      List<Node> nodes,
      List<Long> nodeRegionSizes,
      int replicaIndex,
      int nodeIndex,
      long ringspaceToAllocate,
      List<Integer> shuffle) {
    Map<String, Long> allocations;

    allocations = prList.getAllocations();

    if (ringspaceToAllocate < -magnitudeTolerance) {
      long regionSize;
      Node n;
      Long allocation;
      int i;

      i = shuffle.get(nodeIndex);
      n = nodes.get(i);
      regionSize = nodeRegionSizes.get(i);
      allocation = allocations.get(n.getIDString());
      if (allocation == null) {
        allocation = Long.valueOf(0);
      }
      log.debug(
          "{} {}  {}  {}  {}  {}***FAILED1\n",
          n.getIDString(),
          regionSize,
          regionSize * (replicaIndex + 1),
          allocation,
          regionSize * (replicaIndex + 1) - allocation,
          ringspaceToAllocate);
      // throw new RuntimeException("Fatal: quickCheckAllocations failed@1");
      System.exit(-1);
    }

    /*
    for (int _i = 0; _i < nodes.size(); _i++) {
        long    regionSize;
        Node    n;
        Long    allocation;
        int        i;

        i = shuffle.get(_i);
        n = nodes.get(i);
        regionSize = nodeRegionSizes.get(i);
        allocation = allocations.get(n.getIDString());

        System.out.printf("sc:\t%s\t%d\t%d\n", n.getIDString(), regionSize, allocation);

        if (allocation != null) {
            long    _ringspaceToAllocate;

            _ringspaceToAllocate = i == nodeIndex ? ringspaceToAllocate : 0;
            //System.out.printf("%s\t%d\t%d\t%d\t%d\t%d\n", n.getIDString(), regionSize, regionSize *
            (replicaIndex + 1), allocation, regionSize * (replicaIndex + 1) - allocation, _ringspaceToAllocate);
            if (_ringspaceToAllocate + allocation > regionSize * (replicaIndex + 1) + magnitudeTolerance) {
                System.out.printf("%d\t%d\t%d\n", i, nodeIndex, replicaIndex);
                System.out.printf("%s\t%d\t%d\t%d\t%d\t%d\t***FAILED\n", n.getIDString(), regionSize, regionSize
                * (replicaIndex + 1), allocation, regionSize * (replicaIndex + 1) - allocation,
                _ringspaceToAllocate);
                //throw new RuntimeException("Fatal: quickCheckAllocations failed");
                System.exit(-1);
            }
        }
    }
    */
  }

  // ///////////////////////////////////////////////////////////

  public TopologyRing reduceDataMovement(
      TopologyRing oldRing, TopologyRing newRing, RingTreeRecipe recipe) {
    SingleRing _old;
    SingleRing _new;
    SingleRing _ring;

    _old = (SingleRing) oldRing.clone(Mutability.Mutable);
    _new = (SingleRing) newRing.clone(Mutability.Mutable);
    _ring = new SingleRing(_new.getNodeClass(), _new.getVersion(), _new.getStoragePolicyName());
    while (_new.getMembers().iterator().hasNext()) {
      RingEntry nEntry;
      RingEntry oEntry;
      IntersectionResult ir;
      RingEntry shifted_nEntry;

      nEntry = _new.getMembers().iterator().next();
      oEntry = findBestRegionFor(_old, nEntry);
      shifted_nEntry = nEntry.shiftTo(oEntry.getRegion().getStart());
      ir = RingRegion.intersect(oEntry.getRegion(), shifted_nEntry.getRegion());
      switch (ir.getIntersectionType()) {
        case isomorphic:
          _ring.addEntry(shifted_nEntry);
          _old.removeEntry(oEntry);
          _new.removeEntry(nEntry);
          break;
        case aSubsumesB:
          // only one sub-case possible here
          // r1 r2 (r1 ignored in this case)
          // aaaaaa (oEntry)
          // bbbb (shifted_nEntry)
          {
            RingEntry oEntry_2;
            RingRegion r2;

            r2 =
                new RingRegion(
                    LongRingspace.nextPoint(shifted_nEntry.getRegion().getEnd()),
                    oEntry.getRegion().getEnd());
            oEntry_2 = oEntry.replaceRegion(r2);
            _ring.addEntry(shifted_nEntry);
            _old.removeEntry(oEntry);
            _new.removeEntry(nEntry);
            _old.addEntry(oEntry_2);
          }
          break;
        case bSubsumesA:
          // only one sub-case possible here
          // aaaa (oEntry)
          // bbbbbb (shifted_nEntry)
          // r1 r2
          {
            RingEntry shifted_nEntry_1;
            RingEntry shifted_nEntry_2;
            RingRegion r1;
            RingRegion r2;

            r1 = oEntry.getRegion();
            r2 =
                new RingRegion(
                    LongRingspace.nextPoint(oEntry.getRegion().getEnd()),
                    shifted_nEntry.getRegion().getEnd());
            shifted_nEntry_1 = shifted_nEntry.replaceRegion(r1);
            shifted_nEntry_2 = shifted_nEntry.replaceRegion(r2);
            _ring.addEntry(shifted_nEntry_1);
            _old.removeEntry(oEntry);
            _new.removeEntry(nEntry);
            _new.addEntry(shifted_nEntry_2);
          }
          break;
        case disjoint:
        case abPartial:
        case baPartial:
          throw new RuntimeException("shift makes these cases impossible");
        default:
          throw new RuntimeException("panic");
      }
    }
    _ring.freeze(recipe.weightSpecs);
    _ring = (SingleRing) _ring.simplify();
    _ring.freeze(recipe.weightSpecs);
    return _ring;
  }

  /**
   * Find the best RingEntry in a TopologyRing for a given RingEntry
   *
   * @param _old the TopologyRing in which to look
   * @param nEntry the entry for which to find a match
   * @return the best matching RingEntry in _old for nEntry
   */
  private RingEntry findBestRegionFor(TopologyRing _old, RingEntry nEntry) {
    RingEntry bestEntry;
    int bestDifference;

    bestEntry = null;
    bestDifference = Integer.MAX_VALUE;
    for (RingEntry oEntry : _old.getMembers()) {
      int difference;

      difference = oEntry.getNumberOfNewOwners(nEntry);
      if (difference < bestDifference) {
        bestEntry = oEntry;
        bestDifference = difference;
      } else if (difference == bestDifference) {
        long bestSizeDiff;
        long curSizeDiff;

        bestSizeDiff = Math.abs(bestEntry.getRegion().getSize() - nEntry.getRegion().getSize());
        curSizeDiff = Math.abs(oEntry.getRegion().getSize() - nEntry.getRegion().getSize());
        if (curSizeDiff < bestSizeDiff) {
          bestEntry = oEntry;
        } else if (curSizeDiff == bestSizeDiff) {
          if (bestEntry.getRegion().getStart() != nEntry.getRegion().getStart()
              && oEntry.getRegion().getStart() == nEntry.getRegion().getStart()) {
            bestEntry = oEntry;
          }
        }
      }
    }
    return bestEntry;
  }

  // //////////////////////////////////////////////////

  public static TopologyRing removeExcludedMembers(TopologyRing oldRing, Set<Node> exclusionSet)
      throws InvalidRingException {
    SingleRing _new;
    Set<RingEntry> oldEntries;

    _new = (SingleRing) oldRing.clone(Mutability.Mutable);
    oldEntries = ImmutableSet.copyOf(oldRing.getMembers());
    for (RingEntry oldEntry : oldEntries) {
      RingEntry newEntry;

      newEntry = oldEntry.removeOwnersInSet(exclusionSet);
      /*
       * if (newEntry.getPrimaryOwnersList().size() == 0) {
       * Log.info("old entry\n"+ oldEntry); throw new
       * InvalidRingException("No primary owners for: "+
       * newEntry.getRegion()); }
       */
      _new.removeEntry(oldEntry);
      _new.addEntry(newEntry);
    }
    _new.freeze(((SingleRing) oldRing).getWeightSpecifications());
    return _new;
  }

  private static Set<Node> sampleSetNodes(Set<String> nodeIDs) {
    Set<Node> sampleNodes;

    sampleNodes = new HashSet<>();
    for (String nodeID : nodeIDs) {
      sampleNodes.add(new GenericNode(NodeClass.server, nodeID));
    }
    return sampleNodes;
  }

  public static TopologyRing removeExcludedMembers(TopologyRing oldRing, ExclusionSet exclusionSet)
      throws InvalidRingException {
    return removeExcludedMembers(oldRing, sampleSetNodes(exclusionSet.getServers()));
  }

  // //////////////////////////////////////////////////

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 8) {
        log.debug(
            "<topologyFile> <weightSpecsFile> <exclusionList> <nodeID> <storagePolicyGroup> <policyID> "
                + "<HostGroupTableFile> <HostGroup,...>");
      } else {
        TopologyRingCreator topologyRingCreator;
        Topology topology;
        File topologyFile;
        File weightSpecsFile;
        File exclusionFile;
        File storagePolicyGroupFile;
        String nodeID;
        String policyID;
        // Replication replication;
        StoragePolicyGroup storagePolicyGroup;
        TopologyRing topologyRing;
        Node node;
        RingTreeRecipe recipe;
        HostGroupTable hostGroupTable;
        Set<String> hostGroups;

        // Log.setLevelAll();
        topologyFile = new File(args[0]);
        weightSpecsFile = new File(args[1]);
        exclusionFile = new File(args[2]);
        nodeID = args[3];
        storagePolicyGroupFile = new File(args[4]);
        policyID = args[5];
        storagePolicyGroup =
            new PolicyParser()
                .parsePolicyGroup(storagePolicyGroupFile, VersionedDefinition.NO_VERSION);
        topology = TopologyParser.parse(topologyFile);
        topologyRingCreator = new TopologyRingCreator();

        hostGroupTable = HostGroupTable.parse(args[6], VersionedDefinition.NO_VERSION);
        hostGroups = ImmutableSet.copyOf(args[7].split(","));

        recipe =
            new RingTreeRecipe(
                topology,
                topology.getNodeByID(nodeID),
                new WeightSpecifications(VersionedDefinition.NO_VERSION).parse(weightSpecsFile),
                ExclusionSet.parse(exclusionFile),
                storagePolicyGroup,
                policyID,
                hostGroupTable,
                hostGroups,
                0L,
                // version not
                // used for
                // this tree
                DHTUtil.currentTimeMillis());
        log.debug("RingTreeRecipe: " + recipe);
        topologyRing = topologyRingCreator.create(recipe, nodeID);
        log.debug("\n\nTopologyRing:\n");
        log.debug("{}", topologyRing);
        log.debug("");
        topologyRing = topologyRingCreator.reduceDataMovement(topologyRing, topologyRing, recipe);
        log.debug("{}", topologyRing);
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
}
