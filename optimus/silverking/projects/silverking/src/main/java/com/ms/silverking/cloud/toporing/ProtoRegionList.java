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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** List of ProtoRegions with convenience functions for creation and manipulation. */
class ProtoRegionList {
  private final List<ProtoRegion> protoRegions;

  private static Logger log = LoggerFactory.getLogger(ProtoRegionList.class);

  private static final int minPrimaryUnderFailure =
      1; // TODO (OPTIMUS-0000): temporarily hardcoded until minPrimaryUnderFailure
  // code is complete

  private static final boolean debug = true;

  private ProtoRegionList(List<ProtoRegion> protoRegions) {
    this.protoRegions = protoRegions;
  }

  ProtoRegionList duplicate() {
    List<ProtoRegion> protoRegions2;

    protoRegions2 = new ArrayList<>(protoRegions.size());
    for (ProtoRegion pr : protoRegions) {
      protoRegions2.add(pr.duplicate());
    }
    return new ProtoRegionList(protoRegions2);
  }

  ProtoRegion get(int index) {
    return protoRegions.get(index);
  }

  List<ProtoRegion> getRegionList() {
    return protoRegions;
  }

  Set<Node> getOwners() {
    ImmutableSet.Builder<Node> owners;

    owners = ImmutableSet.builder();
    owners.addAll(getOwners(ReplicationType.Primary));
    owners.addAll(getOwners(ReplicationType.Secondary));
    return owners.build();
  }

  Set<Node> getOwners(ReplicationType rType) {
    ImmutableSet.Builder<Node> owners;

    owners = ImmutableSet.builder();
    for (ProtoRegion region : protoRegions) {
      owners.addAll(region.getOwners(rType));
    }
    return owners.build();
  }

  double getOwnedFraction(Node owner) {
    BigDecimal totalOwned;

    totalOwned = BigDecimal.ZERO;
    for (ProtoRegion region : protoRegions) {
      if (region.contains(owner)) {
        totalOwned = totalOwned.add(region.getRegion().getSizeBD(), LongRingspace.mathContext);
      }
    }
    return totalOwned.divide(LongRingspace.bdSize, LongRingspace.mathContext).doubleValue();
  }

  boolean isEmpty() {
    return protoRegions.size() == 0;
  }

  int size() {
    return protoRegions.size();
  }

  long getFirstRegionStart() {
    return protoRegions.get(0).getRegion().getStart();
  }

  static ProtoRegionList createEmpty() {
    List<ProtoRegion> protoRegions;

    protoRegions = new ArrayList<>();
    protoRegions.add(new ProtoRegion(RingRegion.allRingspace, minPrimaryUnderFailure));
    return new ProtoRegionList(protoRegions);
  }

  static ProtoRegionList createIsomorphic(SingleRing sourceRing) {
    List<RingRegion> sourceRegions;
    List<ProtoRegion> protoRegions;

    Preconditions.checkNotNull(sourceRing);
    sourceRegions = sourceRing.getRegionsSorted();
    protoRegions = new ArrayList<>(sourceRegions.size());
    for (RingRegion sourceRegion : sourceRegions) {
      protoRegions.add(new ProtoRegion(sourceRegion, minPrimaryUnderFailure));
    }
    return new ProtoRegionList(protoRegions);
  }

  /**
   * Create a ProtoRegionList from the given nodes weighted according to the weightSpecs
   *
   * @param nodes
   * @param weightSpecs
   * @return
   */
  static ProtoRegionList create(List<Node> nodes, WeightSpecifications weightSpecs) {
    List<ProtoRegion> protoRegions;
    List<RingRegion> regions;

    protoRegions = new ArrayList<>();
    regions = RingRegion.allRingspace.divide(weightSpecs.getWeights(nodes));
    for (RingRegion region : regions) {
      protoRegions.add(new ProtoRegion(region, minPrimaryUnderFailure));
    }
    return new ProtoRegionList(protoRegions);
  }

  // static ProtoRegionList create(Collection<Node> nodes, WeightSpecifications weightSpecs) {
  //    return create(ImmutableList.copyOf(nodes), weightSpecs);
  // }

  /**
   * Find next region that does not contain the given node
   *
   * @param protoRegions
   * @param startIndex
   * @param node
   * @param replicaIndex
   * @return
   */
  int nextRegion(int startIndex, Node node, int replicaIndex, ReplicationType rType) {
    int index;
    int regionsExamined;

    if (debug) {
      System.out.printf(
          "nextRegion. startIndex %d node %s replicaIndex %d rType %s\n",
          startIndex, node.getIDString(), replicaIndex, rType);
    }
    assert startIndex >= 0;
    assert replicaIndex >= 0;
    assert node != null;
    assert rType != null;
    regionsExamined = 0;
    index = startIndex;
    while (regionsExamined < protoRegions.size()
        && (protoRegions.get(index).contains(node)
            || protoRegions.get(index).totalOwners(rType) > replicaIndex)) {
      if (debug) {
        log.debug("{} {}", index, protoRegions.get(index));
      }
      index = (index + 1) % protoRegions.size();
      regionsExamined++;
    }
    if (!protoRegions.get(index).contains(node)
        && protoRegions.get(index).totalOwners(rType) <= replicaIndex) {
      if (debug) {
        log.debug("nextRegion. index {}", index);
      }
      return index;
    } else {
      if (debug) {
        log.debug("nextRegion. Couldn't find region without node: {}", node.getIDString());
      }
      return -1;
    }
  }

  /**
   * Find next region that does not contain the given node
   *
   * @param protoRegions
   * @param startIndex
   * @param node
   * @param replicaIndex
   * @return
   */
  int nextRegion2(int startIndex, Node node, int maxOwners, ReplicationType rType) {
    int index;
    int regionsExamined;

    if (debug) {
      log.debug(
          "nextRegion. startIndex {} node {} rType {}", startIndex, node.getIDString(), rType);
    }
    assert startIndex >= 0;
    assert maxOwners > 0;
    assert node != null;
    assert rType != null;
    regionsExamined = 0;
    index = startIndex;
    while (regionsExamined < protoRegions.size()
        && (protoRegions.get(index).contains(node)
            || protoRegions.get(index).totalOwners(rType) >= maxOwners)) {
      if (debug) {
        log.debug("{} {}", index, protoRegions.get(index));
      }
      index = (index + 1) % protoRegions.size();
      regionsExamined++;
    }
    if (!protoRegions.get(index).contains(node)
        && protoRegions.get(index).totalOwners(rType) < maxOwners) {
      if (debug) {
        log.debug("nextRegion. index {}", index);
      }
      return index;
    } else {
      if (debug) {
        log.debug("nextRegion. Couldn't find region without node: {}", node.getIDString());
      }
      return -1;
    }
  }

  void mergeResidualRegions(long sizeThreshold) {
    int i;

    i = protoRegions.size() - 1;
    while (i >= 0) {
      ProtoRegion r0;
      int insertionIndex;

      r0 = protoRegions.get(i);
      if (r0.getRegionSize() < sizeThreshold) {
        ProtoRegion r1;
        ProtoRegion merged;

        if (i > 0) {
          protoRegions.remove(i);
          r1 = protoRegions.remove(i - 1);
          insertionIndex = i - 1;
        } else {
          if (protoRegions.size() <= 1) {
            // Can't have a single small region
            throw new RuntimeException(
                "mergeResidualRegions() called on an incomplete region list");
          } else {
            r1 = protoRegions.remove(1);
            insertionIndex = 0;
            protoRegions.remove(0);
          }
        }
        merged =
            new ProtoRegion(
                r0.getRegion().merge(r1.getRegion()), r1.getOwners(), minPrimaryUnderFailure);
        protoRegions.add(insertionIndex, merged);
      }
      i--;
    }
  }

  /**
   * Split the given ProtoRegion at the given point
   *
   * @param index
   * @param splitSize
   */
  void splitProtoRegion(int index, long splitSize) {
    if (splitSize < 0 || splitSize > protoRegions.get(index).getRegion().getSize()) {
      throw new RuntimeException("illegal splitSize");
    } else {
      if (splitSize == protoRegions.get(index).getRegion().getSize()) {
        // no split needed
      } else {
        RingRegion[] splitRegions;
        ProtoRegion oldProtoRegion;

        oldProtoRegion = protoRegions.get(index);
        splitRegions = oldProtoRegion.getRegion().split(splitSize);
        protoRegions.remove(index);
        protoRegions.add(
            index,
            new ProtoRegion(
                splitRegions[0], oldProtoRegion.getOwners().duplicate(), minPrimaryUnderFailure));
        protoRegions.add(
            index + 1,
            new ProtoRegion(
                splitRegions[1], oldProtoRegion.getOwners().duplicate(), minPrimaryUnderFailure));
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (ProtoRegion pr : protoRegions) {
      sb.append(pr);
      sb.append("\n");
    }
    sb.append("\n");
    for (Node owner : getOwners()) {
      sb.append(owner + ":\t" + getOwnedFraction(owner));
      sb.append("\n");
    }
    sb.append("\n");
    return sb.toString();
  }

  static ProtoRegionList merge(ProtoRegionList l0, ProtoRegionList l1) {
    log.info("merge");
    log.info("{}", l0);
    log.info("{}", l1);
    if (l0 == null) {
      return l1;
    } else if (l1 == null) {
      return l0;
    } else {
      List<ProtoRegion> merged;
      int i0;
      int i1;

      merged = new ArrayList<>();
      if (l0.getFirstRegionStart() != l1.getFirstRegionStart()) {
        throw new RuntimeException("list region start mismatch");
      }
      i0 = 0;
      i1 = 0;
      while (i0 < l0.size() || i1 < l1.size()) {
        if (i0 > l0.size()) {
          merged.add(l1.get(i1).duplicate());
          i1++;
        } else if (i1 > l1.size()) {
          merged.add(l0.get(i0).duplicate());
          i0++;
        } else {
          ProtoRegion pr0;
          ProtoRegion pr1;
          RingRegion rr0;
          RingRegion rr1;
          ProtoRegion intersection;

          // determine splitEnd
          pr0 = l0.get(i0);
          pr1 = l1.get(i1);
          rr0 = pr0.getRegion();
          rr1 = pr1.getRegion();
          intersection = ProtoRegion.intersect(pr0, pr1);
          merged.add(intersection);
          if (intersection.getRegion().getEnd() == rr0.getEnd()) {
            i0++;
          }
          if (intersection.getRegion().getEnd() == rr1.getEnd()) {
            i1++;
          }
        }
      }
      return new ProtoRegionList(merged);
    }
  }

  public SingleRing toSingleRing(NodeClass nodeClass, RingTreeRecipe recipe) {
    SingleRing ring;

    ring =
        new SingleRing(
            nodeClass,
            0,
            recipe.storagePolicy.getName()); // TODO (OPTIMUS-0000): version of zero here
    for (ProtoRegion pr : getRegionList()) {
      ring.put(
          pr.getRegion().getEnd(),
          new RingEntry(
              pr.getPrimaryOwners(),
              pr.getSecondaryOwners(),
              pr.getRegion(),
              minPrimaryUnderFailure));
    }
    ring.freeze(recipe.weightSpecs);
    RingEntry.ensureEntryRegionsDisjoint(ring.getMembers());
    return ring;
  }

  public Map<String, Long> getAllocations() {
    Map<String, Long> allocations;

    allocations = new HashMap<>();
    for (ProtoRegion pr : protoRegions) {
      for (Node node : pr.getPrimaryOwners()) {
        Long allocation;

        allocation = allocations.get(node.getIDString());
        if (allocation == null) {
          allocation = new Long(0);
        }
        allocation += pr.getRegionSize();
        allocations.put(node.getIDString(), allocation);
      }
    }
    return allocations;
  }
}
