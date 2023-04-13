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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.ring.LongNavigableMapRing;
import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.util.Mutability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ring implementation.
 * Structure is immutable after constructed, but may mutate during construction.
 * State may mutate
 */
public class SingleRing extends LongNavigableMapRing<RingEntry> implements TopologyRing {
  private final long version;
  private final NodeClass nodeClass;
  private Map<String, Double> weights;
  // FUTURE - do we want the weights?
  private final String storagePolicyName;

  private static Logger log = LoggerFactory.getLogger(SingleRing.class);

  public SingleRing(NodeClass nodeClass, long version, String storagePolicyName) {
    super();
    this.nodeClass = nodeClass;
    this.version = version;
    this.storagePolicyName = storagePolicyName;
  }

  public static SingleRing emptyRing(NodeClass nodeClass, long version, String storagePolicyName) {
    SingleRing singleRing;

    singleRing = new SingleRing(nodeClass, version, storagePolicyName);
    singleRing.addEntry(RingEntry.unownedWholeRing);
    return singleRing;
  }

  @Override
  public void freeze() {
    throw new RuntimeException("specify weightSpecs for freeze");
  }

  private void _freeze(Map<String, Double> weights) {
    this.weights = weights;
    super.freeze();
  }

  //private void freeze(Map<String, Double> weights) {
  //    this.weights = weights;
  //    super.freeze();
  //}

  @Override
  public void freeze(WeightSpecifications weightSpecs) {
    Map<String, Double> _weights;

    _weights = new HashMap<>();
    for (Node node : getMemberNodes(OwnerQueryMode.All)) {
      _weights.put(node.getIDString(), weightSpecs.getWeight(node));
    }
    this.weights = ImmutableMap.copyOf(_weights);
    super.freeze();
  }

  /**
   * Create a copy of this ring.
   */
  @Override
  public SingleRing clone(Mutability mutability) {
    SingleRing newRing;

    newRing = cloneEmpty(Mutability.Mutable);
    for (RingEntry member : getMembers()) {
      newRing.addEntry(member);
    }
    if (mutability == Mutability.Immutable) {
      newRing._freeze(weights);
    }
    return newRing;
  }

  /**
   * Create a copy of this ring without any entries
   *
   * @return a copy of this ring with no entries
   */
  @Override
  public SingleRing cloneEmpty(Mutability mutability) {
    SingleRing newRing;

    ensureImmutable();
    newRing = new SingleRing(nodeClass, version, storagePolicyName);
    if (mutability == Mutability.Immutable) {
      newRing._freeze(weights);
    }
    return newRing;
  }

  public void addEntry(RingEntry entry) {
    ensureMutable();
    put(entry.getRegion().getEnd(), entry);
  }

  @Override
  public void removeEntry(RingEntry entry) {
    RingEntry oldEntry;

    ensureMutable();
    oldEntry = removeOwner(entry.getRegion().getStart());
    if (oldEntry != entry) {
      throw new RuntimeException("panic");
    }
  }

  @Override
  public NodeClass getNodeClass() {
    return nodeClass;
  }

  @Override
  public int numMemberNodes(OwnerQueryMode oqm) {
    ensureImmutable();
    return getMemberNodesSet(oqm).size();
  }

  @Override
  public Collection<Node> getMemberNodes(OwnerQueryMode oqm) {
    return getMemberNodesSet(oqm);
  }

  public Set<Node> getMemberNodesSet(OwnerQueryMode oqm) {
    ImmutableSet.Builder<Node> builder;

    builder = ImmutableSet.builder();
    for (RingEntry entry : getMembers()) {
      builder.addAll(entry.getOwnersList(oqm));
    }
    return builder.build();
  }

  @Override
  public Node getNodeByID(String nodeID) {
    // FUTURE - need more efficient implementation if used heavily
    ensureImmutable();
    for (Node node : getMemberNodes(OwnerQueryMode.All)) {
      if (node.getIDString().equals(nodeID)) {
        return node;
      }
    }
    return null;
  }

  @Override
  public boolean containsNode(Node node, OwnerQueryMode oqm) {
    ensureImmutable();
    for (Node _node : getMemberNodes(oqm)) {
      if (node.getIDString().equals(_node.getIDString())) {
        return true;
      }
    }
    return false;
  }

  public boolean isSubset(SingleRing subRing) {
    for (RingEntry subEntry : subRing.getMembers()) {
      RingEntry superEntry;

      superEntry = getOwner(subEntry.getRegion().getStart());
      if (superEntry == null) {
        return false;
      } else {
        if (!superEntry.isSubset(subEntry)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public double getWeight(String nodeID) {
    return weights.get(nodeID);
  }

  public WeightSpecifications getWeightSpecifications() {
    return new WeightSpecifications(version, weights);
  }

  @Override
  public BigDecimal getTotalWeight() {
    BigDecimal total;

    ensureImmutable();
    total = BigDecimal.ZERO;
    for (Node owner : getMemberNodes(OwnerQueryMode.All)) {
      total = total.add(new BigDecimal(weights.get(owner.getIDString())), LongRingspace.mathContext);
    }
    return total;
  }

  @Override
  public Collection<RingRegion> getNodeRegions(Node node, OwnerQueryMode oqm) {
    return getNodeRegionsFiltered(node, null, oqm);
  }

  @Override
  public Collection<RingRegion> getNodeRegionsFiltered(Node node, Node filterNode, OwnerQueryMode oqm) {
    Set<RingRegion> regionSet;

    ensureImmutable();
    regionSet = new HashSet<>();
    for (RingEntry entry : getMembers()) {
      if (entry.containsOwner(node, oqm) && (filterNode == null || !entry.containsOwner(filterNode, oqm))) {
        regionSet.add(entry.getRegion());
      }
    }
    return regionSet;
  }

  @Override
  public List<RingRegion> getNodeRegionsSorted(Node node, Comparator<RingRegion> comparator, Node filterNode,
      OwnerQueryMode oqm) {
    List<RingRegion> regionsList;

    ensureImmutable();
    regionsList = new ArrayList<>(getNodeRegionsFiltered(node, filterNode, oqm));
    Collections.sort(regionsList, comparator);
    return regionsList;
  }

  @Override
  public BigDecimal getOwnedFraction(Node owner, OwnerQueryMode oqm) {
    ensureImmutable();
    return LongRingspace.longToFraction(getOwnedRingspace(owner, oqm));
  }

  @Override
  public long getOwnedRingspace(Node owner, OwnerQueryMode oqm) {
    ensureImmutable();
    return RingRegion.getTotalSize(getNodeRegionsFiltered(owner, null, oqm));
  }

  @Override
  public String getStoragePolicyName() {
    return storagePolicyName;
  }

  @Override
  public boolean pointOwnedByNode(long point, Node node, OwnerQueryMode oqm) {
    ensureImmutable();
    return getOwner(point).containsOwner(node, oqm);
  }

  @Override
  public TopologyRing simplify() {
    List<RingEntry> ringEntriesList;
    int index;
    RingEntry curEntry;
    SingleRing simplifiedTopology;

    ensureImmutable();
    simplifiedTopology = new SingleRing(nodeClass, version, storagePolicyName);
    ringEntriesList = new ArrayList<>(getMembers());
    curEntry = ringEntriesList.get(0);
    index = 1;
    while (index < ringEntriesList.size()) {
      RingEntry nextEntry;

      nextEntry = ringEntriesList.get(index);
      if (curEntry.getRegion().isContiguousWith(nextEntry.getRegion()) && curEntry.getPrimaryOwnersSet().equals(
          nextEntry.getPrimaryOwnersSet()) && curEntry.getSecondaryOwnersSet().equals(
          nextEntry.getSecondaryOwnersSet())) {
        curEntry = curEntry.merge(nextEntry);
      } else {
        simplifiedTopology.put(curEntry.getRegion().getEnd(), curEntry);
        curEntry = nextEntry;
      }
      index++;
    }
    simplifiedTopology.put(curEntry.getRegion().getEnd(), curEntry);
    return simplifiedTopology;
  }

  /**
   * Mutates this ring to move the specified region from the oldOwner to the newOwner.
   * The specified region must be a subregion of an existing region owned by the oldOwner.
   */
  @Override
  public void releaseToNode(RingRegion region, Node oldOwner, Node newOwner) {
    RingEntry oldEntry;
    RingRegion otherRegion;
    RingRegion oldRegion;

    ensureMutable();
    log.debug("region: {}" , region);
    oldEntry = removeOwner(region.getEnd());
    oldRegion = oldEntry.getRegion();
    log.debug("oldRegion: {}" , oldRegion);
    if (region.getSize() > oldRegion.getSize()) {
      throw new RuntimeException("region.getSize() > oldRegion.getSize() " + oldRegion + " " + region);
    }
    if (oldRegion.getEnd() == region.getEnd()) {
      if (oldRegion.getStart() == region.getStart()) {
        // complete replacement
        log.debug("complete replacement");
        addEntry(oldEntry.replacePrimaryOwner(oldOwner, newOwner));
      } else {
        // region is the end of the old region
        log.debug("region is the end of the old region");
        addEntry(new RingEntry(oldEntry.getOwnersSetWithReplacement(oldOwner, newOwner, OwnerQueryMode.Primary), region,
            oldEntry.getMinPrimaryUnderFailure()));

        otherRegion = new RingRegion(oldRegion.getStart(), LongRingspace.prevPoint(region.getStart()));
        addEntry(new RingEntry(oldEntry.getPrimaryOwnersList(), otherRegion, oldEntry.getMinPrimaryUnderFailure()));
      }
    } else {
      if (oldRegion.getStart() == region.getStart()) {
        // region is the start of the old region
        log.debug("region is the start of the old region");
        otherRegion = new RingRegion(LongRingspace.nextPoint(region.getEnd()), oldRegion.getEnd());
        addEntry(new RingEntry(oldEntry.getPrimaryOwnersList(), otherRegion, oldEntry.getMinPrimaryUnderFailure()));

        addEntry(new RingEntry(oldEntry.getOwnersSetWithReplacement(oldOwner, newOwner, OwnerQueryMode.Primary), region,
            oldEntry.getMinPrimaryUnderFailure()));
      } else {
        throw new RuntimeException("invalid regions: " + oldRegion + "\t" + region);
      }
    }
  }

  @Override
  public long getVersion() {
    return version;
  }

  public Collection<RingRegion> getRegions() {
    return getRegionsSorted();
  }

  public List<RingRegion> getRegionsSorted() {
    List<RingRegion> regions;

    regions = new ArrayList<>();
    for (RingEntry entry : getMembers()) {
      regions.add(entry.getRegion());
    }
    Collections.sort(regions, RingRegion.positionComparator);
    return regions;
  }
    
    /*
    public void setPrimaryStoragePolicy(String nodeID, String storagePolicyName) {
        setStoragePolicy(primaryStoragePolicyNames, nodeID, storagePolicyName);
    }

    public void setSecondaryStoragePolicy(String nodeID, String storagePolicyName) {
        setStoragePolicy(secondaryStoragePolicyNames, nodeID, storagePolicyName);
    }

    private final Map<String,String>   primaryStoragePolicyNames;
    private final Map<String,String>   secondaryStoragePolicyNames;
    private void setStoragePolicy(Map<String,String> map, String nodeID, String storagePolicyName) {
        String  existingPolicy;
        
        existingPolicy = map.get(storagePolicyName);
        if (existingPolicy != null && !existingPolicy.equals(storagePolicyName)) {
            throw new RuntimeException("Multiple storage policies used for nodeID: "+ existingPolicy +" "+
            storagePolicyName);
        }
        map.put(nodeID, storagePolicyName);
    }

    @Override
    public String getPrimaryStoragePolicy(String nodeID) {
        return primaryStoragePolicyNames.get(nodeID);
    }

    @Override
    public String getSecondaryStoragePolicy(String nodeID) {
        return secondaryStoragePolicyNames.get(nodeID);
    }
    */

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(super.toString());
    sb.append('\n');
    for (Node owner : getMemberNodes(OwnerQueryMode.All)) {
      sb.append(owner);
      sb.append('\t');
      sb.append(getOwnedFraction(owner, OwnerQueryMode.All));
      sb.append('\n');
    }
    return sb.toString();
  }
}
