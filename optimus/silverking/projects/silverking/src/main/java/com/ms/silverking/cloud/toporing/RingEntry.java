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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.DHTNodePort;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Associates a RingRegion with one or more Node primary owners, and zero or more secondary owners.
 */
public class RingEntry {
  private final List<Node> primaryOwners;
  private final List<Node> secondaryOwners;
  private final RingRegion region;
  private final int minPrimaryUnderFailure;

  private static Logger log = LoggerFactory.getLogger(RingEntry.class);

  private static final String primarySecondarySeparator = "::";
  private static final int defaultMinPrimaryUnderFailureIndicator = -1;

  private static final List<Node> emptyNodeList = ImmutableList.of();

  public static final Comparator<RingEntry> positionComparator = new RingEntryPositionComparator();
  public static RingEntry unownedWholeRing =
      new RingEntry(emptyNodeList, emptyNodeList, RingRegion.allRingspace, 0);

  public RingEntry(
      Collection<Node> primaryOwners,
      Collection<Node> secondaryOwners,
      RingRegion region,
      int minPrimaryUnderFailure) {
    this.primaryOwners = ImmutableList.copyOf(primaryOwners);
    this.secondaryOwners = ImmutableList.copyOf(secondaryOwners);
    this.region = region;
    if (minPrimaryUnderFailure < defaultMinPrimaryUnderFailureIndicator
        || minPrimaryUnderFailure > primaryOwners.size()) {
      throw new RuntimeException(
          "Bad minPrimaryUnderFailure: " + minPrimaryUnderFailure + " " + primaryOwners.size());
    } else {
      this.minPrimaryUnderFailure = minPrimaryUnderFailure;
    }
    // assert no duplicates
    assert primaryOwners.size() == getPrimaryOwnersSet().size();
    assert secondaryOwners.size() == getSecondaryOwnersSet().size();
  }

  public RingEntry(Collection<Node> primaryOwners, RingRegion region, int minPrimaryUnderFailure) {
    this(new ArrayList<Node>(primaryOwners), emptyNodeList, region, minPrimaryUnderFailure);
  }

  public List<IPAndPort> getOwnersIPList(OwnerQueryMode oqm) {
    return ImmutableList.copyOf(nodesToIPAndPort(getOwnersList(oqm)));
  }

  private static IPAndPort[] nodesToIPAndPort(List<Node> replicaNodes) {
    IPAndPort[] replicas;

    replicas = new IPAndPort[replicaNodes.size()];
    for (int i = 0; i < replicas.length; i++) {
      replicas[i] = new IPAndPort(replicaNodes.get(i).getIDString(), DHTNodePort.getDhtPort());
    }
    return replicas;
  }

  public List<Node> getPrimaryOwnersList() {
    return primaryOwners;
  }

  public Set<Node> getPrimaryOwnersSet() {
    return ImmutableSet.copyOf(primaryOwners);
  }

  public List<Node> getSecondaryOwnersList() {
    return secondaryOwners;
  }

  public Set<Node> getSecondaryOwnersSet() {
    return ImmutableSet.copyOf(secondaryOwners);
  }

  public List<Node> getOwnersList(OwnerQueryMode oqm) {
    switch (oqm) {
      case Primary:
        return primaryOwners;
      case Secondary:
        return secondaryOwners;
      case All:
        ImmutableList.Builder<Node> builder;

        builder = ImmutableList.builder();
        return builder.addAll(primaryOwners).addAll(secondaryOwners).build();
      default:
        throw new RuntimeException("panic");
    }
  }

  public Set<Node> getOwnersSet(OwnerQueryMode oqm) {
    switch (oqm) {
      case Primary:
        return ImmutableSet.copyOf(primaryOwners);
      case Secondary:
        return ImmutableSet.copyOf(secondaryOwners);
      case All:
        return new ImmutableSet.Builder().addAll(primaryOwners).addAll(secondaryOwners).build();
      default:
        throw new RuntimeException("panic");
    }
  }

  public Set<Node> getOwnersSetWithReplacement(Node oldOwner, Node newOwner, OwnerQueryMode oqm) {
    Set<Node> ownersSet;

    ownersSet = getOwnersSet(oqm);
    if (!ownersSet.remove(oldOwner)) {
      throw new RuntimeException("Not an owner: " + oldOwner);
    }
    ownersSet.add(newOwner);
    return ownersSet;
  }

  public Node getPrimaryOwner(int index) {
    return primaryOwners.get(index);
  }

  public RingRegion getRegion() {
    return region;
  }

  public int getMinPrimaryUnderFailure() {
    if (minPrimaryUnderFailure == defaultMinPrimaryUnderFailureIndicator) {
      return DHTConstants.defaultMinPrimaryUnderFailure;
    } else {
      return minPrimaryUnderFailure;
    }
  }

  public boolean isSubset(RingEntry o) {
    return region.equals(o.region)
        && nodesAreSubset(primaryOwners, o.primaryOwners)
        && nodesAreSubset(secondaryOwners, o.secondaryOwners)
        && minPrimaryUnderFailure == o.minPrimaryUnderFailure;
  }

  private boolean nodesAreSubset(List<Node> nodes0, List<Node> nodes1) {
    for (Node n : nodes1) {
      if (!nodes0.contains(n)) {
        return false;
      }
    }
    return true;
  }

  public RingEntry replacePrimaryOwner(Node oldOwner, Node newOwner) {
    int index;

    index = primaryOwners.indexOf(oldOwner);
    if (index < 0) {
      throw new RuntimeException("No such owner: " + oldOwner + " " + this);
    } else {
      return replacePrimaryOwner(index, newOwner);
    }
  }

  public RingEntry replacePrimaryOwner(int index, Node newOwner) {
    List<Node> newOwners;

    newOwners = new ArrayList<Node>(primaryOwners.size());
    if (index < 0 || index >= primaryOwners.size()) {
      throw new IndexOutOfBoundsException("bad index: " + index);
    }
    for (int i = 0; i < primaryOwners.size(); i++) {
      if (i == index) {
        newOwners.add(newOwner);
      } else {
        if (primaryOwners.get(i).equals(newOwner)) {
          throw new RuntimeException("Attempted multiple additions: " + newOwner);
        }
        newOwners.add(primaryOwners.get(i));
      }
    }
    return new RingEntry(newOwners, region, minPrimaryUnderFailure);
  }

  public RingEntry merge(RingEntry oEntry) {
    if (!region.isContiguousWith(oEntry.region)) {
      throw new RuntimeException("Can't merge entries with non-contiguous regions");
    } else if (!getPrimaryOwnersSet().equals(oEntry.getPrimaryOwnersSet())) {
      throw new RuntimeException("Can't merge entries with non-equal owners");
    } else {
      RingRegion mergedRegion;

      mergedRegion = region.merge(oEntry.region);
      return new RingEntry(primaryOwners, mergedRegion, minPrimaryUnderFailure);
    }
  }

  public boolean containsOwner(Node owner, OwnerQueryMode oqm) {
    return (oqm.includePrimary() && (primaryOwners.indexOf(owner)) >= 0)
        || (oqm.includeSecondary() && (secondaryOwners.indexOf(owner)) >= 0);
  }

  public RingEntry replaceRegion(Collection<RingRegion> newRegionCollection) {
    if (newRegionCollection.size() != 1) {
      throw new RuntimeException("Invalid region collection cardinality");
    }
    return replaceRegion(newRegionCollection.iterator().next());
  }

  public RingEntry replaceRegion(RingRegion newRegion) {
    return new RingEntry(primaryOwners, secondaryOwners, newRegion, minPrimaryUnderFailure);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append('[');
    sb.append(region);
    sb.append(':');
    sb.append(getZKString(primaryOwners));
    sb.append(primarySecondarySeparator);
    sb.append(getZKString(secondaryOwners));
    if (minPrimaryUnderFailure != defaultMinPrimaryUnderFailureIndicator) {
      sb.append(primarySecondarySeparator);
      sb.append(minPrimaryUnderFailure);
    }
    sb.append(']');
    return sb.toString();
  }

  public String getPrimaryOwnersZKString() {
    return getZKString(primaryOwners);
  }

  public String getSecondaryOwnersZKString() {
    return getZKString(secondaryOwners);
  }

  public String getZKString(List<Node> owners) {
    StringBuilder sb;

    sb = new StringBuilder();
    for (int i = 0; i < owners.size(); i++) {
      sb.append(owners.get(i).getIDString());
      if (i < owners.size() - 1) {
        sb.append(',');
      }
    }
    return sb.toString();
  }

  public static List<Node> parsePrimaryOwnersString(Topology topology, String s) {
    String[] splitS;

    splitS = s.split(primarySecondarySeparator);
    if (splitS.length > 0) {
      return parseOwnersString(topology, splitS[0]);
    } else {
      // throw new RuntimeException("Unexpected bad primary owners string: "+ s);
      // Manager mode ring creation may create empty entries at some levels of the topology
      // we allow this. This requires verifying the projected ring to ensure that no
      // projected entries are without primary owners.
      return emptyNodeList;
    }
  }

  public static List<Node> parseSecondaryOwnersString(Topology topology, String s) {
    String[] splitS;

    splitS = s.split(primarySecondarySeparator);
    if (splitS.length >= 2) {
      return parseOwnersString(topology, splitS[1]);
    } else {
      return emptyNodeList;
    }
  }

  private static List<Node> parseOwnersString(Topology topology, String s) {
    String[] tokens;
    List<Node> nodes;

    tokens = s.split(",");
    nodes = new ArrayList<>(tokens.length);
    for (String token : tokens) {
      nodes.add(topology.getNodeByID(token));
    }
    return nodes;
  }

  public static int parseMinPrimaryUnderFailure(String s) {
    String[] splitS;

    splitS = s.split(primarySecondarySeparator);
    if (splitS.length >= 3) {
      return Integer.parseInt(splitS[2]);
    } else {
      return defaultMinPrimaryUnderFailureIndicator;
    }
  }

  public static RingEntry parseZKDefs(Topology topology, String regionDef, String ownersDef) {
    return new RingEntry(
        parsePrimaryOwnersString(topology, ownersDef),
        parseSecondaryOwnersString(topology, ownersDef),
        RingRegion.parseZKString(regionDef),
        parseMinPrimaryUnderFailure(ownersDef));
  }

  public RingEntry addOwners(RingEntry oEntry) {
    Set<Node> newPrimaryOwners;
    Set<Node> newSecondaryOwners;

    newPrimaryOwners = new HashSet<>();
    newSecondaryOwners = new HashSet<>();
    newPrimaryOwners.addAll(primaryOwners);
    newSecondaryOwners.addAll(secondaryOwners);
    newPrimaryOwners.addAll(oEntry.getPrimaryOwnersList());
    newSecondaryOwners.addAll(oEntry.getSecondaryOwnersList());
    return new RingEntry(newPrimaryOwners, newSecondaryOwners, region, minPrimaryUnderFailure);
  }

  public static List<RingRegion> getRegions(Collection<RingEntry> entries) {
    List<RingRegion> regions;

    regions = new ArrayList<>(entries.size());
    for (RingEntry entry : entries) {
      regions.add(entry.getRegion());
    }
    return regions;
  }

  public static void ensureEntryRegionsDisjoint(Collection<RingEntry> entries) {
    if (!entryRegionsDisjoint(entries)) {
      throw new RuntimeException("Regions not disjoint");
    }
  }

  public static boolean entryRegionsDisjoint(Collection<RingEntry> entries) {
    if (entries.size() > 1) {
      List<RingRegion> regions;

      regions = getRegions(entries);
      if (entries.size() > 2) {
        Collections.sort(regions, RingRegion.positionComparator);
      }
      for (int i = 0; i < regions.size(); i++) {
        int j;

        j = (i + 1) % regions.size();
        if (regions.get(i).overlaps(regions.get(j))) {
          return false;
        }
      }
      return true;
    } else {
      return true;
    }
  }

  public static void ensureMinPrimaryUnderFailureMet(Collection<RingEntry> entries) {
    if (!minPrimaryUnderFailureMet(entries)) {
      throw new RuntimeException("minPrimaryUnderFailure not met");
    }
  }

  private static boolean minPrimaryUnderFailureMet(Collection<RingEntry> entries) {
    boolean met;

    met = true;
    for (RingEntry entry : entries) {
      if (!entry.minPrimaryUnderFailureMet()) {
        // Note that we check all even if we find a single bad
        met = false;
        log.info("minPrimaryUnderFailureNotMet: {}", entry);
      }
    }
    return met;
  }

  private boolean minPrimaryUnderFailureMet() {
    return primaryOwners.size() >= minPrimaryUnderFailure;
  }

  public static String toString(List<RingEntry> entries) {
    return toString(entries, null);
  }

  public static String toString(List<RingEntry> entries, String separator) {
    StringBuilder sb;

    sb = new StringBuilder();
    for (RingEntry entry : entries) {
      sb.append(entry.toString());
      if (separator != null) {
        sb.append(separator);
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    RingEntry oEntry;

    oEntry = (RingEntry) other;
    return region.equals(oEntry.region)
        && getPrimaryOwnersSet().equals(oEntry.getPrimaryOwnersSet())
        && getSecondaryOwnersSet().equals(oEntry.getSecondaryOwnersSet())
        && this.minPrimaryUnderFailure == oEntry.minPrimaryUnderFailure;
  }

  public static List<RingEntry> eliminateDuplicates(List<RingEntry> entries) {
    List<RingEntry> simplified;

    simplified = new ArrayList<>(entries.size());
    for (RingEntry entry : entries) {
      if (!simplified.contains(entry)) {
        simplified.add(entry);
      }
    }
    return simplified;
  }

  public static List<RingEntry> simplify(List<RingEntry> raw) {
    List<RingEntry> simplified;
    int index;
    RingEntry curEntry;

    // System.out.println("raw\t"+ raw);
    simplified = new ArrayList<>();
    Collections.sort(simplified, positionComparator);
    curEntry = raw.get(0);
    index = 1;
    while (index < raw.size()) {
      RingEntry nextEntry;

      nextEntry = raw.get(index);
      // System.out.println(curEntry);
      // System.out.println(nextEntry);
      // System.out.println(curEntry.getRegion().isContiguousWith(nextEntry.getRegion()));
      // System.out.println(curEntry.getPrimaryOwnersSet().equals(nextEntry.getPrimaryOwnersSet()));
      if (curEntry.getRegion().isContiguousWith(nextEntry.getRegion())
          && curEntry.getPrimaryOwnersSet().equals(nextEntry.getPrimaryOwnersSet())
          && curEntry.getSecondaryOwnersSet().equals(nextEntry.getSecondaryOwnersSet())) {
        curEntry = curEntry.merge(nextEntry);
      } else {
        simplified.add(curEntry);
        curEntry = nextEntry;
      }
      index++;
    }
    simplified.add(curEntry);
    // System.out.println("simplified\t"+ simplified);
    return simplified;
  }

  public RingEntry convertPrimaryToSecondary() {
    List<Node> _secondary;

    _secondary = new ArrayList<>(primaryOwners.size() + secondaryOwners.size());
    _secondary.addAll(primaryOwners);
    _secondary.addAll(secondaryOwners);
    return new RingEntry(emptyNodeList, _secondary, region, minPrimaryUnderFailure);
  }

  private static List<Node> removeOwnersNotInSet(List<Node> old, Set<Node> includeNodes) {
    ImmutableList.Builder<Node> builder;

    builder = ImmutableList.builder();
    for (Node node : old) {
      if (includeNodes.contains(node)) {
        builder.add(node);
      }
    }
    return builder.build();
  }

  public RingEntry removeOwnersNotInSet(Set<Node> includeNodes) {
    List<Node> _p;
    List<Node> _s;

    _p = removeOwnersNotInSet(primaryOwners, includeNodes);
    _s = removeOwnersNotInSet(secondaryOwners, includeNodes);
    return new RingEntry(_p, _s, region, minPrimaryUnderFailure);
  }

  public RingEntry removeOwnersInSet(Set<Node> excludeNodes) {
    Set<Node> includeNodes;
    List<Node> _p;
    List<Node> _s;

    includeNodes = new HashSet<>(getOwnersSet(OwnerQueryMode.All));
    includeNodes.removeAll(excludeNodes);
    _p = removeOwnersNotInSet(primaryOwners, includeNodes);
    _s = removeOwnersNotInSet(secondaryOwners, includeNodes);
    return new RingEntry(_p, _s, region, minPrimaryUnderFailure);
  }

  public int getNumberOfNewOwners(RingEntry newEntry) {
    Set<Node> oldOwners;
    Set<Node> newOwners;

    oldOwners = getOwnersSet(OwnerQueryMode.All);
    newOwners = new HashSet<>(newEntry.getOwnersSet(OwnerQueryMode.All));
    newOwners.removeAll(oldOwners);
    return newOwners.size();
  }

  public RingEntry shiftTo(long newStart) {
    return replaceRegion(region.shiftTo(newStart));
  }
}
