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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedReplicaMap {
  private final NavigableMap<Long, RingEntry> entryMap;
  private final NavigableMap<Long, MapEntry> replicaMap;
  private final HashedListMap<IPAndPort, RingEntry>[] replicaEntries;
  private final List<RingRegion> regions;
  private Set<IPAndPort> allReplicas;
  private final Comparator<IPAndPort> replicaPrioritizer;

  private static Logger log = LoggerFactory.getLogger(ResolvedReplicaMap.class);

  private static final int initialReplicaListSize = 4; // used to reduce the size of some ArrayLists

  private static int dhtPort; // FUTURE - eliminate

  public static void setDHTPort(int _dhtPort) {
    synchronized (ResolvedReplicaMap.class) {
      if (dhtPort != 0 && dhtPort != _dhtPort) {
        throw new RuntimeException("Attempted to reset dhtPort");
      } else {
        dhtPort = _dhtPort;
      }
    }
  }

  public ResolvedReplicaMap(Comparator<IPAndPort> replicaPrioritizer) {
    this.replicaPrioritizer = replicaPrioritizer;
    entryMap = new TreeMap<>();
    replicaMap = new TreeMap<>();
    replicaEntries = new HashedListMap[EnumValues.ownerQueryMode.length];
    regions = new ArrayList<>();
    for (OwnerQueryMode oqm : EnumValues.ownerQueryMode) {
      replicaEntries[oqm.ordinal()] = new HashedListMap<>();
    }
  }

  public ResolvedReplicaMap() {
    this(null);
  }

  public Set<RingEntry> getEntries() {
    ImmutableSet.Builder<RingEntry> entriesBuilder;

    entriesBuilder = ImmutableSet.builder();
    for (HashedListMap<IPAndPort, RingEntry> entryMap : replicaEntries) {
      entriesBuilder.addAll(entryMap.valueSet());
    }
    return entriesBuilder.build();
  }

  public Set<Set<IPAndPort>> getReplicaSets() {
    Set<Set<IPAndPort>> replicaSets;

    replicaSets = new HashSet<>();
    for (MapEntry mapEntry : replicaMap.values()) {
      replicaSets.add(mapEntry.getReplicaSet(OwnerQueryMode.Primary));
    }
    return replicaSets;
  }

  public void addEntry(RingEntry entry) {
    MapEntry mapEntry;

    mapEntry = new MapEntry(entry);
    if (replicaMap.get(entry.getRegion().getStart()) != null) {
      throw new RuntimeException("Duplicate regions");
    }
    entryMap.put(entry.getRegion().getStart(), entry);
    replicaMap.put(entry.getRegion().getStart(), mapEntry);
    regions.add(entry.getRegion());

    for (OwnerQueryMode oqm : EnumValues.ownerQueryMode) {
      addReplicaEntries(replicaEntries[oqm.ordinal()], entry, mapEntry.getReplicaList(oqm));
    }
  }

  private void addReplicaEntries(HashedListMap<IPAndPort, RingEntry> replicaEntries, RingEntry entry,
      Collection<IPAndPort> replicas) {
    for (IPAndPort replica : replicas) {
      replicaEntries.addValue(replica, entry);
    }
  }

  public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm) {
    return getEntry(key).getReplicas(oqm);
  }

  public List<IPAndPort> getReplicaList(DHTKey key, OwnerQueryMode oqm) {
    return getEntry(key).getReplicaList(oqm);
  }

  public Set<IPAndPort> getReplicaSet(DHTKey key, OwnerQueryMode oqm) {
    return getEntry(key).getReplicaSet(oqm);
  }

  public PrimarySecondaryIPListPair getReplicaListPair(DHTKey key) {
    return getEntry(key).getIPListPair();
  }

  private MapEntry getEntry(DHTKey key) {
    return replicaMap.floorEntry(KeyUtil.keyToCoordinate(key)).getValue();
  }

  public List<RingEntry> getReplicaEntries(IPAndPort replica, OwnerQueryMode oqm) {
    return replicaEntries[oqm.ordinal()].getList(replica);
  }

  public Set<IPAndPort> getOwners(RingRegion foreignRegion, OwnerQueryMode oqm) {
    ImmutableSet.Builder<IPAndPort> owners;
    List<RingEntry> entries;

    entries = getEntries(foreignRegion.getStart(), foreignRegion.getEnd());
    owners = ImmutableSet.builder();
    for (RingEntry entry : entries) {
      owners.add(nodesToIPAndPort(entry.getOwnersList(oqm)));
    }
    return owners.build();
  }

  public List<RingEntry> getEntries(RingRegion foreignRegion) {
    return getEntries(foreignRegion.getStart(), foreignRegion.getEnd());
  }

  /**
   * Return entries in the range. The returned entries may cover more than the range specified,
   * but will at least cover the range specified.
   *
   * @param minCoordinate
   * @param maxCoordinate
   * @return entries in the given range
   */
  public List<RingEntry> getEntries(long minCoordinate, long maxCoordinate) {
    List<RingEntry> owners;
    SortedMap<Long, RingEntry> intervalMap;
    Map.Entry<Long, RingEntry> entry;

    owners = new ArrayList<>(initialReplicaListSize);
    // Create a map for the purposes of iterating through entries in the given range
    // Exclude the minCoordinate as it is handled below
    intervalMap = entryMap.subMap(minCoordinate, false, maxCoordinate, true);
    for (RingEntry owner : intervalMap.values()) {
      owners.add(owner);
    }
    // Now handle the minCoordinate by finding the entry that contains it
    entry = entryMap.floorEntry(minCoordinate);
    if (entry == null) {
      entry = entryMap.lastEntry();
    }
    if (!owners.contains(entry.getValue())) {
      owners.add(entry.getValue());
    }
    return owners;
  }

  private static IPAndPort[] nodesToIPAndPort(List<Node> replicaNodes) {
    IPAndPort[] replicas;

    replicas = new IPAndPort[replicaNodes.size()];
    for (int i = 0; i < replicas.length; i++) {
      replicas[i] = new IPAndPort(replicaNodes.get(i).getIDString(), dhtPort);
      if (log.isDebugEnabled()) {
        log.debug("*** {} {} {}", replicaNodes.get(i), replicaNodes.get(i).getIDString(), replicas[i]);
      }
    }
    return replicas;
  }

  public boolean isSubset(ResolvedReplicaMap o) {
    for (RingEntry subEntry : o.getEntries()) {
      RingEntry superEntry;

      superEntry = entryMap.get(subEntry.getRegion().getStart());
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
  public boolean equals(Object obj) {
    ResolvedReplicaMap o;

    o = (ResolvedReplicaMap) obj;
    for (RingEntry subEntry : o.getEntries()) {
      RingEntry superEntry;

      superEntry = entryMap.get(subEntry.getRegion().getStart());
      if (superEntry == null) {
        return false;
      } else {
        if (!superEntry.equals(subEntry)) {
          return false;
        }
      }
    }
    return true;
  }

  private class MapEntry {
    private final IPAndPort[][] replicas;
    private final Set<IPAndPort>[] replicaSets;
    private final List<IPAndPort>[] replicaLists;
    private final PrimarySecondaryIPListPair ipListPair;

    private MapEntry(List<Node> _primaryReplicas, List<Node> _secondaryReplicas) {
      IPAndPort[] primaryReplicas;
      IPAndPort[] secondaryReplicas;
      IPAndPort[] allReplicas;

      primaryReplicas = nodesToIPAndPort(_primaryReplicas);
      secondaryReplicas = nodesToIPAndPort(_secondaryReplicas);
      if (replicaPrioritizer != null) {
        Arrays.sort(primaryReplicas, replicaPrioritizer);
        Arrays.sort(secondaryReplicas, replicaPrioritizer);
      }

      ipListPair = new PrimarySecondaryIPListPair(ImmutableList.copyOf(primaryReplicas),
          ImmutableList.copyOf(secondaryReplicas));

      allReplicas = new IPAndPort[primaryReplicas.length + secondaryReplicas.length];
      System.arraycopy(primaryReplicas, 0, allReplicas, 0, primaryReplicas.length);
      System.arraycopy(secondaryReplicas, 0, allReplicas, primaryReplicas.length, secondaryReplicas.length);

      replicas = new IPAndPort[EnumValues.ownerQueryMode.length][];
      replicas[OwnerQueryMode.Primary.ordinal()] = primaryReplicas;
      replicas[OwnerQueryMode.Secondary.ordinal()] = secondaryReplicas;
      replicas[OwnerQueryMode.All.ordinal()] = allReplicas;

      replicaSets = new Set[EnumValues.ownerQueryMode.length];
      replicaLists = new List[EnumValues.ownerQueryMode.length];
      for (OwnerQueryMode oqm : EnumValues.ownerQueryMode) {
        replicaSets[oqm.ordinal()] = ImmutableSet.copyOf(replicas[oqm.ordinal()]);
        replicaLists[oqm.ordinal()] = ImmutableList.copyOf(replicas[oqm.ordinal()]);
      }
    }

    MapEntry(RingEntry entry) {
      this(entry.getPrimaryOwnersList(), entry.getSecondaryOwnersList());
    }

    IPAndPort[] getReplicas(OwnerQueryMode oqm) {
      return replicas[oqm.ordinal()];
    }

    Set<IPAndPort> getReplicaSet(OwnerQueryMode oqm) {
      return replicaSets[oqm.ordinal()];
    }

    List<IPAndPort> getReplicaList(OwnerQueryMode oqm) {
      return replicaLists[oqm.ordinal()];
    }

    PrimarySecondaryIPListPair getIPListPair() {
      return ipListPair;
    }

    public String toString() {
      return "P[" + IPAndPort.arrayToString(
          replicas[OwnerQueryMode.Primary.ordinal()]) + "] S[" + IPAndPort.arrayToString(
          replicas[OwnerQueryMode.Secondary.ordinal()]) + "]";
    }
  }

  public String toDetailString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<Long, MapEntry> entry : replicaMap.entrySet()) {
      sb.append(entry.getKey());
      sb.append('\t');
      sb.append(entry.getValue());
      sb.append('\n');
    }
    return sb.toString();
  }

  public void display() {
    System.out.println(toDetailString());
  }

  /**
   * Not intended for high performanc use. For debugging only.
   *
   * @param key
   * @return
   */
  public RingRegion getRegion(DHTKey key) {
    for (RingRegion region : regions) {
      if (region.contains(KeyUtil.keyToCoordinate(key))) {
        return region;
      }
    }
    return null;
  }

  public List<RingRegion> getRegions() {
    return regions;
  }

  public void computeReplicaSet() {
    ImmutableSet.Builder<IPAndPort> _replicaSet;

    _replicaSet = ImmutableSet.builder();
    for (HashedListMap<IPAndPort, RingEntry> replicaEntry : replicaEntries) {
      _replicaSet.addAll(replicaEntry.keySet());
    }
    this.allReplicas = _replicaSet.build();
  }

  public Set<IPAndPort> allReplicas() {
    return allReplicas;
  }

  public List<Set<IPAndPort>> getExcludedReplicaSets(Set<IPAndPort> exclusionSet) {
    List<Set<IPAndPort>> excludedReplicaSets;

    //System.out.printf("exclusionSet %s\n", exclusionSet);
    excludedReplicaSets = new ArrayList<>();
    for (MapEntry entry : replicaMap.values()) {
      Set<IPAndPort> replicaSet;
      Set<IPAndPort> _replicaSet;

      replicaSet = entry.getReplicaSet(OwnerQueryMode.Primary);
      //System.out.printf("replicaSet %s\n", replicaSet);
      _replicaSet = new HashSet<>(replicaSet);
      _replicaSet.removeAll(exclusionSet);
      if (_replicaSet.size() == 0) {
        excludedReplicaSets.add(replicaSet);
      }
    }
    return excludedReplicaSets;
  }
}
