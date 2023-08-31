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
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.cloud.topology.Node;

/**
 * Prototype region used for region creation. Pairs a RingRegion with a PrimarySecondaryListPair.
 */
class ProtoRegion {
  private final RingRegion region;
  private final PrimarySecondaryListPair owners;
  private final int minPrimaryUnderFailure;

  ProtoRegion(RingRegion region, int minPrimaryUnderFailure) {
    this.region = region;
    owners = new PrimarySecondaryListPair();
    this.minPrimaryUnderFailure = minPrimaryUnderFailure;
  }

  ProtoRegion(RingRegion region, PrimarySecondaryListPair owners, int minPrimaryUnderFailure) {
    this.region = region;
    this.owners = owners;
    this.minPrimaryUnderFailure = minPrimaryUnderFailure;
  }

  public ProtoRegion duplicate() {
    return new ProtoRegion(region, owners.duplicate(), minPrimaryUnderFailure);
  }

  RingRegion getRegion() {
    return region;
  }

  long getRegionSize() {
    return region.getSize();
  }

  Set<Node> getOwnersSet() {
    ImmutableSet.Builder<Node> ownersSet;

    ownersSet = ImmutableSet.builder();
    ownersSet.addAll(getPrimaryOwners());
    ownersSet.addAll(getSecondaryOwners());
    return ownersSet.build();
  }

  PrimarySecondaryListPair getOwners() {
    return owners;
  }

  List<Node> getOwners(ReplicationType rType) {
    return owners.getOwners(rType);
  }

  List<Node> getPrimaryOwners() {
    return owners.getPrimaryOwners();
  }

  List<Node> getSecondaryOwners() {
    return owners.getSecondaryOwners();
  }

  int getMinPrimaryUnderFailure() {
    return minPrimaryUnderFailure;
  }

  int totalOwners() {
    return owners.totalOwners();
  }

  int totalOwners(ReplicationType rType) {
    return owners.totalOwners(rType);
  }

  boolean contains(Node node) {
    return owners.contains(node);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(region);
    sb.append(' ');
    sb.append(owners);
    return sb.toString();
  }

  static List<ProtoRegion> merge(ProtoRegion r0, ProtoRegion r1) {
    List<ProtoRegion> merged;

    merged = new ArrayList<>();
    if (r0.getRegion().isContiguousWith(r1.getRegion())) {
      RingRegion region;
      ProtoRegion protoRegion;
      PrimarySecondaryListPair owners;

      region = r0.getRegion().merge(r1.getRegion());
      owners = PrimarySecondaryListPair.merge(r0.getOwners(), r1.getOwners());
      protoRegion =
          new ProtoRegion(
              region,
              owners,
              Math.max(r0.getMinPrimaryUnderFailure(), r1.getMinPrimaryUnderFailure()));
      merged.add(protoRegion);
    } else if (r0.getRegion().overlaps(r1.getRegion())) {
      RingRegion _r0;
      RingRegion _r01;
      RingRegion _r1;

      _r0 = r0.getRegion().trimOverlappingWith(r1.getRegion());
      _r01 = r0.getRegion().trimOverlappingWith(_r0);
      _r1 = r1.getRegion().trimOverlappingWith(r0.getRegion());

      if (_r0 != null) {
        merged.add(
            new ProtoRegion(
                _r0,
                r0.getOwners().duplicate(),
                Math.max(r0.getMinPrimaryUnderFailure(), r1.getMinPrimaryUnderFailure())));
      }
      merged.add(
          new ProtoRegion(
              _r01,
              PrimarySecondaryListPair.merge(r0.getOwners(), r1.getOwners()),
              Math.max(r0.getMinPrimaryUnderFailure(), r1.getMinPrimaryUnderFailure())));
      if (_r1 != null) {
        merged.add(
            new ProtoRegion(
                _r1,
                r1.getOwners().duplicate(),
                Math.max(r0.getMinPrimaryUnderFailure(), r1.getMinPrimaryUnderFailure())));
      }
    } else {
      throw new RuntimeException("Invalid regions for merge");
    }
    return merged;
  }

  static ProtoRegion intersect(ProtoRegion r0, ProtoRegion r1) {
    RingRegion rr0;
    RingRegion rr1;

    rr0 = r0.getRegion();
    rr1 = r1.getRegion();
    if (!rr0.equals(rr1) && rr0.isContiguousWith(rr1)) {
      throw new RuntimeException("contiguous not expected");
    } else if (rr0.overlaps(rr1)) {
      RingRegion _r0;
      RingRegion _r01;

      _r0 = rr0.trimOverlappingWith(rr1);
      if (_r0 != null) {
        _r01 = rr0.trimOverlappingWith(_r0);
      } else {
        _r01 = rr0;
      }
      return new ProtoRegion(
          _r01,
          PrimarySecondaryListPair.merge(r0.getOwners(), r1.getOwners()),
          Math.max(r0.getMinPrimaryUnderFailure(), r1.getMinPrimaryUnderFailure()));
    } else {
      throw new RuntimeException("Invalid regions for merge");
    }
  }

  public void addOwner(ReplicationType replicationType, Node node) {
    getOwners(replicationType).add(node);
  }

  public void replaceOwner(Node oldOwner, Node newOwner) {
    if (owners.contains(newOwner)) {
      throw new RuntimeException(this + " Already owned: " + newOwner + " " + oldOwner);
    } else {
      owners.replaceOwner(oldOwner, newOwner);
    }
  }

  public Node getLastAddedOwnerNotIn(Set<Node> excluding) {
    return owners.getLastAddedOwnerNotIn(excluding);
  }
}
