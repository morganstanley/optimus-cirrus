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

import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.net.IPAndPort;

/**
 * Two lists: one for primaryOwners, and one for secondaryOwners.
 */
public class PrimarySecondaryIPListPair {
  private final List<IPAndPort> primaryOwners;
  private final List<IPAndPort> secondaryOwners;

  public PrimarySecondaryIPListPair(List<IPAndPort> primaryOwners, List<IPAndPort> secondaryOwners) {
    this.primaryOwners = primaryOwners;
    this.secondaryOwners = secondaryOwners;
  }

  PrimarySecondaryIPListPair() {
    this.primaryOwners = new ArrayList<>();
    this.secondaryOwners = new ArrayList<>();
  }

  PrimarySecondaryIPListPair duplicate() {
    List<IPAndPort> _primaryOwners;
    List<IPAndPort> _secondaryOwners;

    _primaryOwners = new ArrayList<>(primaryOwners);
    _secondaryOwners = new ArrayList<>(secondaryOwners);
    return new PrimarySecondaryIPListPair(_primaryOwners, _secondaryOwners);
  }

  List<IPAndPort> getOwners(ReplicationType rType) {
    switch (rType) {
    case Primary:
      return primaryOwners;
    case Secondary:
      return secondaryOwners;
    default:
      throw new RuntimeException("panic");
    }
  }

  public List<IPAndPort> getPrimaryOwners() {
    return primaryOwners;
  }

  public List<IPAndPort> getSecondaryOwners() {
    return secondaryOwners;
  }

  int totalOwners() {
    return primaryOwners.size() + secondaryOwners.size();
  }

  int totalOwners(ReplicationType rType) {
    switch (rType) {
    case Primary:
      return primaryOwners.size();
    case Secondary:
      return secondaryOwners.size();
    default:
      throw new RuntimeException("Panic");
    }
  }

  public boolean contains(IPAndPort node) {
    return primaryOwners.contains(node) || secondaryOwners.contains(node);
  }

  public String toString(String primaryPrefix, String secondaryPrefix, String psDelimiter) {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(primaryPrefix);
    for (IPAndPort owner : primaryOwners) {
      sb.append(owner);
      sb.append(' ');
    }
    sb.append(psDelimiter);
    sb.append(secondaryPrefix);
    for (IPAndPort owner : secondaryOwners) {
      sb.append(owner);
      sb.append(' ');
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return toString("", "", "");
  }

  public static PrimarySecondaryIPListPair merge(PrimarySecondaryIPListPair p0, PrimarySecondaryIPListPair p1) {
    List<IPAndPort> primaryOwners;
    List<IPAndPort> secondaryOwners;

    primaryOwners = new ArrayList<>(p0.getPrimaryOwners());
    // add unique primaries from p1
    for (IPAndPort node : p1.getPrimaryOwners()) {
      if (!primaryOwners.contains(node)) {
        primaryOwners.add(node);
      }
    }
    secondaryOwners = new ArrayList<>();
    // add secondaries from p0 that are not primaries (in p1)
    for (IPAndPort node : p0.getSecondaryOwners()) {
      if (!primaryOwners.contains(node)) {
        secondaryOwners.add(node);
      }
    }
    // add unique secondaries from p1 that are not primaries (in p0)
    for (IPAndPort node : p1.getSecondaryOwners()) {
      if (!primaryOwners.contains(node) && !secondaryOwners.contains(node)) {
        secondaryOwners.add(node);
      }
    }
    return new PrimarySecondaryIPListPair(primaryOwners, secondaryOwners);
  }
}
