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

import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.collection.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Two lists: one for primaryOwners, and one for secondaryOwners.
 */
class PrimarySecondaryListPair {
  private final List<Node> primaryOwners;
  private final List<Node> secondaryOwners;
  private static Logger log = LoggerFactory.getLogger(PrimarySecondaryListPair.class);

  PrimarySecondaryListPair(List<Node> primaryOwners, List<Node> secondaryOwners) {
    this.primaryOwners = primaryOwners;
    this.secondaryOwners = secondaryOwners;
  }

  PrimarySecondaryListPair() {
    this.primaryOwners = new ArrayList<>();
    this.secondaryOwners = new ArrayList<>();
  }

  PrimarySecondaryListPair duplicate() {
    List<Node> _primaryOwners;
    List<Node> _secondaryOwners;

    _primaryOwners = new ArrayList<>(primaryOwners);
    _secondaryOwners = new ArrayList<>(secondaryOwners);
    return new PrimarySecondaryListPair(_primaryOwners, _secondaryOwners);
  }

  List<Node> getOwners(ReplicationType rType) {
    switch (rType) {
    case Primary:
      return primaryOwners;
    case Secondary:
      return secondaryOwners;
    default:
      throw new RuntimeException("panic");
    }
  }

  List<Node> getPrimaryOwners() {
    return primaryOwners;
  }

  List<Node> getSecondaryOwners() {
    return secondaryOwners;
  }

  void replaceOwner(Node oldOwner, Node newOwner) {
    int index;
    List<Node> targetList;

    if (contains(newOwner)) {
      throw new RuntimeException("Already owned: " + newOwner);
    }

    targetList = null;
    index = primaryOwners.indexOf(oldOwner);
    if (index >= 0) {
      targetList = primaryOwners;
    } else {
      index = secondaryOwners.indexOf(oldOwner);
      if (index < 0) {
        throw new RuntimeException("oldOwner not found");
      }
      targetList = secondaryOwners;
    }

    targetList.remove(index);
    targetList.add(index, newOwner);
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

  boolean contains(Node node) {
    return primaryOwners.contains(node) || secondaryOwners.contains(node);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append("p ");
    for (Node owner : primaryOwners) {
      sb.append(owner);
      sb.append(' ');
    }
    sb.append("s ");
    for (Node owner : secondaryOwners) {
      sb.append(owner);
      sb.append(' ');
    }
    return sb.toString();
  }

  static PrimarySecondaryListPair merge(PrimarySecondaryListPair p0, PrimarySecondaryListPair p1) {
    List<Node> primaryOwners;
    List<Node> secondaryOwners;

    primaryOwners = new ArrayList<>(p0.getPrimaryOwners());
    // add unique primaries from p1
    for (Node node : p1.getPrimaryOwners()) {
      if (!primaryOwners.contains(node)) {
        primaryOwners.add(node);
      }
    }
    secondaryOwners = new ArrayList<>();
    // add secondaries from p0 that are not primaries (in p1)
    for (Node node : p0.getSecondaryOwners()) {
      if (!primaryOwners.contains(node)) {
        secondaryOwners.add(node);
      }
    }
    // add unique secondaries from p1 that are not primaries (in p0)
    for (Node node : p1.getSecondaryOwners()) {
      if (!primaryOwners.contains(node) && !secondaryOwners.contains(node)) {
        secondaryOwners.add(node);
      }
    }
    return new PrimarySecondaryListPair(primaryOwners, secondaryOwners);
  }

  public Node getLastAddedOwnerNotIn(Set<Node> excluding) {
    int secondaryIndex;
    int primaryIndex;
    Node node;

    secondaryIndex = secondaryOwners.size() - 1;
    primaryIndex = primaryOwners.size() - 1;
    do {
      if (secondaryIndex > 0) {
        node = secondaryOwners.get(secondaryIndex);
        secondaryIndex--;
      } else {
        if (primaryOwners.size() == 0) {
          log.info("{}",this);
          log.info("{}",CollectionUtil.toString(excluding));
          throw new RuntimeException("getLastAddedOwnerNotIn() failed");
        }
        node = primaryOwners.get(primaryIndex);
        primaryIndex--;
      }
      if (excluding.contains(node)) {
        node = null;
      }
    } while (node == null);
    return node;
        /*
        if (secondaryOwners.size() > 0) {
            return secondaryOwners.get(secondaryOwners.size() - 1);
        } else {
            assert primaryOwners.size() > 0;
            return primaryOwners.get(primaryOwners.size() - 1);
        }
        */
  }
}
