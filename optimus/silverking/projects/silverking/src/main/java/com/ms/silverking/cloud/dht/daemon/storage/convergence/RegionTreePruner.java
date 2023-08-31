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
package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.IntersectionType;
import com.ms.silverking.cloud.ring.RingRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prunes a complete region tree to create a new tree pertaining to a sub region. */
public class RegionTreePruner {

  private static Logger log = LoggerFactory.getLogger(RegionTreePruner.class);

  public static ChecksumNode prune(ChecksumNode root, RingRegion pruneRegion) {
    if (log.isDebugEnabled()) {
      log.debug("in prune {} {} {}", root.getRegion(), pruneRegion, root.estimatedKeys());
    }
    if (root.getRegion().equals(pruneRegion)) {
      log.debug("root.getRegion == pruneRegion");
      return root;
    } else {
      if (RingRegion.intersectionType(root.getRegion(), pruneRegion)
          != IntersectionType.aSubsumesB) {
        throw new RuntimeException("Invalid pruneRegion");
      } else {
        ChecksumNode prunedRoot;

        prunedRoot = _prune(root, pruneRegion);
        if (log.isDebugEnabled()) {
          log.debug(
              "out prune {} {} {}",
              prunedRoot.getRegion(),
              pruneRegion,
              prunedRoot.estimatedKeys());
        }
        return prunedRoot;
      }
    }
  }

  private static ChecksumNode _prune(ChecksumNode node, RingRegion pruneRegion) {
    if (node instanceof LeafChecksumNode) {
      return pruneLeafNode((LeafChecksumNode) node, pruneRegion);
    } else if (node instanceof NonLeafChecksumNode) {
      return pruneNonLeafNode((NonLeafChecksumNode) node, pruneRegion);
    } else {
      throw new RuntimeException("panic");
    }
  }

  private static ChecksumNode pruneNonLeafNode(NonLeafChecksumNode node, RingRegion pruneRegion) {
    List<ChecksumNode> newChildren;
    IntersectionResult iResult;

    if (!node.getRegion().overlaps(pruneRegion)) {
      throw new RuntimeException("panic");
    }

    iResult = RingRegion.intersect(node.getRegion(), pruneRegion);
    if (iResult.getIntersectionType() == IntersectionType.wrappedPartial) {
      // should be a very unusual case
      // FUTURE - handle this
      throw new RuntimeException("wrappedPartial pruning not yet handled");
    }

    newChildren = new ArrayList<>();
    for (ChecksumNode child : node.getChildren()) {
      if (child.getRegion().overlaps(pruneRegion)) {
        newChildren.add(_prune(child, pruneRegion));
      }
    }

    return new NonLeafChecksumNode(iResult.getOverlapping().get(0), newChildren);
  }

  private static ChecksumNode pruneLeafNode(LeafChecksumNode node, RingRegion pruneRegion) {
    IntersectionResult iResult;
    RingRegion oRegion;
    // long[]                _kvc;
    List<KeyAndVersionChecksum> kvcList;
    List<KeyAndVersionChecksum> prunedKVCList;

    if (!node.getRegion().overlaps(pruneRegion)) {
      throw new RuntimeException("panic");
    }

    iResult = RingRegion.intersect(node.getRegion(), pruneRegion);
    if (iResult.getIntersectionType() == IntersectionType.wrappedPartial) {
      // should be a very unusual case
      // FUTURE - handle this
      throw new RuntimeException("wrappedPartial pruning not yet handled");
    }

    oRegion = iResult.getOverlapping().get(0);

    // _kvc = node.getKeyAndVersionChecksumsAsArray();

    // FUTURE - improve the efficiency of this approach
    // This is n log n, and each step is heavy
    // Should be able to do at least log n with very light steps
    kvcList = node.getKeyAndVersionChecksums();
    prunedKVCList = new ArrayList<>(kvcList.size());
    for (KeyAndVersionChecksum kvc : kvcList) {
      if (pruneRegion.contains(KeyUtil.keyToCoordinate(kvc.getKey()))) {
        prunedKVCList.add(kvc);
      }
    }

    return new LeafChecksumNode(oRegion, prunedKVCList);
  }
}
