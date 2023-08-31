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

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.numeric.NumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds a checksum tree for a single region. */
public class RegionTreeBuilder {
  private final RingRegion region;
  private ChecksumNode root;

  private static Logger log = LoggerFactory.getLogger(RegionTreeBuilder.class);

  private final int entriesPerNode;
  private final int estimatedKeys;
  private final int numLeaves;
  private final long leafRegionSize;
  private final LeafChecksumNode[] leaves;
  private final int height;

  private static final boolean debug = false;

  RegionTreeBuilder(RingRegion region, int entriesPerNode, int estimatedKeys) {
    if (entriesPerNode < 2) {
      throw new RuntimeException("entriesPerNode must be >= 2");
    }
    this.region = region;
    this.entriesPerNode = entriesPerNode;
    this.estimatedKeys = estimatedKeys;
    numLeaves = Math.max((int) Math.ceil((double) estimatedKeys / (double) entriesPerNode), 1);
    leafRegionSize = region.getSize() / numLeaves;
    assert estimatedKeys >= 0;
    if (estimatedKeys == 0) {
      height = 1;
    } else {
      height =
          Math.max(
              (int) Math.ceil(NumUtil.log((double) entriesPerNode, (double) estimatedKeys)), 1);
    }
    leaves = createLeaves(region, numLeaves);
    if (debug) {
      log.debug("{}", height);
      log.debug("Leaves: {}", leavesToString());
    }
    root = createInternalNodes(leaves, height, entriesPerNode, estimatedKeys);
    if (debug) {
      log.debug("{}", root.getRegion());
      log.debug("{}", region);
      log.debug("");
      log.debug("{}", root);
    }
    assert root.getRegion().equals(region);
  }

  public static ChecksumNode build(
      RingRegion region,
      int entriesPerNode,
      int estimatedKeys,
      Iterable<KeyAndVersionChecksum> kvcIterable) {
    RegionTreeBuilder rtb;
    ChecksumNode root;

    rtb = new RegionTreeBuilder(region, entriesPerNode, estimatedKeys);
    rtb.addChecksums(kvcIterable);
    root = rtb.getRoot();
    rtb.freezeLeaves();
    // root.freeze();
    return root;
  }

  private static LeafChecksumNode[] createLeaves(RingRegion region, int numLeaves) {
    LeafChecksumNode[] leaves;
    long leafRegionSize;
    long lastEnd;

    leaves = new LeafChecksumNode[numLeaves];
    leafRegionSize = region.getSize() / numLeaves;
    lastEnd = LongRingspace.prevPoint(region.getStart());
    for (int i = 0; i < numLeaves; i++) {
      RingRegion leafRegion;
      long start;
      long end;

      start = LongRingspace.nextPoint(lastEnd);
      if (i < numLeaves - 1) {
        end = LongRingspace.add(start, leafRegionSize - 1);
      } else {
        end = region.getEnd();
      }
      leafRegion = new RingRegion(start, end);
      leaves[i] = new LeafChecksumNode(leafRegion);
      lastEnd = end;
    }
    return leaves;
  }

  private static ChecksumNode createInternalNodes(
      LeafChecksumNode[] leaves, int height, int entriesPerNode, int estimatedKeys) {
    ChecksumNode[][] nodes;

    nodes = new ChecksumNode[height][];
    /*
     * 0          => root
     * height - 1 => leaves
     *
     * e.g. with 128 keys, 8 per node we have
     * row  numMembers  children or content per member
     * 0:   1           2
     * 1:   2           8
     * 2:   16          8 (content in this case)
     *
     */
    int nextRowChildren;

    nextRowChildren = estimatedKeys;
    for (int i = height - 1; i >= 0; i--) {
      // System.out.printf("\n%d **********************************\n", i);
      if (i < height - 1) {
        int rowSize;
        int childrenPerNode;

        assert nextRowChildren > 0;
        rowSize = (nextRowChildren - 1) / entriesPerNode + 1;
        nodes[i] = new NonLeafChecksumNode[rowSize];
        // childrenPerNode = Math.round(nextRowChildren / rowSize);
        childrenPerNode = entriesPerNode;
        for (int j = 0; j < nodes[i].length; j++) {
          RingRegion region;
          List<ChecksumNode> children;
          int i0;
          int i1;

          i0 = j * childrenPerNode;
          i1 = Math.min(i0 + childrenPerNode, nextRowChildren);
          children = ImmutableList.copyOf(Arrays.copyOfRange(nodes[i + 1], i0, i1));
          region =
              new RingRegion(
                  children.get(0).getRegion().getStart(),
                  children.get(children.size() - 1).getRegion().getEnd());
          nodes[i][j] = new NonLeafChecksumNode(region, children);
          if (debug) {
            log.debug("{} {} {} {} {}", i, j, i0, i1, nodes[i][j].getRegion());
          }
        }
        nextRowChildren = rowSize;
      } else {
        nodes[i] = leaves;
        nextRowChildren = leaves.length;
      }
      if (debug) {
        System.out.println("");
      }
    }
    if (nodes[0].length != 1) {
      throw new RuntimeException("panic");
    }
    return nodes[0][0];
  }

  public void freeze() {
    freezeLeaves();
  }

  private void freezeLeaves() {
    for (LeafChecksumNode leaf : leaves) {
      leaf.freeze();
    }
  }

  public RingRegion getRegion() {
    return region;
  }

  public ChecksumNode getRoot() {
    return root;
  }

  private int leafIndex(long p) {
    long distToStart;

    distToStart = LongRingspace.clockwiseDistance(region.getStart(), p);
    return (int) (distToStart / leafRegionSize);
  }

  public void addChecksums(Iterable<KeyAndVersionChecksum> kvcIterable) {
    for (KeyAndVersionChecksum kvc : kvcIterable) {
      addChecksum(kvc);
    }
  }

  public void addChecksum(KeyAndVersionChecksum kvc) {
    long p;
    int index;

    p = KeyUtil.keyToCoordinate(kvc.getKey());
    index = leafIndex(p);
    leaves[index].addChecksum(kvc);
  }

  private String leavesToString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (LeafChecksumNode leaf : leaves) {
      sb.append(leaf.toString());
      sb.append('\n');
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    root.toString(sb, 0);
    return sb.toString();
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      log.info("numKeys keysPerNode regionStart regionEnd");
    } else {
      RegionTreeBuilder rtb;
      int numKeys;
      int keysPerNode;
      long regionStart;
      long regionEnd;
      RingRegion region;

      numKeys = Integer.parseInt(args[0]);
      keysPerNode = Integer.parseInt(args[1]);
      regionStart = Long.parseLong(args[2]);
      regionEnd = Long.parseLong(args[3]);
      region = new RingRegion(regionStart, regionEnd);
      rtb = new RegionTreeBuilder(region, keysPerNode, numKeys);
      for (int i = 0; i < numKeys; i++) {
        rtb.addChecksum(new KeyAndVersionChecksum(KeyUtil.randomRegionKey(region), i, 0));
      }
      log.info("{}", rtb);
    }
  }
}
