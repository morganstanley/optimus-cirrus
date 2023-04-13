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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RegionTreeBuilder;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.TreeMatcher;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.ring.RingRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeTest {
  private final int numKeys;
  private final List<KeyAndVersionChecksum> kvcList;
  private final RingRegion region;

  private static Logger log = LoggerFactory.getLogger(TreeTest.class);

  private static final int entriesPerNode = 10;

  public TreeTest(int numKeys) {
    this.numKeys = numKeys;
    region = new RingRegion(1, 1000000);
    kvcList = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      kvcList.add(new KeyAndVersionChecksum(KeyUtil.randomRegionKey(region), 0, 0));
    }
    Collections.sort(kvcList);
  }

  public void test() {
    ChecksumNode tree1;
    ChecksumNode tree2;
    ChecksumNode tree2b;
    ChecksumNode tree3;
    ChecksumNode tree4;
    ChecksumNode tree5;

    tree1 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, kvcList);
    log.info("{}", tree1);
    tree2 = tree1.duplicate();

    log.info(" *** Should be equal ***");
    log.info("{}",TreeMatcher.match(tree2, tree1));

    tree2b = RegionTreeBuilder.build(region, entriesPerNode, numKeys / 2, kvcList);
    log.info("{}",tree2b);
    log.info(" *** Should be equal (different key estimates) ***");
    log.info("{}",TreeMatcher.match(tree2b, tree1));

    List<KeyAndVersionChecksum> _kvcList;
    List<KeyAndVersionChecksum> _kvcList4;
    KeyAndVersionChecksum kvc1;
    KeyAndVersionChecksum kvc2;
    long checksum2;

    // remove one key so that we have destNotInSource and sourceNotInDest tests
    _kvcList = new ArrayList<>(kvcList);
    _kvcList.remove(0);
    tree3 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList);

    // now create a mismatch
    _kvcList4 = new ArrayList<>(kvcList);
    kvc1 = _kvcList4.remove(0);
    checksum2 = kvc1.getVersionChecksum() + 1;
    kvc2 = new KeyAndVersionChecksum(kvc1.getKey(), checksum2, 0);
    _kvcList4.add(0, kvc2);
    tree4 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList4);

    _kvcList4.remove(1);
    tree5 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList4);

    log.info(" *** Should have destNotInSource ***");
    log.info("{}",TreeMatcher.match(tree3, tree1));
    log.info(" *** Should have sourceNotInDest ***");
    log.info("{}",TreeMatcher.match(tree1, tree3));
    log.info(" *** Should have mismatch ***");
    log.info("{}",TreeMatcher.match(tree4, tree1));
    log.info(" *** Should be a mix ***");
    log.info("{}",TreeMatcher.match(tree5, tree3));
    System.out.flush();

    serializationTest(tree1);
  }

  private void serializationTest(ChecksumNode checksumNode) {
    ChecksumNode dNode;
    ByteBuffer buffer;

    buffer = ByteBuffer.allocate(1024 * 1024);
    ProtoChecksumTreeMessageGroup.serialize(buffer, checksumNode);
    log.info("{}", buffer);
    buffer.flip();
    log.info("{}", buffer);
    dNode = ProtoChecksumTreeMessageGroup.deserialize(buffer);
    log.info(" *** Pre-serialization ***");
    log.info("{}",checksumNode);
    log.info(" *** Post-serialization ***");
    log.info("{}", dNode);

    log.info(" *** Match test ***");
    log.info("{}",TreeMatcher.match(dNode, checksumNode));
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        log.info("args: <numKeys>");
        return;
      } else {
        TreeTest test;
        int numTests;

        test = new TreeTest(Integer.parseInt(args[0]));
        numTests = 1;
        for (int i = 0; i < numTests; i++) {
          test.test();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
