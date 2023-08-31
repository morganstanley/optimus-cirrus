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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.RingRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds checksum trees for multiple regions. This allows all desired trees to be built in a single
 * pass over local data.
 */
public class TreeBuilder {
  private final Iterator<KeyAndVersionChecksum> keyIterator;
  private final NavigableMap<Long, RegionTreeBuilder> rtBuilders;

  private static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(TreeBuilder.class);

  private TreeBuilder(
      Collection<RingRegion> regions,
      Iterator<KeyAndVersionChecksum> keyIterator,
      int entriesPerNode,
      long estimatedKeys) {
    this.keyIterator = keyIterator;
    rtBuilders = new TreeMap<>();
    for (RingRegion region : regions) {
      if (debug) {
        log.debug("Builder: {}", region);
      }
      rtBuilders.put(
          region.getStart(),
          new RegionTreeBuilder(region, entriesPerNode, estimateRegionKeys(region, estimatedKeys)));
    }
  }

  /**
   * Given several regions, build checksum trees for each region using the keys provided by the
   * KeyAndVersionChecksum iterator.
   *
   * @param regions
   * @param keyIterator
   * @param entriesPerNode
   * @param estimatedKeys
   * @param creationTimeMillis
   * @param minVersion
   * @param maxVersion
   * @param allowRegionNotFound TODO (OPTIMUS-0000): describe
   * @return
   */
  public static ChecksumTreeGroup build(
      Collection<RingRegion> regions,
      Iterator<KeyAndVersionChecksum> keyIterator,
      int entriesPerNode,
      long estimatedKeys,
      long creationTimeMillis,
      long minVersion,
      long maxVersion,
      boolean allowRegionNotFound) {
    return new ChecksumTreeGroup(
        creationTimeMillis,
        minVersion,
        maxVersion,
        new TreeBuilder(regions, keyIterator, entriesPerNode, estimatedKeys)
            .build(allowRegionNotFound));
  }

  private static int estimateRegionKeys(RingRegion region, long estimatedKeys) {
    return (int) (region.getRingspaceFraction() * (double) estimatedKeys);
  }

  private RegionTreeBuilder getBuilderForKey(DHTKey key, boolean allowRegionNotFound) {
    long p;
    RegionTreeBuilder rtb;
    Map.Entry<Long, RegionTreeBuilder> floorEntry;

    p = KeyUtil.keyToCoordinate(key);
    floorEntry = rtBuilders.floorEntry(p);
    if (floorEntry != null) {
      rtb = floorEntry.getValue();
    } else {
      rtb = null;
    }
    if (rtb == null) {
      if (!allowRegionNotFound) {
        log.info("{} {}", key, p);
        throw new RuntimeException("Can't find builder for coordinate ");
      } else {
        return null;
      }
    }
    if (!rtb.getRegion().contains(p)) {
      if (!allowRegionNotFound) {
        log.info("{} {} {}", key, p, rtb.getRegion());
        throw new RuntimeException("Can't find builder for coordinate ");
      } else {
        return null;
      }
    }
    return rtb;
  }

  private NavigableMap<Long, RegionTreeBuilder> build(boolean allowRegionNotFound) {
    while (keyIterator.hasNext()) {
      KeyAndVersionChecksum kvc;
      RegionTreeBuilder rtb;

      kvc = keyIterator.next();
      rtb = getBuilderForKey(kvc.getKey(), allowRegionNotFound);
      if (rtb != null) {
        rtb.addChecksum(kvc);
      }
    }
    freezeBuilders();
    return rtBuilders;
  }

  private void freezeBuilders() {
    for (RegionTreeBuilder rtb : rtBuilders.values()) {
      rtb.freeze();
    }
  }

  @Override
  public String toString() {
    return null;
  }
}
