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

import java.util.Map;
import java.util.NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A group of ChecksumTrees for a set of regions and a particular version
 * as of a particular time. External code should ensure that - in the context
 * of a particular namespace - no two ChecksumTreeGroups have the same
 * creationTimeMillis and versions.
 * <p>
 * ChecksumTrees are obtained from RegionTreeBuilders.
 */
public class ChecksumTreeGroup {
  private final long creationTimeMillis;
  private final long minVersion;
  private final long maxVersion;
  private final NavigableMap<Long, RegionTreeBuilder> regionTreeBuilders;

  private static Logger log = LoggerFactory.getLogger(ChecksumTreeGroup.class);


  public ChecksumTreeGroup(long creationTimeMillis, long minVersion, long maxVersion,
      NavigableMap<Long, RegionTreeBuilder> regionTreeBuilders) {
    this.creationTimeMillis = creationTimeMillis;
    this.minVersion = minVersion;
    this.maxVersion = maxVersion;
    this.regionTreeBuilders = regionTreeBuilders;
  }

  public void displayRegions() {
    for (RegionTreeBuilder rtb : regionTreeBuilders.values()) {
      log.info("{}", rtb.getRegion());
    }
  }

  public long getCreationTimeMillis() {
    return creationTimeMillis;
  }

  public long getMinVersion() {
    return minVersion;
  }

  public long getMaxVersion() {
    return maxVersion;
  }

  //public Map<Long, RegionTreeBuilder> getRegionTreeBuilders() {
  //    return regionTreeBuilders;
  //}

  public ChecksumNode getTreeRoot(long regionStart) {
    RegionTreeBuilder rtb;
    Map.Entry<Long, RegionTreeBuilder> floorEntry;

    floorEntry = regionTreeBuilders.floorEntry(regionStart);
    rtb = floorEntry != null ? floorEntry.getValue() : null;
    if (rtb != null) {
      return rtb.getRoot();
    } else {
            /*
            StringBuilder   sb;
            
            sb = new StringBuilder();
            for (Long rs : regionTreeBuilders.keySet()) {
                sb.append(rs +"\n");
            }
            System.out.println(sb);
            */
      log.error("creationTimeMillis {} minVersion {} maxVersion {}", creationTimeMillis, minVersion,
          maxVersion);
      throw new RuntimeException("Can't find RegionTreeBuilder for: " + regionStart);
    }
  }
}
