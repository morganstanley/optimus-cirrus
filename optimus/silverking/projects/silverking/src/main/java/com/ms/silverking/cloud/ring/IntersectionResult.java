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
package com.ms.silverking.cloud.ring;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.text.StringUtil;

/**
 * Result of intersecting two RingRegions
 */
public class IntersectionResult {
  private final IntersectionType intersectionType;
  private final List<RingRegion> aNonOverlapping;
  private final List<RingRegion> bNonOverlapping;
  private final List<RingRegion> overlapping;
  ;

  public IntersectionResult(IntersectionType intersectionType, List<RingRegion> aNonOverlapping,
      List<RingRegion> bNonOverlapping, List<RingRegion> overlapping) {
    this.intersectionType = intersectionType;
    this.aNonOverlapping = aNonOverlapping;
    this.bNonOverlapping = bNonOverlapping;
    this.overlapping = overlapping;
  }

  public IntersectionResult(IntersectionType intersectionType, List<RingRegion> aNonOverlapping,
      List<RingRegion> bNonOverlapping, RingRegion overlapping) {
    this(intersectionType, aNonOverlapping, bNonOverlapping, ImmutableList.of(overlapping));
  }

  public IntersectionType getIntersectionType() {
    return intersectionType;
  }

  public List<RingRegion> getANonOverlapping() {
    return aNonOverlapping;
  }

  public List<RingRegion> getBNonOverlapping() {
    return bNonOverlapping;
  }

  public List<RingRegion> getOverlapping() {
    return overlapping;
  }

  private long totalRegionSize() {
    return RingRegion.getTotalSize(aNonOverlapping) + RingRegion.getTotalSize(
        bNonOverlapping) + RingRegion.getTotalSize(overlapping);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(intersectionType);
    sb.append(':');
    sb.append("aNon[");
    sb.append(StringUtil.listToString(aNonOverlapping, ':'));
    sb.append(']');
    sb.append("bNon[");
    sb.append(StringUtil.listToString(bNonOverlapping, ':'));
    sb.append("]overlap");
    sb.append(StringUtil.listToString(overlapping, ':'));
    sb.append(']');
    sb.append("sz:" + totalRegionSize());
    return sb.toString();
  }
}
