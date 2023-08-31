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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.numeric.NumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingRegion {
  private final long start;
  private final long end;

  private static Logger log = LoggerFactory.getLogger(RingRegion.class);

  public static final RingRegion allRingspace =
      new RingRegion(LongRingspace.start, LongRingspace.end);

  // used for ordering RingRegions
  public static final Comparator<RingRegion> sizeComparator = new RingRegionSizeComparator();
  public static final Comparator<RingRegion> positionComparator =
      new RingRegionPositionComparator();
  public static final Comparator<RingRegion> sizePositionComparator =
      new RingRegionSizePositionComparator();

  public RingRegion(long start, long end) {
    LongRingspace.ensureInRingspace(start);
    LongRingspace.ensureInRingspace(end);
    if (getSize(start, end) == LongRingspace.size) {
      // normalize any region that consumes the entire ringspace
      start = LongRingspace.start;
      end = LongRingspace.end;
    }
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  private static long getSize(long start, long end) {
    return LongRingspace.clockwiseDistance(start, end) + 1;
  }

  public long getSize() {
    return getSize(start, end);
  }

  public boolean before(long p0, long p1) {
    long d0;
    long d1;

    d0 = LongRingspace.clockwiseDistance(start, p0);
    d1 = LongRingspace.clockwiseDistance(start, p1);
    return d0 < d1;
  }

  public boolean after(long p0, long p1) {
    long d0;
    long d1;

    d0 = LongRingspace.clockwiseDistance(start, p0);
    d1 = LongRingspace.clockwiseDistance(start, p1);
    return d0 > d1;
  }

  public BigDecimal getSizeBD() {
    return new BigDecimal(getSize(), LongRingspace.mathContext);
  }

  public double getRingspaceFraction() {
    return getSizeBD().divide(LongRingspace.bdSize, LongRingspace.mathContext).doubleValue();
  }

  public static void ensureIdentical(RingRegion r0, RingRegion r1) {
    if (!r0.equals(r1)) {
      System.err.println(r0);
      System.err.println(r1);
      throw new RuntimeException("Regions not identical");
    }
  }

  @Override
  public boolean equals(Object other) {
    RingRegion oRegion;

    oRegion = (RingRegion) other;
    return this.start == oRegion.start && this.end == oRegion.end;
  }

  @Override
  public int hashCode() {
    return (int) start ^ (int) end;
  }

  public static RingRegion parseZKString(String s) {
    try {
      int index;

      index = s.indexOf(':');
      return new RingRegion(
          Long.parseLong(s.substring(0, index)), Long.parseLong(s.substring(index + 1)));
    } catch (RuntimeException re) {
      System.err.println(s);
      throw re;
    }
  }

  public String toZKString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(start);
    sb.append(':');
    sb.append(end);
    return sb.toString();
  }

  @Override
  public String toString() {
    return "[" + toZKString() + ":" + getSize() + "]";
  }

  public boolean isContiguousWith(RingRegion oRegion) {
    return LongRingspace.nextPoint(end) == oRegion.start
        || LongRingspace.nextPoint(oRegion.end) == start;
  }

  public static void display(Collection<RingRegion> regions) {
    long totalSize;

    totalSize = 0;
    for (RingRegion r : regions) {
      System.out.println(r);
      totalSize += r.getSize();
    }
    System.out.println(totalSize + "\t" + allRingspace.getSize());
  }

  public RingRegion partitionStart(long subRegionSize) {
    long subRegionEnd;

    assert subRegionSize > 0 && subRegionSize < getSize();
    subRegionEnd = LongRingspace.add(start, subRegionSize - 1);
    return new RingRegion(start, subRegionEnd);
  }

  public RingRegion partitionEnd(long subRegionSize) {
    long subRegionStart;

    assert subRegionSize > 0 && subRegionSize < getSize();
    subRegionStart = LongRingspace.subtract(end, subRegionSize - 1);
    return new RingRegion(subRegionStart, end);
  }

  public RingRegion[] split(long firstRegionSize) {
    if (firstRegionSize < 0) {
      throw new RuntimeException("firstRegionSize < 0");
    } else if (firstRegionSize < getSize()) {
      long subRegionEnd;
      RingRegion[] regions;

      regions = new RingRegion[2];
      subRegionEnd = LongRingspace.add(start, firstRegionSize - 1);
      regions[0] = new RingRegion(start, subRegionEnd);
      regions[1] = new RingRegion(subRegionEnd + 1, end);
      return regions;
    } else if (firstRegionSize == getSize()) {
      RingRegion[] regions;

      regions = new RingRegion[1];
      regions[0] = this;
      return regions;
    } else {
      throw new RuntimeException("firstRegionSize > region size");
    }
  }

  // only for testing currently
  private List<RingRegion> divide(int numRegions) {
    ArrayList<RingRegion> subRegions;
    long subRegionSize;
    long subRegionStart;

    assert numRegions >= 0;
    subRegionSize = getSize() / numRegions;
    subRegions = new ArrayList<RingRegion>();
    subRegionStart = start;
    for (int i = 0; i < numRegions; i++) {
      long subRegionEnd;

      if (i < numRegions - 1) {
        subRegionEnd = subRegionStart + (subRegionSize - 1);
      } else {
        subRegionEnd = end;
      }
      // System.out.println(i +" "+ subRegionStart +" "+ subRegionEnd +" "+ subRegionSize);
      subRegions.add(new RingRegion(subRegionStart, subRegionEnd));
      subRegionStart += subRegionSize;
    }
    return subRegions;
  }

  public List<Long> dividedRegionSizes(List<Double> weights) {
    List<Long> regionSizes;
    List<RingRegion> regions;

    regions = divide(weights);
    regionSizes = new ArrayList<>(regions.size());
    for (int i = 0; i < regions.size(); i++) {
      regionSizes.add(regions.get(i).getSize());
    }
    return regionSizes;
  }

  /*
  public List<RingRegion> divide(List<Double> weights) {
      List<RingRegion>   subRegions;
      long               subRegionStart;
      int                numRegions;
      List<Double>       normalizedWeights;

      normalizedWeights = NumUtil.normalize(weights);
      numRegions = normalizedWeights.size();
      subRegions = new ArrayList<>(numRegions);
      subRegionStart = start;
      for (int i = 0; i < numRegions; i++) {
          long    subRegionEnd;
          long    subRegionSize;

          subRegionSize = (long)((double)getSize() * normalizedWeights.get(i));
          if (i < numRegions - 1) {
              subRegionEnd = subRegionStart + (subRegionSize - 1);
          } else {
              subRegionEnd = end;
          }
          if (Log.levelMet(Level.FINE)) {
              Log.fine(i +" "+ subRegionStart +" "+ subRegionEnd +" "+ subRegionSize +" "+ normalizedWeights.get(i));
          }
          subRegions.add(new RingRegion(subRegionStart, subRegionEnd));
          subRegionStart += subRegionSize;
      }
      return subRegions;
  }
  */

  public List<RingRegion> divide(List<Double> weights) {
    List<RingRegion> subRegions;
    long subRegionStart;
    int numRegions;
    List<BigDecimal> normalizedWeights;

    normalizedWeights = NumUtil.normalizeAsBD(weights, LongRingspace.mathContext);
    numRegions = normalizedWeights.size();
    subRegions = new ArrayList<>(numRegions);
    subRegionStart = start;
    for (int i = 0; i < numRegions; i++) {
      long subRegionEnd;
      long subRegionSize;

      // subRegionSize = (long)((double)getSize() * normalizedWeights.get(i));
      subRegionSize =
          new BigDecimal(getSize(), LongRingspace.mathContext)
              .multiply(normalizedWeights.get(i), LongRingspace.mathContext)
              .longValue();
      if (i < numRegions - 1) {
        subRegionEnd = subRegionStart + (subRegionSize - 1);
      } else {
        subRegionEnd = end;
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "{} {} {} {} {}",
            i,
            subRegionStart,
            subRegionEnd,
            subRegionSize,
            normalizedWeights.get(i));
      }
      subRegions.add(new RingRegion(subRegionStart, subRegionEnd));
      subRegionStart += subRegionSize;
    }
    return subRegions;
  }

  public RingRegion merge(RingRegion oRegion) {
    if (!isContiguousWith(oRegion)) {
      throw new RuntimeException("Not adjacent: " + this + " " + oRegion);
    } else {
      if (LongRingspace.nextPoint(end) == oRegion.start) {
        return new RingRegion(start, oRegion.end);
      } else {
        return new RingRegion(oRegion.start, end);
      }
    }
  }

  public static List<RingRegion> mergeAdjacent(List<RingRegion> sourceRegions) {
    List<RingRegion> mergedRegions;
    List<RingRegion> regions;
    RingRegion curRegion;
    int index;

    regions = new ArrayList<RingRegion>(sourceRegions.size());
    regions.addAll(sourceRegions);
    Collections.sort(regions, RingRegion.positionComparator);
    mergedRegions = new ArrayList<RingRegion>(sourceRegions.size());
    curRegion = regions.get(0);
    index = 1;
    while (index < regions.size()) {
      RingRegion nextRegion;

      nextRegion = regions.get(index);
      if (curRegion.isContiguousWith(nextRegion)) {
        curRegion = curRegion.merge(nextRegion);
      } else {
        mergedRegions.add(curRegion);
        curRegion = nextRegion;
      }
      index++;
    }
    mergedRegions.add(curRegion);
    return mergedRegions;
  }

  public static long getTotalSize(Collection<RingRegion> regions) {
    long total;

    total = 0;
    for (RingRegion region : regions) {
      total += region.getSize();
    }
    return total;
  }

  public static BigDecimal getTotalFractionBD(Collection<RingRegion> regions) {
    return LongRingspace.longToFraction(getTotalSize(regions));
  }

  public static double getTotalFraction(Collection<RingRegion> regions) {
    return getTotalFractionBD(regions).doubleValue();
  }

  public boolean contains(long point) {
    if (end >= start) {
      return point >= start && point <= end;
    } else {
      return point <= end || point >= start;
    }
  }

  public boolean overlaps(RingRegion oRegion) {
    return contains(oRegion.start)
        || contains(oRegion.end)
        || oRegion.contains(start)
        || oRegion.contains(end);
  }

  public RingRegion shiftTo(long newStart) {
    return new RingRegion(
        newStart, LongRingspace.prevPoint(LongRingspace.add(newStart, getSize())));
  }

  public RingRegion trimOverlappingWith(RingRegion region) {
    return region.trimOverlappingIn(this);
  }

  /**
   * Trim portions of trimRegion that overlap with this region.
   *
   * @param trimRegion region to trim
   * @return trimmed trimRegion, null if it is completely overlapped
   */
  public RingRegion trimOverlappingIn(RingRegion trimRegion) {
    // for clarity, first handle completely overlapping
    // and completely non-overlapping cases
    if (contains(trimRegion.start) && contains(trimRegion.end)) {
      // trimRegion is completely contained within this region
      return null;
    } else if (trimRegion.contains(start) && trimRegion.contains(end)) {
      // this region is completely contained within trimRegion
      // we need to split the trim region up
      // but for our current use, we don't expect this case
      throw new RuntimeException("Unexpected trim case");
    } else if (!trimRegion.contains(start) && !trimRegion.contains(end)) {
      // completely non-overlapping
      return trimRegion;
    } else {
      // now handle partial overlaps
      if (contains(trimRegion.start)) {
        return new RingRegion(LongRingspace.nextPoint(end), trimRegion.end);
      } else {
        if (contains(trimRegion.end)) {
          return new RingRegion(trimRegion.start, LongRingspace.prevPoint(start));
        } else {
          throw new RuntimeException("panic");
        }
      }
    }
  }

  public static RingRegion trimOverlappingIn(
      Collection<RingRegion> regions, RingRegion trimRegion) {
    for (RingRegion region : regions) {
      trimRegion = region.trimOverlappingIn(trimRegion);
      if (trimRegion == null) {
        return null;
      }
    }
    return trimRegion;
  }

  private static void runTrimTests() {
    RingRegion region;

    log.info("Trimming");
    region = new RingRegion(100, 200);

    trimTest(region, 300, 400);
    trimTest(region, 100, 200);
    trimTest(region, 50, 150);
    trimTest(region, 150, 250);
    trimTest(region, 125, 175);
    trimTest(region, 400, 0);
    trimTest(region, 150, 0);
    trimTest(region, 400, 150);

    trimTest(
        -3074457345618258603L, -3074457345618258603L, -3074457345618258602L, -2767011611056432744L);
    trimTest(
        -3074457345618258603L, -3074457345618258603L, -4611686018427387903L, -3996794549303736184L);
  }

  private static void trimTest(long s1, long e1, long s2, long e2) {
    trimTest(new RingRegion(s1, e1), new RingRegion(s2, e2));
  }

  private static void trimTest(RingRegion region, long s2, long e2) {
    trimTest(region, new RingRegion(s2, e2));
  }

  private static void trimTest(RingRegion region, RingRegion trimRegion) {
    System.out.println(region + "\t" + trimRegion + "\t" + region.trimOverlappingIn(trimRegion));
  }

  public static IntersectionType intersectionType(RingRegion a, RingRegion b) {
    if (a.contains(b.start)) {
      if (a.contains(b.end)) {
        if (a.start == b.start && b.end == a.end) {
          // aaaaaa
          // bbbbbb
          return IntersectionType.isomorphic;
        } else {
          if (LongRingspace.clockwiseDistance(a.getStart(), b.getStart())
              <= LongRingspace.clockwiseDistance(a.getStart(), b.getEnd())) {
            // aaaaaaaa  aaaaaa  aaaaaa
            //   bbbb    bbbb      bbbb
            return IntersectionType.aSubsumesB;
          } else {
            if (a.getSize() == LongRingspace.size && b.getSize() == LongRingspace.size) {
              // We currently prevent this by normalizing all entire ringspace RingRegions
              // This is included for completeness
              return IntersectionType.nonIdenticalAllRingspace;
            } else {
              if (a.getSize() == LongRingspace.size) {
                return IntersectionType.aSubsumesB;
              } else if (b.getSize() == LongRingspace.size) {
                return IntersectionType.bSubsumesA;
              } else {
                return IntersectionType.wrappedPartial;
              }
            }
          }
        }
      } else {
        if (b.contains(a.start)) {
          // aaaa
          // bbbbbb
          return IntersectionType.bSubsumesA;
        } else {
          // aaaaaa
          //   bbbbbb
          return IntersectionType.abPartial;
        }
      }
    } else if (b.contains(a.start)) {
      if (b.contains(a.end)) {
        //   aaaa      aaaa
        // bbbbbbbb  bbbbbb
        return IntersectionType.bSubsumesA;
      } else {
        //   aaaaaa
        // bbbbbb
        return IntersectionType.baPartial;
      }
    } else {
      // aaaaaa
      //         bbbbbb
      return IntersectionType.disjoint;
    }
  }

  /**
   * Trim portions of trimRegion that overlap with this region. This routine should be favored over
   * case-specific logic.
   *
   * @return trimmed trimRegion, null if it is completely overlapped
   */
  public static IntersectionResult intersect(RingRegion a, RingRegion b) {
    IntersectionType intersectionType;
    List<RingRegion> aNonOverlapping;
    List<RingRegion> bNonOverlapping;
    List<RingRegion> overlapping;
    ImmutableList.Builder<RingRegion> builder;

    intersectionType = intersectionType(a, b);
    switch (intersectionType) {
      case isomorphic:
        // aaaaaa
        // bbbbbb
        aNonOverlapping = ImmutableList.of();
        bNonOverlapping = ImmutableList.of();
        overlapping = ImmutableList.of(a);
        break;
      case aSubsumesB:
        // aaaaaa  aaaaaaaa  aaaaaa
        // bbbb      bbbb      bbbb
        builder = ImmutableList.builder();
        if (a.start != b.start) {
          builder.add(new RingRegion(a.start, LongRingspace.prevPoint(b.start)));
        }
        if (a.end != b.end) {
          builder.add(new RingRegion(LongRingspace.nextPoint(b.end), a.end));
        }
        aNonOverlapping = builder.build();
        bNonOverlapping = ImmutableList.of();
        overlapping = ImmutableList.of(b);
        break;
      case abPartial:
        // aaaaaa
        //   bbbbbb
        aNonOverlapping =
            ImmutableList.of(new RingRegion(a.start, LongRingspace.prevPoint(b.start)));
        bNonOverlapping = ImmutableList.of(new RingRegion(LongRingspace.nextPoint(a.end), b.end));
        overlapping = ImmutableList.of(new RingRegion(b.start, a.end));
        break;
      case bSubsumesA:
        // aaaa      aaaa      aaaa
        // bbbbbb  bbbbbbbb  bbbbbb
        builder = ImmutableList.builder();
        aNonOverlapping = ImmutableList.of();
        if (a.start != b.start) {
          builder.add(new RingRegion(b.start, LongRingspace.prevPoint(a.start)));
        }
        if (a.end != b.end) {
          builder.add(new RingRegion(LongRingspace.nextPoint(a.end), b.end));
        }
        bNonOverlapping = builder.build();
        overlapping = ImmutableList.of(a);
        break;
      case baPartial:
        //   aaaaaa
        // bbbbbb
        aNonOverlapping = ImmutableList.of(new RingRegion(LongRingspace.nextPoint(b.end), a.end));
        bNonOverlapping =
            ImmutableList.of(new RingRegion(b.start, LongRingspace.prevPoint(a.start)));
        overlapping = ImmutableList.of(new RingRegion(a.start, b.end));
        break;
      case disjoint:
        // aaaaaa
        //         bbbbbb
        aNonOverlapping = ImmutableList.of();
        bNonOverlapping = ImmutableList.of();
        overlapping = ImmutableList.of();
        break;
      case wrappedPartial:
        //       aaaa  aaaa     aaaaaa
        //         bbbbbb     bbbb  bbbb

        aNonOverlapping =
            ImmutableList.of(
                new RingRegion(LongRingspace.nextPoint(b.end), LongRingspace.prevPoint(b.start)));
        bNonOverlapping =
            ImmutableList.of(
                new RingRegion(LongRingspace.nextPoint(a.end), LongRingspace.prevPoint(a.start)));
        builder = ImmutableList.builder();
        builder.add(new RingRegion(a.start, b.end));
        builder.add(new RingRegion(b.start, a.end));
        overlapping = builder.build();
        break;
      default:
        throw new RuntimeException("panic");
    }
    return new IntersectionResult(intersectionType, aNonOverlapping, bNonOverlapping, overlapping);
  }

  private static void testIntersection(RingRegion r1, RingRegion r2) {
    System.out.println(intersect(r1, r2));
  }

  private static void testIntersection2(RingRegion r1, String n1, RingRegion r2, String n2) {
    System.out.printf("%s %s\n", n1, n2);
    testIntersection(r1, r2);
    System.out.printf("%s %s\n", n2, n1);
    testIntersection(r2, r1);
  }

  public static void testIntersection() {
    testIntersection(new RingRegion(-100, 100), new RingRegion(0, 200));
    testIntersection(new RingRegion(0, 200), new RingRegion(-100, 100));
    testIntersection(new RingRegion(-100, 100), new RingRegion(-50, 50));
    testIntersection(new RingRegion(-50, 50), new RingRegion(-100, 100));
    System.out.println();
    testIntersection(new RingRegion(0, 200), new RingRegion(100, 300));
    testIntersection(new RingRegion(100, 300), new RingRegion(0, 200));
    testIntersection(new RingRegion(0, 300), new RingRegion(100, 200));
    testIntersection(new RingRegion(100, 200), new RingRegion(0, 300));
    System.out.println();
    testIntersection(new RingRegion(0, 0), new RingRegion(100, 300));
    testIntersection(new RingRegion(0, 0), new RingRegion(0, 0));
    System.out.println();
    testIntersection(new RingRegion(0, 100), new RingRegion(0, 50));
    testIntersection(new RingRegion(0, 100), new RingRegion(50, 100));
    testIntersection(new RingRegion(0, 50), new RingRegion(0, 100));
    testIntersection(new RingRegion(50, 100), new RingRegion(0, 100));

    System.out.println();
    RingRegion large0 = new RingRegion(LongRingspace.start + 100, LongRingspace.end - 100);
    RingRegion large_m1 = new RingRegion(LongRingspace.end - 200, LongRingspace.end - 300);
    RingRegion large_p1 = new RingRegion(LongRingspace.start + 300, LongRingspace.start + 100);
    RingRegion small0 = new RingRegion(LongRingspace.end - 200, LongRingspace.start + 200);

    System.out.printf("large0\t%s\n", large0);
    System.out.printf("large_m1\t%s\n", large_m1);
    System.out.printf("large_p1\t%s\n", large_p1);
    System.out.printf("small0\t%s\n", small0);

    testIntersection2(large0, "large0", small0, "small0"); // wrapped partial
    testIntersection2(large_m1, "large_m1", small0, "small0"); // wrapped partial
    testIntersection2(large_p1, "large_p1", small0, "small0"); // wrapped partial
    System.out.println();
    testIntersection2(large0, "large0", large_m1, "large_m1");
    testIntersection2(large0, "large0", large_p1, "large_p1");
    testIntersection2(large_m1, "large_m1", large_p1, "large_p1");
    System.out.println();
    System.out.println();
    testIntersection(
        new RingRegion(4459345355264627536L, 4470841357153488195L),
        new RingRegion(4469529232424279716L, 4469544142932566175L));
  }

  /////////////////////////////

  public RingRegion union(RingRegion oRegion) {
    return union(this, oRegion);
  }

  public static RingRegion union(RingRegion a, RingRegion b) {
    IntersectionType intersectionType;

    intersectionType = intersectionType(a, b);
    switch (intersectionType) {
      case isomorphic:
        // aaaaaa
        // bbbbbb
        return a;
      case aSubsumesB:
        // aaaaaa  aaaaaaaa  aaaaaa
        // bbbb      bbbb      bbbb
        return a;
      case abPartial:
        // aaaaaa
        //   bbbbbb
        return new RingRegion(a.start, b.end);
      case bSubsumesA:
        // aaaa      aaaa      aaaa
        // bbbbbb  bbbbbbbb  bbbbbb
        return b;
      case baPartial:
        //   aaaaaa
        // bbbbbb
        return new RingRegion(b.start, a.end);
      case disjoint:
        throw new RuntimeException("Can't union disjoint regions");
      default:
        throw new RuntimeException("panic");
    }
  }

  public static List<RingRegion> union(Collection<RingRegion>[] c) {
    List<RingRegion> regions;

    regions = new ArrayList<>();
    for (int i = 1; i < c.length; i++) {
      regions = union(regions, c[i]);
    }
    return regions;
  }

  public static List<RingRegion> union(Collection<RingRegion> c0, Collection<RingRegion> c1) {
    List<RingRegion> regions;

    regions = new ArrayList<>();
    regions.addAll(c0);
    regions.addAll(c1);
    return union(regions);
  }

  public static List<RingRegion> union(Collection<RingRegion> regions) {
    if (regions.size() == 0) {
      return ImmutableList.of();
    } else {
      RingRegion curRegion;
      List<RingRegion> sortedRegions;
      List<RingRegion> mergedRegions;
      int i;

      sortedRegions = new ArrayList<>(regions);
      Collections.sort(sortedRegions, positionComparator);
      mergedRegions = new ArrayList<>();
      curRegion = sortedRegions.get(0);
      i = 1;
      while (i < sortedRegions.size()) {
        if (curRegion.overlaps(sortedRegions.get(i))) {
          curRegion = curRegion.union(sortedRegions.get(i));
        } else {
          mergedRegions.add(curRegion);
          curRegion = sortedRegions.get(i);
        }
        i++;
      }
      mergedRegions.add(curRegion);
      return mergedRegions;
    }
  }

  public static Collection<RingRegion> union(
      List<RingRegion>[] regionsA, List<RingRegion> regionsB) {
    return RingRegion.union(RingRegion.union(regionsA), regionsB);
  }

  /////////////////////////////

  /**
   * Return a Comparator that orders points within this region from start to end.
   *
   * @return
   */
  public Comparator<Long> positionComparator() {
    return new PositionComparator();
  }

  /** Orders points within this region */
  private class PositionComparator implements Comparator<Long> {
    @Override
    public int compare(Long p0, Long p1) {
      long d0;
      long d1;

      d0 = LongRingspace.clockwiseDistance(start, p0);
      d1 = LongRingspace.clockwiseDistance(start, p1);
      if (d0 < d1) {
        return -1;
      } else if (d0 > d1) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public static String toString(List<RingRegion> regions) {
    return toString(regions, null);
  }

  public static String toString(List<RingRegion> regions, String separator) {
    StringBuilder sb;

    sb = new StringBuilder();
    for (RingRegion region : regions) {
      sb.append(region);
      if (separator != null) {
        sb.append(separator);
      }
    }
    return sb.toString();
  }

  // for unit testing
  public static void main(String[] args) {
    try {
      /*
      List<RingRegion>    r;

      r = allRingspace.divide(3);
      display(r);
      System.out.println("\nMerged");
      r = mergeAdjacent(r);
      display(r);
      runTrimTests();
      */
      testIntersection();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
