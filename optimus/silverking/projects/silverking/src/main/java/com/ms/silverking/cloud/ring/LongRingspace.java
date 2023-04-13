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
import java.math.MathContext;

import com.google.common.base.Preconditions;

/**
 * Provides methods for manipulating the portion of the Long value space
 * that we use as ring space. There is a difference due to the need
 * to be able to manipulate longs without problems such as overflow.
 * <p>
 * Note that we don't use the entire key space as our ringspace as we
 * have sufficient randomness in a much smaller space. This means
 * that many (logical) keys will map to the same coordinate even when
 * the keys themselves are distinct.
 */
public class LongRingspace {
  public static final MathContext mathContext = MathContext.DECIMAL128;

  public static final long halfRingspaceErrorRoom = 2000;

  public static BigDecimal newBD(long val) {
    return new BigDecimal(val, mathContext);
  }

  public static BigDecimal newBD(double val) {
    return new BigDecimal(val, mathContext);
  }

  public static final long start = (Long.MIN_VALUE >> 1) + halfRingspaceErrorRoom;
  public static final long end = (Long.MAX_VALUE >> 1) - halfRingspaceErrorRoom;
  public static final long size = end - start + 1;
  public static final BigDecimal bdSize = newBD(size);
  public static final BigDecimal bdStart = newBD(start);

  public static long longToRingspace(long l) {
    if (l == Long.MIN_VALUE) {
      return start;
    } else {
      return l >> 1;
    }
  }

  public static BigDecimal longToFraction(long l) {
    return newBD(l).divide(bdSize, mathContext);
  }

  public static long fractionToLong(double fraction) {
    assert fraction >= 0.0 && fraction <= 1.0;

    //return (long)((double)size * fraction);
    return bdSize.multiply(newBD(fraction)).longValue();
  }

  public static long fractionToLong(long dividend, long divisor) {
    assert divisor > 0 && dividend > 0 && dividend <= divisor;
    return fractionToLong(newBD(dividend).divide(newBD(divisor), mathContext));
  }

  public static long fractionToLong(BigDecimal fraction) {
    assert fraction.doubleValue() >= 0.0 && fraction.doubleValue() <= 1.0;
    return bdSize.multiply(fraction).longValue();
  }

  public static long addRingspace(long a, long b) {
    Preconditions.checkArgument(a >= 0);
    Preconditions.checkArgument(b >= 0);
    if (a == 0) {
      return b;
    } else if (b == 0) {
      return a;
    } else {
      BigDecimal sum;

      sum = newBD(a).add(newBD(b));
      if (sum.compareTo(bdSize) <= 0) {
        return sum.longValue();
      } else {
        return size;
      }
    }
  }

  public static long add(long p, long a) {
    Preconditions.checkArgument(a >= 0);
    long r;

    r = p + a;
    if (r > end) {
      r = start + (r - end) - 1;
    }
    return r;
  }

  public static long subtract(long p, long a) {
    Preconditions.checkArgument(a >= 0);
    long r;

    r = p - a;
    if (r < start) {
      r = end - (start - r - 1);
    }
    return r;
  }

  public static long clockwiseDistance(long p0, long p1) {
    if (p1 >= p0) {
      return p1 - p0;
    } else {
      return (end - p0) + (p1 - start) + 1;
    }
  }

  public static long nextPoint(long p) {
    long nextP;

    assert inRingspace(p);
    nextP = p + 1;
    if (nextP > end) {
      nextP = start;
    }
    return nextP;
  }

  public static long prevPoint(long p) {
    long prevP;

    assert inRingspace(p);
    prevP = p - 1;
    if (prevP < start) {
      prevP = end;
    }
    return prevP;
  }

  public static boolean inRingspace(long p) {
    return p >= start && p <= end;
  }

  public static void ensureInRingspace(long p) {
    if (!inRingspace(p)) {
      throw new RuntimeException(p + " not in LongRingspace");
    }
  }

  public static long mapRegionPointToRingspace(RingRegion region, long p) {
    long mp;

    assert region.contains(p);
    mp = newBD(p - region.getStart()).multiply(bdSize).divide(region.getSizeBD(), mathContext).add(bdStart).longValue();
    return longToRingspace(mp);
  }

  public static long mapChildRegionspacePointToParentRegion(RingRegion childRegionspace, long p,
      RingRegion parentRegion) {
    long mp;

    mp = newBD(p - childRegionspace.getStart()).multiply(parentRegion.getSizeBD()).divide(childRegionspace.getSizeBD(),
        mathContext).add(newBD(parentRegion.getStart())).longValue();
    return mp;
  }

  public static RingRegion mapChildRegionToParentRegion(RingRegion childRegionspace, RingRegion childRegion,
      RingRegion parent) {
    long mStart;
    long mEnd;

    mStart = mapChildRegionspacePointToParentRegion(childRegionspace, childRegion.getStart(), parent);
    mEnd = mapChildRegionspacePointToParentRegion(childRegionspace, childRegion.getEnd(), parent);
    return new RingRegion(mStart, mEnd);
  }

  public static boolean equalMagnitudes(long m1, long m2, long tolerance) {
    return Math.abs(m1 - m2) < tolerance;
  }

  public static void main(String[] args) {
    try {
      System.out.println(start);
      System.out.println(end);
      System.out.println(size);
      System.out.println(fractionToLong(0.0));
      System.out.println(fractionToLong(0.25));
      System.out.println(fractionToLong(0.5));
      System.out.println(fractionToLong(0.75));
      System.out.println(fractionToLong(1.0));
      System.out.println(fractionToLong(1.0 / 3.0));
      System.out.println(fractionToLong(1.0 / 3.0) * 3 + "\t" + size);
      System.out.println(fractionToLong(1.0 / 3.0) * 3 - size);
      System.out.println();

      System.out.println(fractionToLong(1, 3));
      System.out.println(fractionToLong(1, 3) * 3);
      System.out.println(fractionToLong(1, 3) * 3 - size);
      System.out.println();

      System.out.println(mapChildRegionspacePointToParentRegion(new RingRegion(0, 100), 50, new RingRegion(0, 200)));
      System.out.println(
          mapChildRegionToParentRegion(new RingRegion(0, 100), new RingRegion(0, 49), new RingRegion(0, 49)));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
