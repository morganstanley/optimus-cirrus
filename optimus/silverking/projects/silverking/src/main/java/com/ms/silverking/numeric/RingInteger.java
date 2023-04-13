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
package com.ms.silverking.numeric;

import java.util.Random;

import com.google.common.base.Preconditions;

public class RingInteger {
  final int min;
  final int max;
  int value;

  public RingInteger(int min, int max, int value) {
    Preconditions.checkArgument(max >= min);
    Preconditions.checkArgument(value >= min);
    Preconditions.checkArgument(value <= max);
    this.min = min;
    this.max = max;
    this.value = value;
  }

  public RingInteger(int max, int value) {
    this(0, max, value);
  }

  public int increment() {
    value++;
    if (value > max) {
      value = min;
    }
    return value;
  }

  public int decrement() {
    value--;
    if (value < min) {
      value = max;
    }
    return value;
  }

  public int getValue() {
    return value;
  }

  public static RingInteger createRandom(int min, int max) {
    return createRandom(new Random(), min, max);
  }

  public static RingInteger createRandom(Random random, int min, int max) {
    int offset;

    offset = random.nextInt(max - min + 1);
    return new RingInteger(min, max, min + offset);
  }

  public boolean equals(Object other) {
    RingInteger otherRingInteger;

    otherRingInteger = (RingInteger) other;
    return value == otherRingInteger.value;
  }

  public int hashCode() {
    return value;
  }

  public String toString() {
    return Integer.toString(value);
  }

  public static void ensureRingShared(RingInteger x0, RingInteger x1) {
    if (!ringShared(x0, x1)) {
      throw new RuntimeException("Ring not shared");
    }
  }

  public static boolean ringShared(RingInteger x0, RingInteger x1) {
    return x0.min == x1.min && x0.max == x1.max;
  }
}
