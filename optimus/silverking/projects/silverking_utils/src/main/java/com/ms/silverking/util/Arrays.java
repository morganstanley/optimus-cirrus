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
package com.ms.silverking.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;

public class Arrays {
  public static boolean matchesRegion(byte[] a1, int offset1, byte[] a2, int offset2, int length) {
    for (int i = 0; i < length; i++) {
      if (a1[offset1 + i] != a2[offset2 + i]) {
        return false;
      }
    }
    return true;
  }

  public static boolean matchesStart(byte[] a1, byte[] a2) {
    for (int i = 0; i < a1.length; i++) {
      if (a1[i] != a2[i]) {
        return false;
      }
    }
    return true;
  }

  public static <T> int indexOf(T[] a1, T v) {
    for (int i = 0; i < a1.length; i++) {
      if (a1[i].equals(v)) {
        return i;
      }
    }
    return -1;
  }

  public static <T> int lastIndexOf(T[] a1, T v) {
    for (int i = a1.length - 1; i >= 0; i--) {
      if (a1[i].equals(v)) {
        return i;
      }
    }
    return -1;
  }

  public static int indexOf(byte[] a1, byte v) {
    for (int i = 0; i < a1.length; i++) {
      if (a1[i] == v) {
        return i;
      }
    }
    return -1;
  }

  public static int lastIndexOf(byte[] a1, byte v) {
    for (int i = a1.length - 1; i >= 0; i--) {
      if (a1[i] == v) {
        return i;
      }
    }
    return -1;
  }

  public static <T> boolean contains(T[] a1, T v) {
    return indexOf(a1, v) >= 0;
  }

  public static void shuffleIntArray(int[] a) {
    shuffleIntArray(a, ThreadLocalRandom.current());
  }

  public static void shuffleIntArray(int[] a, Random random) {
    int index;

    for (int i = a.length - 1; i > 0; i--) {
      index = random.nextInt(i + 1);
      if (index != i) {
        a[index] ^= a[i];
        a[i] ^= a[index];
        a[index] ^= a[i];
      }
    }
  }

  public static int[] randomUniqueIntArray(int max) {
    return randomUniqueIntArray(max, ThreadLocalRandom.current());
  }

  public static int[] randomUniqueIntArray(int max, Random random) {
    return randomUniqueIntArray(0, max, random);
  }

  public static int[] randomUniqueIntArray(int min, int max) {
    return randomUniqueIntArray(min, max, ThreadLocalRandom.current());
  }

  /**
   * @param min
   * @param max max exclusive
   * @param random
   * @return
   */
  public static int[] randomUniqueIntArray(int min, int max, Random random) {
    int[] x;
    int size;

    Preconditions.checkArgument(min >= 0, "min must be non-negative");
    Preconditions.checkArgument(max >= 0, "max must be non-negative");
    Preconditions.checkArgument(max > min, "max must be > min");
    size = max - min;
    x = new int[size];
    for (int i = 0; i < x.length; i++) {
      x[i] = min + i;
    }
    Arrays.shuffleIntArray(x, random);
    return x;
  }

  public static <T> Iterator<T> iterator(T[] a) {
    return new ArrayIterator(a);
  }

  private static class ArrayIterator<T> implements Iterator<T> {
    private T[] a;
    private int i;

    ArrayIterator(T[] a) {
      this.a = a;
    }

    @Override
    public boolean hasNext() {
      return i < a.length;
    }

    @Override
    public T next() {
      if (hasNext()) {
        T e;

        e = a[i];
        i++;
        return e;
      } else {
        throw new NoSuchElementException();
      }
    }
  }
}
