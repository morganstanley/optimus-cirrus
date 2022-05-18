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

import java.util.Random;

import com.ms.silverking.collection.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrayUtil<T> {
  private final Random random;

  public enum MismatchedLengthMode {Exception, Ignore};

  private static Logger log = LoggerFactory.getLogger(ArrayUtil.class);

  public static final byte[] emptyByteArray = new byte[0];

  public ArrayUtil() {
    random = new Random();
  }

  public void shuffle(T[] a) {
    for (int i = 0; i < a.length; i++) {
      int newPosition;
      T temp;

      newPosition = random.nextInt(a.length);
      temp = a[newPosition];
      a[newPosition] = a[i];
      a[i] = temp;
    }
  }

  public boolean equals(T[] a, int startA, T[] b) {
    return equals(a, startA, b, 0, b.length);
  }

  public boolean equals(T[] a, int startA, T[] b, int startB, int length) {
    int ai;
    int bi;

    ai = startA;
    bi = startB;
    for (int i = 0; i < length; i++) {
      if (a[ai] != b[bi]) {
        return false;
      }
      ai++;
      bi++;
    }
    return true;
  }

  public static boolean equals(byte[] a, int startA, byte[] b) {
    return equals(a, startA, b, 0, b.length);
  }

  public static boolean equals(byte[] a, int startA, byte[] b, int startB, int length) {
    int ai;
    int bi;

    ai = startA;
    bi = startB;
    for (int i = 0; i < length; i++) {
      if (a[ai] != b[bi]) {
        return false;
      }
      ai++;
      bi++;
    }
    return true;
  }

  public static int compareSigned(byte[] a, byte[] b) {
    return compareSigned(a, b, MismatchedLengthMode.Exception);
  }

  public static int compareSigned(byte[] a, byte[] b, MismatchedLengthMode mismatchedLengthMode) {
    if (a.length != b.length) {
      if (mismatchedLengthMode == MismatchedLengthMode.Exception) {
        throw new RuntimeException("Mismatched lengths " + a.length + " " + b.length);
      }
    }
    return compareSigned(a, 0, b, 0, a.length);
  }

  /**
   * Compare two byte arrays. Handle degenerate cases so that we impose a total
   * ordering on all byte arrays.
   */
  public static int compareSignedForOrdering(byte[] a, byte[] b) {
    if (a == null) {
      if (b == null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (b == null) {
        return 1;
      } else {
        if (a.length != b.length) {
          int c;

          c = compareSigned(a, 0, b, 0, Math.min(a.length, b.length));
          if (c != 0) {
            return c;
          } else {
            if (a.length < b.length) {
              return -1;
            } else {
              return 1;
            }
          }
        } else {
          return compareSigned(a, 0, b, 0, a.length);
        }
      }
    }
  }

  private static int compareSigned(byte[] a, int startA, byte[] b) {
    return compareSigned(a, startA, b, 0, b.length);
  }

  private static int compareSigned(byte[] a, int startA, byte[] b, int startB, int length) {
    int ai;
    int bi;

    ai = startA;
    bi = startB;
    for (int i = 0; i < length; i++) {
      if (a[ai] < b[bi]) {
        return -1;
      } else if (a[ai] != b[bi]) {
        return 1;
      }
      ai++;
      bi++;
    }
    return 0;
  }

  private static int compareUnsigned(byte[] a, int startA, byte[] b, int startB, int length) {
    int ai;
    int bi;

    ai = startA;
    bi = startB;
    for (int i = 0; i < length; i++) {
      int ua;
      int ub;

      ua = Byte.toUnsignedInt(a[ai]);
      ub = Byte.toUnsignedInt(b[bi]);
      if (ua < ub) {
        return -1;
      } else if (ua != ub) {
        return 1;
      }
      ai++;
      bi++;
    }
    return 0;
  }

  public static int compareSigned(byte[] a, int startA, int lengthA, byte[] b, int startB, int lengthB) {
    int result;

    result = compareSigned(a, startA, b, startB, Math.min(lengthA, lengthB));
    if (result != 0) {
      return result;
    } else {
      if (lengthA == lengthB) {
        return 0;
      } else {
        if (lengthA < lengthB) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }

  public static int compareUnsigned(byte[] a, int startA, int lengthA, byte[] b, int startB, int lengthB) {
    int result;

    result = compareUnsigned(a, startA, b, startB, Math.min(lengthA, lengthB));
    if (result != 0) {
      return result;
    } else {
      if (lengthA == lengthB) {
        return 0;
      } else {
        if (lengthA < lengthB) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }

  public static void display(byte[] array) {
    for (int i = 0; i < array.length; i++) {
      log.info("{}:  {}",i , array[i]);
    }
  }

  public static void xor(byte[] a1, byte[] a2) {
    if (a1.length != a2.length) {
      log.info("{} != {}", a1.length, a2.length);
      throw new RuntimeException("a1.length != a2.length");
    }
    for (int i = 0; i < a1.length; i++) {
      a1[i] ^= a2[i];
    }
  }

  public static Double[] doubleToDouble(double[] a1) {
    Double[] a2;

    a2 = new Double[a1.length];
    for (int i = 0; i < a1.length; i++) {
      a2[i] = a1[i];
    }
    return a2;
  }

  public static int hashCode(byte[] a) {
    return hashCode(a, 0, a.length);
  }

  public static int hashCode(byte[] a, int offset, int length) {
    if (length > 0) {
      int h;
      int _offset;

      _offset = offset;
      h = 0;
      for (int i = 0; i < length; i++) {
        h = 31 * h + a[_offset++];
      }
      return h;
    } else {
      return 0;
    }
  }

  public static <K> String toString(K[] c) {
    return toString(c, CollectionUtil.defaultSeparator);
  }

  public static <K> String toString(K[] c, char separator) {
    return toString(c, CollectionUtil.defaultStartBrace, CollectionUtil.defaultEndBrace, separator,
        CollectionUtil.defaultEmptyDef);
  }

  public static <K> String toString(K[] c, String startBrace, String endBrace, char separator, String emptyDef) {
    return CollectionUtil.toString(java.util.Arrays.asList(c), startBrace, endBrace, separator, emptyDef);
  }

  public static <K> boolean containsNull(K[] c) {
    for (K item : c) {
      if (item == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compute xor a1 ^ a2, updating a1
   *
   * @param a1
   * @param a2
   */
  public static void xorInPlace(byte[] a1, byte[] a2) {
    if (a1.length != a2.length) {
      throw new RuntimeException("a1.length != a2.length");
    }
    for (int i = 0; i < a1.length; i++) {
      a1[i] ^= a2[i];
    }
  }

  public static <K> void clear(K[] a) {
    for (int i = 0; i < a.length; i++) {
      a[i] = null;
    }
  }
}
