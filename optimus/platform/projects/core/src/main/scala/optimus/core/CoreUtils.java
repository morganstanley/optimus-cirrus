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
package optimus.core;

import java.util.ArrayList;
import java.util.List;

/** Generic utility functions */
public class CoreUtils {

  /** Return index >= 0 if the key is found or -1 otherwise */
  public static int indexOf(Object[] array, Object key) {
    if (array != null)
      for (int i = 0; i < array.length; ++i) {
        if (key == array[i]) return i;
      }
    return -1;
  }

  /** Returns true if the key is found in the array */
  public static boolean contains(Object[] array, Object key) {
    return indexOf(array, key) != -1;
  }

  /**
   * check if b is a subset of a - currently assumes mask arrays are the same length, and may not be
   * worth the cost of checking right now *
   */
  public static boolean subset(long[] a, long[] b) {
    for (int i = 0; i < a.length; i++) {
      if ((a[i] | b[i]) != a[i]) return false;
    }
    return true;
  }

  /** ORs bits from src into r */
  public static void merge(long[] r, long[] src) {
    if (src != null) {
      for (int i = 0; i < src.length; i++) {
        r[i] |= src[i];
      }
    }
  }

  /** Returns list of indexes for all the bits set on mask */
  public static ArrayList<Integer> maskToIndexList(long[] mask) {
    ArrayList<Integer> r = new ArrayList<>();
    if (mask != null) {
      for (int i = 0; i < mask.length; i++) {
        long maskI = mask[i];
        int id = (i << 6) + 1; // [REVERSE_INDEX_COMPUTE]
        while (maskI != 0) {
          if ((maskI & 1) != 0) r.add(id);
          maskI = maskI >>> 1;
          id++;
        }
      }
    }
    return r;
  }

  // this will be O(n*n) if it's a linked list, so force to ArrayList only
  static int[] toIntArray(ArrayList<Integer> list) {
    int[] r = new int[list.size()];
    for (int i = 0; i < r.length; ++i) r[i] = list.get(i);
    return r;
  }
}
