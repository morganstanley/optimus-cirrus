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
import java.util.Arrays;
import java.util.List;

/*
 * Contains array of pairs [offset1, mask1, offset2, mask2, ...]
 * Offsets are sorted i.e. offsetN < offsetK if only if N < K
 * Offsets are multiple of 32 and mask are the next 32 values
 * <p>
 * Merge: O( N + K )
 * Contains: O( ln(N) )
 * Consider:
 * 1. provide scratch array (on EC?) and avoid allocation
 * 2. detect result mask one of the inputs and avoid new result allocation
 * 3. maybe turn all these functions in to static, store just array and avoid allocating SparseBitSet object
 */
public class SparseBitSet {
  private static final int WORD_BITS = 5; // 5 bits gives you 32 bits
  private static final int WORD_MASK = 31;

  public static final SparseBitSet empty = new SparseBitSet(new int[] {0, 0});

  private int[] _data; // offset/bits

  public SparseBitSet(int index) {
    _data = new int[2];
    _data[0] = offset(index);
    _data[1] = bits(index);
  }

  private SparseBitSet(int[] data) {
    _data = data;
  }

  @Override
  public final String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < _data.length; i += 2) {
      int indexBase = _data[i] << WORD_BITS;
      int bits = _data[i + 1];
      for (int j = 0; j <= WORD_MASK; j++) {
        if ((bits & (1 << j)) != 0) {
          sb.append(indexBase + j);
          sb.append(' ');
        }
      }
    }
    if (sb.length() > 1) sb.setLength(sb.length() - 1); // Remove the last space
    sb.append("]");
    return sb.toString();
  }

  /* Binary search the offset and then check the mask */
  public final boolean contains(int index) {
    int offset = offset(index);
    int bits = bits(index);
    int lo = 0;
    int hi = (_data.length >>> 1) - 1; // Half
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int midVal = _data[mid << 1];
      if (offset > midVal) lo = mid + 1;
      else if (offset < midVal) hi = mid - 1;
      else {
        return (_data[(mid << 1) + 1] & bits) != 0; // Check the bit is set
      }
    }
    return false;
  }

  /* Same idea as in classical merge partitions in merge sort */
  public final SparseBitSet merge(SparseBitSet ws) {
    int ik = 0;
    int im = 0;
    int size = 0;
    int[] mdata = ws._data;
    // Scratch array of max result size. Maybe per thread...
    int[] ndata = new int[_data.length + mdata.length];
    while (true) {
      if (ik >= _data.length) {
        // take the rest of ws
        while (im < mdata.length) {
          ndata[size] = mdata[im];
          ndata[size + 1] = mdata[im + 1];
          im += 2;
          size += 2;
        }
        break;
      } else if (im >= mdata.length) {
        // take the rest of this
        while (ik < _data.length) {
          ndata[size] = _data[ik];
          ndata[size + 1] = _data[ik + 1];
          ik += 2;
          size += 2;
        }
        break;
      }

      int k = _data[ik];
      int l = mdata[im];
      if (k < l) {
        ndata[size] = k; // Copy offset
        ndata[size + 1] = _data[ik + 1]; // Copy bits
        size += 2;
        ik += 2;
      } else if (k > l) {
        ndata[size] = l; // Copy offset
        ndata[size + 1] = mdata[im + 1]; // Copy bits
        size += 2;
        im += 2;
      } else {
        ndata[size] = k; // Could have been = l
        ndata[size + 1] = _data[ik + 1] | mdata[im + 1]; // Or the bits
        ik += 2;
        im += 2;
        size += 2;
      }
    }
    // If no value diff detected return old array
    return new SparseBitSet(Arrays.copyOf(ndata, size));
  }

  private int offset(int index) {
    return index >>> WORD_BITS; // unsigned
  }

  private int bits(int index) {
    return 1 << (index & WORD_MASK);
  }
}
