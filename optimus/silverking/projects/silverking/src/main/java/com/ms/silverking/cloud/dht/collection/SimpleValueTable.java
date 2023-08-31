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
package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.numeric.NumConversion;

final class SimpleValueTable implements ValueTable {
  private final long[] keys;
  private final int[] values;
  private int nextEntry;

  private static final int mslOffset = 0;
  private static final int lslOffset = 1;
  private static final int entrySize = 2;

  SimpleValueTable(long[] keys, int[] values) {
    this.keys = keys;
    this.values = values;
  }

  public SimpleValueTable(int numKeys) {
    this(new long[numKeys * entrySize], new int[numKeys]);
  }

  @Override
  public int getSizeBytes() {
    return keys.length * NumConversion.BYTES_PER_LONG + values.length * NumConversion.BYTES_PER_INT;
  }

  public void clear() {
    for (int i = 0; i < values.length; i++) {
      values[i] = 0;
      keys[i * 2] = 0;
      keys[i * 2 + 1] = 0;
    }
    nextEntry = 0;
  }

  @Override
  public int add(long msl, long lsl, int value) {
    int index;

    index = nextEntry++;
    store(index, msl, lsl, value);
    return index;
  }

  @Override
  public void store(int index, long msl, long lsl, int value) {
    int baseOffset;

    baseOffset = index * entrySize;
    keys[baseOffset + mslOffset] = msl;
    keys[baseOffset + lslOffset] = lsl;
    values[index] = value;
    // System.out.printf("store values[%d]\t%d\n", index, values[index]);
  }

  @Override
  public int matches(int index, long msl, long lsl) {
    int baseOffset;

    baseOffset = index * entrySize;
    if (keys[baseOffset + mslOffset] == msl && keys[baseOffset + lslOffset] == lsl) {
      return values[index];
    } else {
      return noMatch;
    }
  }

  @Override
  public long getMSL(int index) {
    int baseOffset;

    baseOffset = index * entrySize;
    return keys[baseOffset + mslOffset];
  }

  @Override
  public long getLSL(int index) {
    int baseOffset;

    baseOffset = index * entrySize;
    return keys[baseOffset + lslOffset];
  }

  @Override
  public int getValue(int index) {
    // System.out.printf("getValue values[%d]\t%d\n", index, values[index]);
    return values[index];
  }

  long[] getKeys() {
    return keys;
  }

  int[] getValues() {
    return values;
  }
}
