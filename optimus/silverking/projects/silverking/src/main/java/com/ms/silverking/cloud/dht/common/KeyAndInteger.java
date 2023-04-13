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
package com.ms.silverking.cloud.dht.common;

import java.util.Comparator;

public class KeyAndInteger implements DHTKey {
  private final long msl;
  private final long lsl;
  private final int integer;

  public KeyAndInteger(long msl, long lsl, int integer) {
    this.msl = msl;
    this.lsl = lsl;
    this.integer = integer;
  }

  public KeyAndInteger(DHTKey key, int integer) {
    this.msl = key.getMSL();
    this.lsl = key.getLSL();
    this.integer = integer;
  }

  @Override
  public long getMSL() {
    return msl;
  }

  @Override
  public long getLSL() {
    return lsl;
  }

  public int getInteger() {
    return integer;
  }

  @Override
  public int hashCode() {
    // this presumes that this key is strongly random
    // works fine for crypto-hash-derived keys
    return (int) lsl ^ integer;
  }

  @Override
  public boolean equals(Object o) {
    KeyAndInteger oKey;

    oKey = (KeyAndInteger) o;
    return lsl == oKey.getLSL() && msl == oKey.getMSL() && integer == oKey.integer;
  }

  @Override
  public String toString() {
    return KeyUtil.keyToString(this) + ":" + integer;
  }

  public static Comparator<KeyAndInteger> getIntegerComparator() {
    return integerComparator;
  }

  private static final Comparator<KeyAndInteger> integerComparator = new IntegerComparator();

  private static class IntegerComparator implements Comparator<KeyAndInteger> {
    @Override
    public int compare(KeyAndInteger k1, KeyAndInteger k2) {
      if (k1.integer < k2.integer) {
        return -1;
      } else if (k1.integer > k2.integer) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
