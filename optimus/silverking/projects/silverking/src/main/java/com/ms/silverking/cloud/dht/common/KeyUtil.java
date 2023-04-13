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

import java.math.BigInteger;
import java.math.MathContext;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.numeric.EntropyCalculator;
import com.ms.silverking.numeric.NumConversion;

public class KeyUtil {
  public static final MathContext mathContext = MathContext.DECIMAL128;

  private static final byte[] _minValue = new byte[DHTKey.BYTES_PER_KEY];
  private static final byte[] _maxValue = new byte[DHTKey.BYTES_PER_KEY + 1];

  static {
    for (int i = 0; i < DHTKey.BYTES_PER_KEY; i++) {
      _maxValue[i + 1] = (byte) 0xff;
    }
  }

  public static final BigInteger minValue = new BigInteger(_minValue);
  public static final BigInteger maxValue = new BigInteger(_maxValue);

  public static BigInteger keyToBigInteger(DHTKey key) {
    long msl;
    long lsl;
    byte[] keyBytes;

    keyBytes = new byte[DHTKey.BYTES_PER_KEY + 1];
    msl = key.getMSL();
    lsl = key.getLSL();
    NumConversion.longToBytes(msl, keyBytes, 1);
    NumConversion.longToBytes(lsl, keyBytes, NumConversion.BYTES_PER_LONG + 1);
    return new BigInteger(keyBytes);
  }

  public static DHTKey bigIntegerToKey(BigInteger _key) {
    byte[] bytes;
    byte[] _bytes;
    int start;

    _bytes = _key.toByteArray();
    bytes = new byte[DHTKey.BYTES_PER_KEY];
    //System.out.println(StringUtil.byteArrayToHexString(_key.toByteArray()));
    //System.out.println(bytes.length +" "+ _bytes.length);
    if (_bytes.length > DHTKey.BYTES_PER_KEY) {
      start = _bytes.length - DHTKey.BYTES_PER_KEY;
    } else {
      start = 0;
    }
    System.arraycopy(_bytes, start, bytes, bytes.length - _bytes.length + start, _bytes.length - start);
    return new SimpleKey(bytes);
  }

  public static void test() {
    Random random;

    random = new Random();
    for (int i = 0; i < 1024; i++) {
      SimpleKey key1;
      BigInteger _key1;
      DHTKey key2;

      key1 = new SimpleKey(random.nextLong(), random.nextLong());
      _key1 = keyToBigInteger(key1);
      key2 = bigIntegerToKey(_key1);
      //System.out.println(key1 +"\t"+ key2);
      if (!key1.equals(key2)) {
        throw new RuntimeException("key mismatch");
      }
    }
  }

  public static long keyToCoordinate(DHTKey key) {
    // We use MSL to keep the ordering of keys
    // consistent with the ordering of coordinates.
    return LongRingspace.longToRingspace(key.getMSL());
  }

  public static DHTKey arbitraryKeyForCoordinate(long p) {
    return new SimpleKey(0, p << 1);
  }

  public static DHTKey randomRegionKey(RingRegion region) {
    long p;

    p = Math.abs(ThreadLocalRandom.current().nextLong()) % region.getSize() + region.getStart();
    return arbitraryKeyForCoordinate(p);
  }

  public static byte[] keyToBytes(DHTKey key) {
    byte[] b;

    b = new byte[DHTKey.BYTES_PER_KEY];
    NumConversion.longToBytes(key.getMSL(), b, 0);
    NumConversion.longToBytes(key.getLSL(), b, NumConversion.BYTES_PER_LONG);
    return b;
  }

  public static double keyEntropy(DHTKey key) {
    return EntropyCalculator.computeEntropy(keyToBytes(key));
  }

  public static boolean keySatisfiesEntropy(DHTKey key, double threshold) {
    return EntropyCalculator.computeEntropy(keyToBytes(key)) > threshold;
  }

  public static String keyToString(DHTKey key) {
    return String.format("%x:%x", key.getMSL(), key.getLSL());
  }

  public static DHTKey keyStringToKey(String def) {
    String[] s;

    s = def.split(":");
    return new SimpleKey(Long.parseUnsignedLong(s[0], 16), Long.parseUnsignedLong(s[1], 16));
  }

  public static boolean equal(DHTKey k1, DHTKey k2) {
    return k1.getMSL() == k2.getMSL() && k1.getLSL() == k2.getLSL();
  }

  public static int compare(DHTKey k1, DHTKey k2) {
    return DHTKeyComparator.dhtKeyComparator.compare(k1, k2);
  }

  /**
   * @param args
   */
 }
