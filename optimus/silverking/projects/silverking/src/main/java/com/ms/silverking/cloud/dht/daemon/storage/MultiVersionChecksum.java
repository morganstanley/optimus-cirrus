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
package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.crypto.MD5Checksum;
import com.ms.silverking.cloud.dht.crypto.MD5Digest;
import com.ms.silverking.cloud.dht.crypto.MD5Util;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

/**
 * Used by multi-version convergence to compute checksums for all versions of values stored.
 * Currently, this is computed as the xor of all md5 hashes of all <version, storageTime> pairs.
 * As such, the order of the values doesn't matter. Prob(xor checksum collision) is low enough to be ignored.
 */
public class MultiVersionChecksum {
  private final byte[] checksum;

  // For simplicity, we use the client-visible MD5Checksum class.
  // We do not, however, want to modify this class for our convenience here.
  // We simply use it as-is.
  private static final MD5Checksum md5 = new MD5Checksum();

  private static final int SIZE_BYTES = MD5Digest.BYTES;

  public static MultiVersionChecksum fromKey(DHTKey key) {
    MultiVersionChecksum mvc;

    mvc = new MultiVersionChecksum();
    mvc.addKey(key);
    return mvc;
  }

  public void addKey(DHTKey key) {
    byte[] b;

    b = new byte[NumConversion.BYTES_PER_LONG * 2];
    NumConversion.longToBytes(key.getMSL(), b, 0);
    NumConversion.longToBytes(key.getLSL(), b, NumConversion.BYTES_PER_LONG);
    ArrayUtil.xorInPlace(checksum, b);
  }

  protected MultiVersionChecksum() {
    checksum = new byte[SIZE_BYTES];
  }

  public void addVersionAndStorageTime(long version, long storageTime) {
    byte[] b;

    b = new byte[NumConversion.BYTES_PER_LONG * 2];
    NumConversion.longToBytes(version, b, 0);
    NumConversion.longToBytes(storageTime, b, NumConversion.BYTES_PER_LONG);
    ArrayUtil.xorInPlace(checksum, md5.checksum(b, 0, b.length));
  }

  @Override
  public int hashCode() {
    return MD5Util.hashCode(checksum);
  }

  @Override
  public boolean equals(Object obj) {
    MultiVersionChecksum o;

    o = (MultiVersionChecksum) obj;
    return ArrayUtil.equals(checksum, 0, o.checksum);
  }

  @Override
  public String toString() {
    return StringUtil.byteArrayToHexString(checksum);
  }

  public long getLongChecksum() {
    return NumConversion.bytesToLong(checksum, 0) ^ NumConversion.bytesToLong(checksum, NumConversion.BYTES_PER_LONG);
  }
}
