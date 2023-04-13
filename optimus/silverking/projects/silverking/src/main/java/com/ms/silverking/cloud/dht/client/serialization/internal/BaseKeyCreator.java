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
package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.client.impl.KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.crypto.MD5KeyDigest;
import com.ms.silverking.cloud.dht.crypto.SHA1KeyDigest;
import com.ms.silverking.numeric.NumConversion;

public abstract class BaseKeyCreator<K> implements KeyCreator<K> {
  protected final MD5KeyDigest md5KeyDigest;
  protected final SHA1KeyDigest sha1KeyDigest;

  /*
   * Future - break apart the serialization portion and keydigest parts.
   */

  public BaseKeyCreator() {
    md5KeyDigest = new MD5KeyDigest();
    sha1KeyDigest = new SHA1KeyDigest();
  }

  private byte[] getSubKeyBytes(DHTKey key, int subKeyIndex) {
    byte[] keyBytes;

    keyBytes = new byte[2 * NumConversion.BYTES_PER_LONG];
    NumConversion.longToBytes(key.getMSL(), keyBytes, 0);
    NumConversion.longToBytes(key.getLSL() + subKeyIndex, keyBytes, NumConversion.BYTES_PER_LONG);
    return keyBytes;
  }

  @Override
  public DHTKey[] createSubKeys(DHTKey key, int numSubKeys) {
    DHTKey[] subKeys;

    subKeys = new DHTKey[numSubKeys];
    for (int i = 0; i < subKeys.length; i++) {
      subKeys[i] = md5KeyDigest.computeKey(getSubKeyBytes(key, i));
    }
    return subKeys;
  }
}
