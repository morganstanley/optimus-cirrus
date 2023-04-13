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

import java.util.Arrays;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;

/**
 * Key and value checksum pair for use in convergence.
 */
public class KeyValueChecksum {
  private final DHTKey key;
  private final byte[] valueChecksum;

  public KeyValueChecksum(DHTKey key, byte[] valueChecksum) {
    this.key = SimpleKey.of(key);
    this.valueChecksum = Arrays.copyOf(valueChecksum, valueChecksum.length);
  }

  public DHTKey getKey() {
    return key;
  }

  public byte[] getValueChecksum() {
    return valueChecksum;
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ NumConversion.bytesToInt(valueChecksum, valueChecksum.length - NumConversion.BYTES_PER_INT);
  }

  @Override
  public boolean equals(Object other) {
    KeyValueChecksum oKVC;

    oKVC = (KeyValueChecksum) other;
    return key.equals(oKVC.key) && Arrays.equals(valueChecksum, oKVC.valueChecksum);
  }

  @Override
  public String toString() {
    return key.toString() + " " + StringUtil.byteArrayToHexString(valueChecksum);
  }
}
