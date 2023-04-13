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
package com.ms.silverking.cloud.dht.crypto;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.client.impl.Checksum;

public class MD5Checksum implements Checksum {
  private static final byte[] emptyChecksum = new byte[MD5Digest.BYTES];

  @Override
  public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
    dest.put(checksum(source, position, length));
  }

  @Override
  public byte[] checksum(byte[] source, int position, int length) {
    MessageDigest md;

    md = MD5Digest.getLocalMessageDigest();
    md.update(source, position, length);
    return md.digest();
  }

  @Override
  public void checksum(ByteBuffer source, ByteBuffer dest) {
    dest.put(checksum(source));
  }

  @Override
  public byte[] checksum(ByteBuffer source) {
    MessageDigest md;

    md = MD5Digest.getLocalMessageDigest();
    md.update(source);
    return md.digest();
  }

  @Override
  public void emptyChecksum(ByteBuffer dest) {
    dest.put(emptyChecksum);
  }

  @Override
  public boolean isEmpty(byte[] checksum) {
    return Arrays.equals(checksum, emptyChecksum);
  }

  @Override
  public boolean uniquelyIdentifiesValues() {
    return true;
  }
}
