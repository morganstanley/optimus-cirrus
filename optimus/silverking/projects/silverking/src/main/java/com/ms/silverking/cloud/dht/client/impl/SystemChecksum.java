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
package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.numeric.NumConversion;

public class SystemChecksum implements Checksum {
  public static final int BYTES = 8;

  private static final byte[] emptyChecksum = new byte[BYTES];
  private static final byte[] invalidationChecksum;

  static {
    invalidationChecksum = NumConversion.longToBytes(0xdeadbeef);
  }

  public SystemChecksum() {}

  @Override
  public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
    dest.put(checksum(source, position, length));
  }

  @Override
  public byte[] checksum(byte[] source, int position, int length) {
    return invalidationChecksum;
  }

  @Override
  public void checksum(ByteBuffer source, ByteBuffer dest) {
    dest.put(checksum(source));
  }

  @Override
  public byte[] checksum(ByteBuffer source) {
    return invalidationChecksum;
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
    return false;
  }

  public static boolean isInvalidationChecksum(byte[] actualChecksum) {
    return Arrays.equals(actualChecksum, invalidationChecksum);
  }

  public static byte[] getInvalidationChecksum() {
    return invalidationChecksum;
  }
}
