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
package com.ms.silverking.id;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.ms.silverking.numeric.NumConversion;

public class UUIDUtil {
  public static final int BYTES_PER_UUID = 16;

  public static byte[] uuidToBytes(UUID uuid) {
    byte[] b;

    b = new byte[BYTES_PER_UUID];
    uuidToBytes(b, uuid);
    return b;
  }

  public static void uuidToBytes(byte[] dest, UUID uuid) {
    uuidToBytes(dest, 0, uuid);
  }

  public static void uuidToBytes(byte[] dest, int offset, UUID uuid) {
    NumConversion.longToBytes(uuid.getMostSignificantBits(), dest, 0);
    NumConversion.longToBytes(uuid.getLeastSignificantBits(), dest, NumConversion.BYTES_PER_LONG);
  }

  public static UUID bytesToUUID(byte[] source) {
    return bytesToUUID(source, 0);
  }

  public static UUID bytesToUUID(byte[] source, int offset) {
    long msl;
    long lsl;

    msl = NumConversion.bytesToLong(source, offset);
    lsl = NumConversion.bytesToLong(source, offset + NumConversion.BYTES_PER_LONG);
    return new UUID(msl, lsl);
  }

  public static UUID getUUID(ByteBuffer buf) {
    return getUUID(buf, buf.position());
  }

  public static UUID getUUID(ByteBuffer buf, int position) {
    long msl;
    long lsl;

    msl = buf.getLong(position);
    lsl = buf.getLong(position + NumConversion.BYTES_PER_LONG);
    return new UUID(msl, lsl);
  }
}
