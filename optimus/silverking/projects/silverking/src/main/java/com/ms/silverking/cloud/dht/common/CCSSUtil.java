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

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;

/**
 * Compression type, Checksum type, and StorageState
 *
 * <p>ccss format in bits: compression 4 checksum 4 storageState 8
 */
public class CCSSUtil {
  public static byte getCompression(short ccss) {
    return (byte) (ccss >> 12);
  }

  public static ChecksumType getChecksumType(short ccss) {
    // System.out.printf("ccss %x\n", ccss);
    return EnumValues.checksumType[(ccss >> 8) & 0x0f];
  }

  public static byte getStorageState(short ccss) {
    return (byte) (ccss & 0xff);
  }

  public static short updateStorageState(int oldCCSS, byte storageState) {
    return (short) ((oldCCSS & (short) 0xff00) | (storageState & 0xff));
  }

  public static short createCCSS(
      Compression compression, ChecksumType checksumType, int storageState) {
    return (short)
        ((compression.ordinal() << 12) | (checksumType.ordinal() << 8) | (storageState & 0xff));
  }

  public static short createCCSS(Compression compression, ChecksumType checksumType) {
    return (short) ((compression.ordinal() << 12) | (checksumType.ordinal() << 8));
  }

  public static short updateCompression(int oldCCSS, Compression compression) {
    return (short) ((oldCCSS & (short) 0xfff) | (compression.ordinal() << 12));
  }

  public static short updateChecksumType(int oldCCSS, ChecksumType checksumType) {
    return (short) ((oldCCSS & (short) 0xf0ff) | (checksumType.ordinal() << 8));
  }
}
