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

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.impl.SystemChecksum;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods for dealing with metadata stored as a byte array.
 * <p>
 * When stored with data, metadata is stored at the start of the byte array
 * followed by data.
 * <p>

 * Utility class for parsing metadata and data stored as byte[] in SK
 * Data is stored in the following order with specific offsets (see StorageFormat.writeToBuf on how data is put in the byte[])
 * Offsets are point in the ByteBuffer that tells where data for a specific value is stored (start index)
 * i.e. versionOffset 8 means data for version is stored from index 8 (inclusive) onwards
 *
 * |   Offset | Size (bytes) |                                                                                                      |
 * ---------------------------------------------------------------------------------------------------------------------------------|
 * |    0     |     4       | storedLength - all bytes required to store data and metadata including checksums, userdata, etc.      |
 * |    4     |     4       | uncompressedLength - length of value data (the value in key-value that the user stores)               |
 * |    8     |     4       | versionOffset                                                                                         |
 * |   16     |     8       | creationTimeOffset                                                                                    |
 * |   24     |     8       | creatorOffset                                                                                         |
 * |   32     |     2       | lockSecondsOffset                                                                                     |
 * |   34     |     2       | ccssOffset - (compression type, checksum type, StorageState)                                          |
 * |   36     |     1       | user data length                                                                                      |
 * |   37     |     16      | checksumOffset (16 assuming it's MD5)                                                                 |
 * |   53     |    ANY      | data                                                                                                  |
 * | 53 + data|    ANY      | userdata                                                                                              |
 *
 * When data is stored (via put), it first stores a header (16 bytes) and the key (16 bytes).
 * When data is retrieved, the first 32 bytes are removed and only leaves metadata
 *
 * //TODO (OPTIMUS-43326): Remove userdata. we don't want to store userdata on disk, because if user wants to store data, it should go together with the normal data
 */
public class MetaDataUtil {
  private static final int storedLengthOffset = 0;
  private static final int uncompressedLengthOffset = storedLengthOffset + NumConversion.BYTES_PER_INT; //(+ INT because storedLength occupies 4 bytes)
  private static final int versionOffset = uncompressedLengthOffset + NumConversion.BYTES_PER_INT; //(+ INT because uncompressedLength occupies 4 bytes)
  private static final int creationTimeOffset = versionOffset + NumConversion.BYTES_PER_LONG; //(+ LONG because version occupies 8 bytes)
  private static final int creatorOffset = creationTimeOffset + NumConversion.BYTES_PER_LONG; //(+ LONG because creationTime occupies 8 bytes)
  private static final int lockSecondsOffset = creatorOffset + ValueCreator.BYTES;
  private static final int ccss = lockSecondsOffset + NumConversion.BYTES_PER_SHORT; //(+ SHORT because lockSecond occupies 2 bytes)
  private static final int userDataLengthOffset = ccss + NumConversion.BYTES_PER_SHORT; //(+ SHORT because ccss occupies 2 bytes)
  // checksum if any is stored here
  public static final int dataOffset = userDataLengthOffset + 1; //(+ 1 because userDataLength occupies 1 byte)

  private static final int fixedMetaDataLength = dataOffset;

  private static Logger log = LoggerFactory.getLogger(MetaDataUtil.class);


  public static int getMinimumEntrySize() {
    return fixedMetaDataLength;
  }

  public static int computeStoredLength(int compressedLength, int checksumLength, int userDataLength) {
    return compressedLength + checksumLength + userDataLength + fixedMetaDataLength;
  }

  public static int computeMetaDataLength(int checksumLength, int userDataLength) {
    return checksumLength + userDataLength + fixedMetaDataLength;
  }

  // Stored Length

  public static int getStoredLength(ByteBuffer storedValue, int baseOffset) {
    return storedValue.getInt(baseOffset + storedLengthOffset);
  }

  public static int getStoredLength(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToInt(storedValue, baseOffset + storedLengthOffset);
  }

  // Uncompressed Length

  public static int getCompressedLength(ByteBuffer storedValue, int baseOffset) {
    int checksumLength = getChecksumLength(storedValue, baseOffset);
    return getStoredLength(storedValue, baseOffset) - checksumLength // property of namespace
           - getUserDataLength(storedValue, baseOffset) - fixedMetaDataLength;
  }

  public static int getUncompressedLength(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToInt(storedValue, baseOffset + uncompressedLengthOffset);
  }

  public static boolean isCompressed(byte[] storedValue, int baseOffset) {
    return getCompressedLength(storedValue, baseOffset) < getUncompressedLength(storedValue, baseOffset);
  }

  public static int getCompressedLength(byte[] storedValue, int baseOffset) {
    int checksumLength = getChecksumLength(storedValue, baseOffset);
    int userDataLength = getUserDataLength(storedValue, baseOffset);
    return getStoredLength(storedValue, baseOffset) - checksumLength - userDataLength - fixedMetaDataLength;
  }

  public static int getUncompressedLength(ByteBuffer storedValue, int baseOffset) {
    return storedValue.getInt(baseOffset + uncompressedLengthOffset);
  }

  // Version

  public static long getVersion(ByteBuffer buf, int baseOffset) {
    return buf.getLong(baseOffset + versionOffset);
  }

  public static long getVersion(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToLong(storedValue, baseOffset + versionOffset);
  }

  // Creation Time

  public static long getCreationTime(ByteBuffer buf, int baseOffset) {
    return buf.getLong(baseOffset + creationTimeOffset);
  }

  public static long getCreationTime(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToLong(storedValue, baseOffset + creationTimeOffset);
  }

  // Value Creator

  public static ValueCreator getCreator(ByteBuffer storedValue, int baseOffset) {
    return new SimpleValueCreator(storedValue, baseOffset + creatorOffset);
  }

  public static ValueCreator getCreator(byte[] storedValue, int baseOffset) {
    return new SimpleValueCreator(storedValue, baseOffset + creatorOffset);
  }

  // Lock Seconds

  public static short getLockSeconds(ByteBuffer buf, int baseOffset) {
    return buf.getShort(baseOffset + lockSecondsOffset);
  }

  public static short getLockSeconds(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToShort(storedValue, baseOffset + lockSecondsOffset);
  }

  // CCSS

  public static short getCCSS(ByteBuffer storedValue, int baseOffset) {
    return storedValue.getShort(baseOffset + ccss);
  }

  public static short getCCSS(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToShort(storedValue, baseOffset + ccss);
  }

  public byte getStorageState(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getStorageState(getCCSS(storedValue, baseOffset));
  }

  public static byte getCompression(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getCompression(getCCSS(storedValue, baseOffset));
  }

  public static byte getStorageState(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getStorageState(getCCSS(storedValue, baseOffset));
  }

  public static void updateStorageState(ByteBuffer storedValue, int baseOffset, byte storageState) {
    int offset = baseOffset + ccss;
    int oldCCSS = storedValue.getShort(offset);
    storedValue.putShort(offset, CCSSUtil.updateStorageState(oldCCSS, storageState));
  }

  public static byte getCompression(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getCompression(getCCSS(storedValue, baseOffset));
  }

  public static ChecksumType getChecksumType(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getChecksumType(getCCSS(storedValue, baseOffset));
  }

  // Checksum

  private static int getChecksumLength(ByteBuffer storedValue, int baseOffset) {
    ChecksumType checksumType = getChecksumType(storedValue, baseOffset);
    return checksumType.length();
  }

  public static ChecksumType getChecksumType(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getChecksumType(getCCSS(storedValue, baseOffset));
  }

  public static byte[] getChecksum(ByteBuffer storedValue, int baseOffset) {
    byte[] checksum = new byte[getChecksumLength(storedValue, baseOffset)];
    ((ByteBuffer) storedValue.asReadOnlyBuffer().position(baseOffset + userDataLengthOffset + 1)).get(checksum);
    return checksum;
  }

  private static int getChecksumLength(byte[] storedValue, int baseOffset) {
    ChecksumType checksumType = getChecksumType(storedValue, baseOffset);
    return checksumType.length();
  }

  public static byte[] getChecksum(byte[] storedValue, int baseOffset) {
    byte[] checksum = new byte[getChecksumLength(storedValue, baseOffset)];
    System.arraycopy(storedValue, baseOffset + userDataLengthOffset + 1, checksum, 0, checksum.length);
    return checksum;
  }

  // Data

  public static int getDataOffset(ChecksumType checksumType) {
    return dataOffset + checksumType.length();
  }

  public static int getDataOffset(byte[] storedValue, int baseOffset) {
    return baseOffset + dataOffset + getChecksumLength(storedValue, baseOffset);
  }

  public static int getDataOffset(ByteBuffer storedValue, int baseOffset) {
    return baseOffset + dataOffset + getChecksumLength(storedValue, baseOffset);
  }

  // User Data
  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  public static int getUserDataLength(ByteBuffer storedValue, int baseOffset) {
    return NumConversion.unsignedByteToInt(storedValue.get(baseOffset + userDataLengthOffset));
  }

  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  public static int getUserDataLength(byte[] storedValue, int baseOffset) {
    return NumConversion.unsignedByteToInt(storedValue, baseOffset + userDataLengthOffset);
  }

  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  private static int getUserDataOffset(byte[] storedValue, int baseOffset) {
    return dataOffset + getChecksumLength(storedValue, baseOffset) + getCompressedLength(storedValue, baseOffset);
  }

  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  public static byte[] getUserData(ByteBuffer storedValue, int baseOffset) {
    return getUserData(storedValue.array(), baseOffset);
  }

  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  public static byte[] getUserData(byte[] storedValue, int baseOffset) {
    int userDataLength = getUserDataLength(storedValue, baseOffset);
    byte[] userData = new byte[userDataLength];
    if (userDataLength > 0) {
      int userDataOffset = baseOffset + getUserDataOffset(storedValue, baseOffset);
      System.arraycopy(storedValue, userDataOffset, userData, 0, userDataLength);
      return userData;
    } else {
      return null;
    }
  }

  // Other helper methods

  public static boolean isInvalidation(ChecksumType checksumType, byte[] checksum) {
    if (checksumType == ChecksumType.SYSTEM) {
      return SystemChecksum.isInvalidationChecksum(checksum);
    } else {
      return false;
    }
  }

  public static boolean isInvalidated(byte[] storedValue, int baseOffset) {
    ChecksumType checksumType = getChecksumType(storedValue, baseOffset);
    if (checksumType == ChecksumType.SYSTEM) {
      byte[] actualChecksum = getChecksum(storedValue, baseOffset);
      return SystemChecksum.isInvalidationChecksum(actualChecksum);
    } else {
      return false;
    }
  }

  public static boolean isSegmented(byte[] storedValue, int baseOffset) {
    return ArrayUtil.equals(storedValue, baseOffset + creatorOffset, MetaDataConstants.segmentationBytes, 0,
                            MetaDataConstants.segmentationBytes.length);
  }

  public static boolean isSegmented(ByteBuffer buf) {
    // segmentation is indicated by segmentationBytes stored in the creator field
    return BufferUtil.equals(buf, creatorOffset, MetaDataConstants.segmentationBytes, 0,
                             MetaDataConstants.segmentationBytes.length);
  }

  public static boolean isInvalidation(ByteBuffer storedValue, int baseOffset) {
    ChecksumType checksumType = getChecksumType(storedValue, baseOffset);
    if (checksumType == ChecksumType.SYSTEM) {
      byte[] actualChecksum = getChecksum(storedValue, baseOffset);
      return SystemChecksum.isInvalidationChecksum(actualChecksum);
    } else {
      return false;
    }
  }

  public static int getMetaDataLength(ByteBuffer storedValue, int baseOffset) {
    // FIXME - this is ignoring user data
    // current plan is to copy userdata when it is present
    return fixedMetaDataLength + getChecksumLength(storedValue, baseOffset);
  }

  public static void testCorruption(ByteBuffer value, double corruptionProbability, int index) {
    if (ThreadLocalRandom.current().nextDouble() < corruptionProbability) {
      if (value != null && value.limit() > index) {
        log.warn("Corrupting {}", index);
        log.warn("  {}", StringUtil.byteBufferToHexString(value));
        value.put(index, (byte) (value.get(index) + 1));
        log.warn("=> {}", StringUtil.byteBufferToHexString(value));
      }
    }
  }
}
