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
import java.util.Arrays;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.cloud.dht.client.impl.ChecksumProvider;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Decompressor;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueUtil {
  private static final boolean debugChecksum = false;

  private static Logger log = LoggerFactory.getLogger(ValueUtil.class);

  public static final ByteBuffer corruptValue = ByteBuffer.allocate(0);

  public static void verifyChecksum(ByteBuffer storedValue) throws CorruptValueException {
    if (storedValue != null) {
      int baseOffset;
      byte[] storedData;
      byte[] dataToVerify;
      int verifyDataOffset;
      int dataOffset;
      int compressedLength;
      int uncompressedLength;
      Compression compression;

      storedValue = storedValue.duplicate(); // don't alter the source buffer
      baseOffset = storedValue.position();
      storedData = new byte[storedValue.limit()];
      storedValue.get(storedData);
      compressedLength = MetaDataUtil.getCompressedLength(storedData, baseOffset);
      uncompressedLength = MetaDataUtil.getUncompressedLength(storedData, baseOffset);
      if (debugChecksum) {
        log.debug("compressedLength: {}", compressedLength);
        log.debug("uncompressedLength: {}", uncompressedLength);
      }
      dataOffset = MetaDataUtil.getDataOffset(storedData, baseOffset);
      compression = EnumValues.compression[MetaDataUtil.getCompression(storedData, baseOffset)];
      if (MetaDataUtil.isCompressed(storedData, baseOffset)) {
        byte[] uncompressedData;
        Decompressor decompressor;

        log.debug("Compressed");
        decompressor = CodecProvider.getDecompressor(compression);
        try {
          // System.out.println(compression +" "+ decompressor);
          uncompressedData =
              decompressor.decompress(storedData, dataOffset, compressedLength, uncompressedLength);
          dataToVerify = uncompressedData;
          verifyDataOffset = 0;
        } catch (Exception e) {
          throw new CorruptValueException(e);
        }
      } else {
        dataToVerify = storedData;
        verifyDataOffset = dataOffset;
      }
      verifyChecksum(storedData, baseOffset, dataToVerify, verifyDataOffset, uncompressedLength);
      log.info("Checksum OK");
    }
  }

  public static boolean isInvalidated(byte[] storedValue, int storedOffset) {
    return MetaDataUtil.isInvalidated(storedValue, storedOffset);
  }

  public static void verifyChecksum(
      byte[] storedValue, int storedOffset, byte[] value, int valueOffset, int valueLength)
      throws CorruptValueException {
    byte[] expectedChecksum;
    byte[] actualChecksum;
    ChecksumType checksumType;
    Checksum checksum;

    if (debugChecksum) {
      log.debug("storedValue: {}", StringUtil.byteArrayToHexString(storedValue));
    }
    checksumType = MetaDataUtil.getChecksumType(storedValue, storedOffset);
    if (checksumType != ChecksumType.NONE) {
      actualChecksum = MetaDataUtil.getChecksum(storedValue, storedOffset);
      checksum = ChecksumProvider.getChecksum(checksumType);
      if (!checksum.isEmpty(actualChecksum)) {
        expectedChecksum = checksum.checksum(value, valueOffset, valueLength);
        if (debugChecksum) {
          log.debug("valueOffset: {}", valueOffset);
          log.debug("valueLength: {}", valueLength);
          log.debug(
              "value: {}  {}",
              StringUtil.byteArrayToHexString(value, valueOffset, valueLength),
              valueLength);
          log.debug("value: {}", new String(value, valueOffset, valueLength));
          log.debug("expectedChecksum: {}", StringUtil.byteArrayToHexString(expectedChecksum));
          log.debug("actualChecksum: {}", StringUtil.byteArrayToHexString(actualChecksum));
          System.out.flush();
        }
        if (!Arrays.equals(expectedChecksum, actualChecksum)) {
          throw new CorruptValueException(
              StringUtil.byteArrayToHexString(actualChecksum)
                  + " != "
                  + StringUtil.byteArrayToHexString(expectedChecksum));
        }
      }
    }
  }
}
