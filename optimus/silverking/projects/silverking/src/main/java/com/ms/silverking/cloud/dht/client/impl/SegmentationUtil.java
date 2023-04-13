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

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

public class SegmentationUtil {
  static final int segmentedValueBufferLength = NumConversion.BYTES_PER_LONG + 3 * NumConversion.BYTES_PER_INT + 1;
  private static final int creatorBytesOffset = 0;
  private static final int storedLengthOffset = creatorBytesOffset + NumConversion.BYTES_PER_LONG;
  private static final int uncompressedLengthOffset = storedLengthOffset + NumConversion.BYTES_PER_INT;
  private static final int fragmentationThresholdOffset = uncompressedLengthOffset + NumConversion.BYTES_PER_INT;
  private static final int checksumTypeOffset = fragmentationThresholdOffset + NumConversion.BYTES_PER_INT;
  private static final int checksumOffset = checksumTypeOffset + 1;

  static int getNumSegments(int valueSize, int segmentSize) {
    return (valueSize - 1) / segmentSize + 1;
  }

  static byte[] getCreatorBytes(ByteBuffer buf) {
    byte[] creatorBytes;
    int dataStart;

    buf = buf.asReadOnlyBuffer();
    dataStart = buf.position();
    buf.position(dataStart + creatorBytesOffset);
    creatorBytes = new byte[ValueCreator.BYTES];
    buf.get(creatorBytes);
    return creatorBytes;
  }

  public static SimpleMetaData getMetaData(RawRetrievalResult rawResult, ByteBuffer buf) {
    SimpleMetaData metaData;

    metaData = new SimpleMetaData(rawResult);
    metaData = metaData.setStoredLength(getStoredLength(buf));
    metaData = metaData.setUncompressedLength(getUncompressedLength(buf));
    metaData = metaData.setValueCreator(new SimpleValueCreator(getValueCreatorBytes(buf)));
    metaData = metaData.setChecksumTypeAndChecksum(getChecksumType(buf), getChecksum(buf));
    //System.out.printf("%s %s %d\n", buf, getChecksumType(buf), getChecksum(buf).length);
    //System.out.printf("%s\n", StringUtil.byteBufferToHexString(buf));
    return metaData;
  }

  public static int getStoredLength(ByteBuffer buf) {
    return buf.getInt(buf.position() + storedLengthOffset);
  }

  public static int getUncompressedLength(ByteBuffer buf) {
    return buf.getInt(buf.position() + uncompressedLengthOffset);
  }

  static int getFragmentationThreshold(ByteBuffer buf) {
    return buf.getInt(buf.position() + fragmentationThresholdOffset);
  }

  static byte[] getValueCreatorBytes(ByteBuffer buf) {
    byte[] creatorBytes;

    creatorBytes = new byte[ValueCreator.BYTES];
    BufferUtil.get(buf, 0, creatorBytes, creatorBytes.length);
    return creatorBytes;
  }

  static ChecksumType getChecksumType(ByteBuffer buf) {
    return EnumValues.checksumType[buf.get(checksumTypeOffset)];
  }

  static byte[] getChecksum(ByteBuffer buf) {
    ChecksumType checksumType;
    byte[] checksum;

    checksumType = getChecksumType(buf);
    checksum = new byte[checksumType.length()];
    BufferUtil.get(buf, checksumOffset, checksum, checksumType.length());
    return checksum;
  }

  static ByteBuffer createSegmentMetaDataBuffer(byte[] creatorBytes, int storedLength, int uncompressedLength,
      int fragmentationThreshold, ChecksumType checksumType, byte[] checksum) {
    ByteBuffer segmentMetaDataBuffer;
    //Checksum    checksum;
    //ByteBuffer  checksumDest;

    //checksum = ChecksumProvider.getChecksum(checksumType);
    //System.out.printf("createSegmentMetaDataBuffer\t%s\t%d\t%d\n",
    //        StringUtil.byteArrayToHexString(creatorBytes), storedLength, uncompressedLength);
    segmentMetaDataBuffer = ByteBuffer.allocate(segmentedValueBufferLength + checksumType.length());
    // FIXME - checksum the index data
    if (creatorBytes.length != ValueCreator.BYTES) {
      throw new RuntimeException("Unexpected creatorBytes.length != ValueCreator.BYTES");
    }
    segmentMetaDataBuffer.put(creatorBytes);
    segmentMetaDataBuffer.putInt(storedLength);
    segmentMetaDataBuffer.putInt(uncompressedLength);
    segmentMetaDataBuffer.putInt(fragmentationThreshold);
    segmentMetaDataBuffer.put((byte) checksumType.ordinal());
    if (checksumType != ChecksumType.NONE) {
      segmentMetaDataBuffer.put(checksum);
    }

    //System.out.printf("%s %d\n", checksumType, checksum.length);
    //System.out.printf("%s %d\n", getChecksumType(segmentMetaDataBuffer), getChecksum(segmentMetaDataBuffer).length);
    //System.out.printf("%s\n", StringUtil.byteBufferToHexString((ByteBuffer)segmentMetaDataBuffer.asReadOnlyBuffer()
    // .position(0)));

    //System.out.println("source\t"+ StringUtil.byteBufferToHexString((ByteBuffer)segmentMetaDataBuffer
    // .asReadOnlyBuffer().flip()));
        
        /*
        checksumDest = segmentMetaDataBuffer.slice();
        checksum.checksum((ByteBuffer)segmentMetaDataBuffer.asReadOnlyBuffer().flip(), 
                          checksumDest);
                          */

    //System.out.println("dest\t"+ StringUtil.byteBufferToHexString((ByteBuffer)checksumDest.asReadOnlyBuffer().flip
    // ()));

    //System.out.println(StringUtil.byteBufferToHexString(segmentMetaDataBuffer));
    //System.out.println(checksumSegmentMetaDataBuffer(segmentMetaDataBuffer, checksumType));

    return segmentMetaDataBuffer;
  }

    /*
     * Not needed since the regular checksum mechanism is in place
   static boolean checksumSegmentMetaDataBuffer(ByteBuffer buf, ChecksumType checksumType) {
       Checksum     checksum;
       ByteBuffer   bufToChecksum;
       ByteBuffer   checksumOfBuf;
       ByteBuffer   bufInternalChecksum;
       
       checksum = ChecksumProvider.getChecksum(checksumType);
       bufToChecksum = (ByteBuffer)buf.asReadOnlyBuffer().flip().limit(segmentedValueBufferLength);
       //System.out.println("bufToChecksum      \t"+ StringUtil.byteBufferToHexString((ByteBuffer)bufToChecksum
       * .asReadOnlyBuffer().flip()));
       checksumOfBuf = ByteBuffer.wrap(checksum.checksum(bufToChecksum));
       bufInternalChecksum = (ByteBuffer)buf.asReadOnlyBuffer().position(segmentedValueBufferLength);
       //System.out.println("checksumOfBuf      \t"+ StringUtil.byteBufferToHexString(checksumOfBuf));
       //System.out.println("bufInternalChecksum\t"+ StringUtil.byteBufferToHexString(bufInternalChecksum));
       return checksumOfBuf.equals(bufInternalChecksum);
   }
   */
}
