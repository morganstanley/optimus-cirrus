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
package com.ms.silverking.cloud.skfs.dir.serverside;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageParameterSerializer {

  private static Logger log = LoggerFactory.getLogger(StorageParameterSerializer.class);

  // FUTURE - deprecate this class

  private static final int BASE_SERIALIZED_SIZE =
      NumConversion.BYTES_PER_LONG * 2 + NumConversion.BYTES_PER_INT * 2 + NumConversion.BYTES_PER_SHORT + ValueCreator.BYTES + 1;

  public static int getSerializedLength(SSStorageParameters p) {
    return BASE_SERIALIZED_SIZE + p.getChecksumType().length();
  }

  public static byte[] serialize(SSStorageParameters p) {
    ByteBuffer buf;

    buf = ByteBuffer.allocate(getSerializedLength(p));
    buf.putInt(p.getUncompressedSize());
    buf.putInt(p.getCompressedSize());
    buf.putLong(p.getVersion());
    buf.putLong(p.getCreationTime());
    buf.putShort(p.getLockSeconds());
    buf.put(p.getValueCreator());
    buf.putShort(CCSSUtil.createCCSS(p.getCompression(), p.getChecksumType(), p.getStorageState()));
    buf.put((byte) 0);
    buf.put(p.getChecksum());
    return buf.array();
  }

  public static SSStorageParameters deserialize(byte[] a) {
    return deserialize(a, 0);
  }

  public static SSStorageParameters deserialize(byte[] a, int offset) {
    return _deserialize(ByteBuffer.wrap(a, offset, a.length - offset), offset);
  }

  public static SSStorageParameters deserialize(ByteBuffer b) {
    return deserialize(b, 0);
  }

  public static SSStorageParameters deserialize(ByteBuffer b, int offset) {
    return _deserialize(b.duplicate(), 0);
  }

  /**
   * No defensive b.duplicate()
   *
   * @param b
   * @param offset
   * @return
   */
  private static SSStorageParameters _deserialize(ByteBuffer b, int offset) {
    long version;
    int uncompressedSize;
    int compressedSize;
    short ccss;
    byte[] checksum;
    byte[] valueCreator;
    long creationTime;
    short lockSeconds;

    compressedSize = b.getInt();
    uncompressedSize = b.getInt();
    version = b.getLong();
    creationTime = b.getLong();
    lockSeconds = b.getShort();
    valueCreator = BufferUtil.arrayCopy(b, ValueCreator.BYTES);
    ccss = b.getShort();
    b.get(); // userdatalength
    checksum = BufferUtil.arrayCopy(b, CCSSUtil.getChecksumType(ccss).length());
    return new StorageParameters(version, uncompressedSize, compressedSize, lockSeconds, ccss, checksum, valueCreator,
        creationTime);
  }

  public static void main(String[] args) {
    StorageParameters p1;
    SSStorageParameters p2;
    byte[] checksum;
    byte[] valueCreator;
    byte[] s;

    checksum = new byte[ChecksumType.MD5.length()];
    valueCreator = new byte[ValueCreator.BYTES];
    p1 = new StorageParameters(1, 2, 3, (short) 0, CCSSUtil.createCCSS(Compression.NONE, ChecksumType.MD5, 0), checksum,
        valueCreator, System.currentTimeMillis());
    log.info("{}",p1);
    s = serialize(p1);
    p2 = deserialize(s);
    log.info("{}",p2);
  }
}
