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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.io.util.BufferUtil;

public class DataSegmentWalkEntry {
  private final DHTKey key;
  private final long version;
  private final int offset;
  private final int storedLength;
  private final int uncompressedLength;
  private final int compressedLength;
  private final int keyLength;
  private final ByteBuffer storedFormat;
  private final long creationTime;
  private final ValueCreator creator;
  private final byte storageState;

  DataSegmentWalkEntry(DHTKey key, long version, int offset, int storedLength, int uncompressedLength,
      int compressedLength, int keyLength, ByteBuffer storedFormat, long creationTime, ValueCreator creator,
      byte storageState) {
    this.key = key;
    this.version = version;
    this.offset = offset;
    this.storedLength = storedLength;
    this.uncompressedLength = uncompressedLength;
    this.compressedLength = compressedLength;
    this.keyLength = keyLength;
    //System.out.println(storedFormat);
    this.storedFormat = storedFormat;
    this.creationTime = creationTime;
    this.creator = creator;
    this.storageState = storageState;
  }

  public DHTKey getKey() {
    return key;
  }

  public long getVersion() {
    return version;
  }

  public int getOffset() {
    return offset;
  }

  public int getStoredLength() {
    return storedLength;
  }

  public int getUncompressedLength() {
    return uncompressedLength;
  }

  public int getCompressedLength() {
    return compressedLength;
  }

  public int getKeyLength() {
    return keyLength;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public ValueCreator getCreator() {
    return creator;
  }

  public int nextEntryOffset() {
    return offset + storedLength + keyLength;
  }

  public byte getStorageState() {
    return storageState;
  }

  public ByteBuffer getStoredFormat() {
    return storedFormat;
  }

  public ByteBuffer getValue() {
    int dataOffset;
    ByteBuffer rawValue;
    ByteBuffer value;

    dataOffset = MetaDataUtil.getDataOffset(getStoredFormat(), 0);
    rawValue = (ByteBuffer) getStoredFormat().duplicate().position(dataOffset);
    if (getStorageParameters().getCompression() == Compression.NONE) {
      value = rawValue;
    } else {
      byte[] v;

      v = BufferUtil.arrayCopy(rawValue);
      try {
        value = ByteBuffer.wrap(CompressionUtil.decompress(getStorageParameters().getCompression(), v, 0, v.length,
            getStorageParameters().getUncompressedSize()));
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to decompress", ioe);
      }
    }
    return value;
  }

  public StorageParameters getStorageParameters() {
    //System.out.println(storedFormat);
    //System.out.println(MetaDataUtil.getCCSS(storedFormat, 0));
    //System.out.println(MetaDataUtil.getChecksumType(storedFormat, 0));
    return new StorageParameters(version, uncompressedLength, compressedLength, PutOptions.noLock,
        MetaDataUtil.getCCSS(storedFormat, 0), MetaDataUtil.getChecksum(storedFormat, 0), creator.getBytes(),
        creationTime);

  }

  @Override
  public String toString() {
    return KeyUtil.keyToString(
        key) + ":" + offset + ":" + storedLength + ":" + uncompressedLength + ":" + compressedLength + ":" + keyLength + ":" + version + ":" + creationTime + ":" + creator + ":" + storageState;
  }

  public byte[] getUserData() {
    return MetaDataUtil.getUserData(storedFormat, 0);
  }
}
