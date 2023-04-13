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
package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryInPlace extends DirectoryBase {
  private final byte[] buf;
  private final int offset;
  private final int limit;

  private static Logger log = LoggerFactory.getLogger(DirectoryInPlace.class);

  public DirectoryInPlace(byte[] buf, int offset, int limit) {
    //System.out.printf("DirectoryInPlace %d %d %d\n", buf.length, offset, limit);
    this.buf = buf;
    this.offset = offset;
    this.limit = limit;
  }

  public DirectoryInPlace(byte[] buf) {
    this(buf, 0, buf.length);
  }

  public Triple<byte[], Integer, Integer> getBufferOffsetLimit() {
    return new Triple<>(buf, offset, limit);
  }

  @Override
  public int getMagic() {
    return NumConversion.bytesToIntLittleEndian(buf, offset + magicOffset);
  }

  @Override
  public int getLengthBytes() {
    return NumConversion.bytesToIntLittleEndian(buf, offset + lengthOffset);
  }

  @Override
  public int getIndexOffset() {
    int indexOffset;

    indexOffset = NumConversion.bytesToIntLittleEndian(buf, offset + indexOffsetOffset);
    return dataOffset + indexOffset;
  }

  @Override
  public int getNumEntries() {
    return NumConversion.bytesToIntLittleEndian(buf, offset + numEntriesOffset);
  }

  private int getEntryOffset(int index) {
    //System.out.printf("getEntryOffset(%d)\n", index);
    //System.out.printf("%d %d %d %d %d\n", offset + getIndexOffset() + indexFirstEntryOffset + index *
    // DEI_ENTRY_SIZE, offset, getIndexOffset(), indexFirstEntryOffset, index * DEI_ENTRY_SIZE);
    return NumConversion.bytesToIntLittleEndian(buf,
        offset + getIndexOffset() + indexFirstEntryOffset + index * DEI_ENTRY_SIZE);
  }

  @Override
  public DirectoryEntry getEntry(int index) {
    int offset;

    offset = getEntryOffset(index);
    if (offset >= 0) {
      //System.out.printf(">> getEntry %d %d %s\n", index, offset, getEntryAtOffset(offset).getV1());
      return getEntryAtOffset(offset).getV1();
    } else {
      log.info("Bad entry offset in DirectoryInPlace.getEntry()");
      return null;
    }
  }

  @Override
  public Pair<DirectoryEntry, Long> getEntryAtOffset(int offset) {
    if (offset + DirectoryEntryBase.headerSize >= getIndexOffset()) {
      return null;
    } else {
      DirectoryEntryInPlace entry;
      short eSize;
      int nextEntry;

      entry = new DirectoryEntryInPlace(buf, this.offset + dataOffset + offset);
      eSize = entry.getNameLength();
      nextEntry = offset + DirectoryEntryBase.headerSize + eSize;
      //System.out.printf("eSize %x %d\n", eSize, eSize);
      //System.out.printf("nextEntry %d\n", nextEntry);
      return new Pair<>(entry, (long) nextEntry);
    }
  }
}
