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

import java.nio.ByteBuffer;

import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Read-only OffsetList stored on disk. */
public final class BufferOffsetList extends OffsetListBase {
  // there is an unused, deprecated field that lives at offset 0. That field is one int in size. The
  // listSize field
  // goes directly after that field, and so has an offset of BYTES_PER_INT.
  static final int listSizeOffset = NumConversion.BYTES_PER_INT;

  private final ByteBuffer buf;

  private static Logger log = LoggerFactory.getLogger(BufferOffsetList.class);

  BufferOffsetList(ByteBuffer buf, boolean supportsStorageTime) {
    super(supportsStorageTime);
    this.buf = buf;
  }

  private int entryBaseOffsetBytes(int index) {
    return super.entryBaseOffset(index) * NumConversion.BYTES_PER_INT + persistedHeaderSizeBytes;
  }

  @Override
  public int size() {
    return buf.getInt(listSizeOffset);
  }

  @Override
  protected long getVersion(int index) {
    return buf.getLong(entryBaseOffsetBytes(index) + versionOffset);
  }

  @Override
  protected long getStorageTime(int index) {
    return buf.getLong(entryBaseOffsetBytes(index) + storageTimeOffset);
  }

  protected int getOffset(int index) {
    try {
      return buf.getInt(entryBaseOffsetBytes(index) + offsetOffset * NumConversion.BYTES_PER_INT);
    } catch (IndexOutOfBoundsException e) {
      log.error(
          "IndexOutOfBounds when accessing buffer offset list at index {} ({} {})",
          index,
          entryBaseOffsetBytes(index),
          entryBaseOffsetBytes(index) + offsetOffset * NumConversion.BYTES_PER_INT,
          e);
      throw e;
    }
  }

  @Override
  public void putOffset(long version, int offset, long storageTime, boolean isRecovery) {
    throw new UnsupportedOperationException();
  }
}
