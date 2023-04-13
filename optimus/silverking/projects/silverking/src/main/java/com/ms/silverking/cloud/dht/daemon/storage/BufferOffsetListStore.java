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

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferOffsetListStore implements OffsetListStore {
  private final ByteBuffer buf;
  private final boolean supportsStorageTime;
  private final int numLists; // optimization

  private static Logger log = LoggerFactory.getLogger(BufferOffsetListStore.class);

  /*
   * As discussed in RAMOffsetListStore, indexing to
   * the lists is 1-based externally and zero-based internally.
   */

  private BufferOffsetListStore(ByteBuffer buf, NamespaceOptions nsOptions) {
    this.buf = buf;
    supportsStorageTime = nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS;
    numLists = getNumLists(buf);
  }

  public static BufferOffsetListStore createFromBuf(ByteBuffer buf, NamespaceOptions nsOptions) {
    ByteBuffer _buf;

    _buf = BufferUtil.duplicate(buf);
    return new BufferOffsetListStore(_buf, nsOptions);
  }

  private static int getNumLists(ByteBuffer buf) {
    return buf.getInt(0);
  }

  private int getNumLists() {
    return numLists;
  }

  @Override
  public OffsetList getOffsetList(int index) {
    try {
      ByteBuffer listBuf;
      int listOffset;
      int listNumEntries;
      int listSizeBytes;

      if (index <= 0) {
        throw new RuntimeException("index <= 0: " + index);
      }
      if (index > getNumLists()) {
        throw new RuntimeException("index > getNumLists(): " + index);
      }
      // in the line below, the -1 to translate to an internal index
      // and the +1 of the length offset cancel out
      listOffset = buf.getInt(index * NumConversion.BYTES_PER_INT);
      if (listOffset < 0 || listOffset >= buf.limit()) {
        log.info("listOffset: {}", listOffset);
        log.info("buf.limit(): {}", buf.limit());
        throw new InvalidOffsetListIndexException(index);
      }
      listBuf = BufferUtil.sliceAt(buf, listOffset);
      listNumEntries = listBuf.getInt(BufferOffsetList.listSizeOffset);
      listSizeBytes = listNumEntries * entrySizeBytes(supportsStorageTime) + OffsetListBase.persistedHeaderSizeBytes;
      listBuf.limit(listSizeBytes);
      return new BufferOffsetList(listBuf, supportsStorageTime);
    } catch (RuntimeException re) { // FUTURE - consider removing debug
      log.info("{}", buf);
      re.printStackTrace();
      throw re;
    }
  }

  public void displayForDebug() {
    int numLists;

    numLists = getNumLists();
    System.out.printf("numLists %d\n", numLists);
    for (int i = 0; i < numLists; i++) {
      OffsetList l;

      System.out.printf("List %d\n", i);
      l = getOffsetList(i + 1);
      l.displayForDebug();
    }
  }

  private static int entrySizeBytes(boolean supportsStorageTime) {
    return (supportsStorageTime
            ? OffsetListBase.entrySizeInts_supportsStorageTime
            : OffsetListBase.entrySizeInts_noStorageTime) * NumConversion.BYTES_PER_INT;
  }
}
