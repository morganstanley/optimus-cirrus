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
package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Format of header: Header length Header type Header value numEntries <entry...> Each entry is a 4
 * byte type followed by a 4 byte offset
 */
class FSMHeaderElement extends LTVElement {
  private static final int numEntriesOffset = valueOffset;
  private static final int entriesOffset = numEntriesOffset + NumConversion.BYTES_PER_INT;
  private static final int entriesOffset_relative = entriesOffset - numEntriesOffset;

  // for each entry
  private static final int typeOffset = 0;
  private static final int elementOffsetOffset = typeOffset + NumConversion.BYTES_PER_INT;
  private static final int bytesPerEntry = elementOffsetOffset + NumConversion.BYTES_PER_INT;

  static final int typeFieldIndex = 0;
  static final int offsetFieldIndex = 1;

  private static final boolean debug = false || LTVElement.debug;

  private static Logger log = LoggerFactory.getLogger(FSMHeaderElement.class);

  FSMHeaderElement(ByteBuffer buf) {
    super(buf);
  }

  public int getNumEntries() {
    return buf.getInt(numEntriesOffset);
  }

  public Map<FSMElementType, Integer> getElementOffsets() {
    Map<FSMElementType, Integer> elementOffsets;

    elementOffsets = new HashMap<>();
    for (int i = 0; i < getNumEntries(); i++) {
      FSMElementType type;
      int entryOffset;
      int elementOffset;

      entryOffset = entriesOffset + bytesPerEntry * i;
      type = FSMElementType.typeForOrdinal(buf.getInt(entryOffset + typeOffset));
      elementOffset = buf.getInt(entryOffset + elementOffsetOffset);
      elementOffsets.put(type, elementOffset);
    }
    return ImmutableMap.copyOf(elementOffsets);
  }

  public static int getSizeBytes(int numEntries) {
    return entriesOffset + numEntries * bytesPerEntry;
  }

  public int getSizeBytes() {
    return getSizeBytes(getNumEntries());
  }

  public static FSMHeaderElement createFromHeader(FSMHeader header) {
    ByteBuffer buf;
    int entryOffset;
    List<Pair<FSMElementType, Integer>> entriesSorted;
    int sizeBytes;

    sizeBytes = getSizeBytes(header.getNumEntries());
    buf = ByteBuffer.allocate(sizeBytes).order(ByteOrder.nativeOrder());
    buf.putInt(sizeBytes);
    buf.putInt(FSMElementType.Header.ordinal());
    buf.putInt(numEntriesOffset, header.getNumEntries());
    entryOffset = entriesOffset; // We're using the whole LTV buffer,
    // so we need the entriesOffset relative to the LTV buffer
    entriesSorted = header.getEntriesByAscendingOffset();
    for (Pair<FSMElementType, Integer> entry : entriesSorted) {
      if (debug) {
        log.info("{} {}", entry, entryOffset);
      }
      buf.putInt(entryOffset + typeOffset, entry.getV1().ordinal());
      buf.putInt(entryOffset + elementOffsetOffset, entry.getV2());
      entryOffset += bytesPerEntry;
    }
    buf.rewind();
    if (debug) {
      log.debug("{}", buf);
    }
    return new FSMHeaderElement(buf);
  }

  ///////////////////////////////////////

  public FSMHeader toFSMHeader() {
    ByteBuffer buf;
    int numEntries;
    int entryOffset;
    Map<FSMElementType, Integer> elementOffsets;

    elementOffsets = new HashMap<>();
    if (debug) {
      log.debug("{}", getValueBuffer());
    }
    buf = BufferUtil.duplicate(getValueBuffer());
    if (debug) {
      log.debug("{}\n", buf);
      log.debug("{}\n", StringUtil.byteBufferToHexString(buf));
    }
    numEntries = getNumEntries();
    if (debug) {
      log.debug("numEntries: {}\n", numEntries);
      System.out.flush();
    }
    entryOffset =
        entriesOffset_relative; // relative because this is the value buffer that we're looking at
    for (int i = 0; i < numEntries; i++) {
      FSMElementType type;
      int offset;

      if (debug) {
        log.debug("i: {} {}", i, entryOffset);
        System.out.flush();
      }
      type = FSMElementType.typeForOrdinal(buf.getInt(entryOffset + typeOffset));
      offset = buf.getInt(entryOffset + elementOffsetOffset);
      if (debug) {
        log.info("type: {}  offset {}", type, offset);
        System.out.flush();
      }
      elementOffsets.put(type, offset);
      entryOffset += bytesPerEntry;
    }
    return new FSMHeader(ImmutableMap.copyOf(elementOffsets));
  }
}
