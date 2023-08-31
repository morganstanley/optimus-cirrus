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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ms.silverking.cloud.skfs.dir.serverside.ByteString;
import com.ms.silverking.collection.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryInMemory extends DirectoryBase {
  private final SortedMap<ByteString, DirectoryEntryInPlace> entries;
  private int entryBytes;

  private static Logger log = LoggerFactory.getLogger(DirectoryInMemory.class);

  private static final double largeUpdateThreshold = 0.2;

  private static final boolean debug = false;

  public DirectoryInMemory(DirectoryBase d) {
    int numEntries;

    entries = new TreeMap<>();
    if (d != null) {
      numEntries = d.getNumEntries();
      if (!(d instanceof DirectoryInMemory)) {
        for (int i = 0; i < numEntries; i++) {
          DirectoryEntryInPlace entry;

          entry = (DirectoryEntryInPlace) d.getEntry(i);
          addEntry(entry.getNameAsByteString(), entry);
        }
      } else {
        DirectoryInMemory _d;

        _d = (DirectoryInMemory) d;
        for (DirectoryEntryInPlace entry : _d.entries.values()) {
          addEntry(entry.getNameAsByteString(), entry);
        }
      }
    }
  }

  public DirectoryInMemory(byte[] buf, int offset, int limit) {
    this(new DirectoryInPlace(buf, offset, limit));
  }

  public DirectoryInMemory() {
    entries = new TreeMap<>();
  }

  protected DirectoryEntryInPlace[] _createEntryIndex() {
    DirectoryEntryInPlace[] indexedEntries;
    int i;

    indexedEntries = new DirectoryEntryInPlace[getNumEntries()];
    i = 0;
    for (DirectoryEntryInPlace entry : entries.values()) {
      indexedEntries[i] = entry;
      i++;
    }
    return indexedEntries;
  }

  @Override
  public int getMagic() {
    return DEI_MAGIC;
  }

  @Override
  public int getLengthBytes() {
    return entryBytes;
  }

  @Override
  public int getIndexOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumEntries() {
    return entries.size();
  }

  @Override
  public DirectoryEntry getEntry(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<DirectoryEntry, Long> getEntryAtOffset(int offset) {
    throw new UnsupportedOperationException();
  }

  private void addEntry(ByteString name, DirectoryEntryInPlace entry) {
    // copy so that the source can be garbage collected
    entries.put(name.duplicateBuffer(), entry.duplicate());
    entryBytes += entry.getLengthBytes();
    // System.out.printf("name %s name.length() %d\n", name.toString(), name.toString().length());
    // System.out.printf("entryBytes %d entry.getLengthBytes() %d\n", entryBytes,
    // entry.getLengthBytes());
  }

  /**
   * Update local state of this directory with the incoming update
   *
   * @param update
   * @return true if the update resulted in a state change; false if it is redundant
   */
  public boolean update(DirectoryBase update) {
    if (debug) {
      log.debug("in update()");
    }
    // update.display();
    if (debug) {
      log.debug(
          "{} {} {}",
          update.getNumEntries(),
          entries.size(),
          ((double) update.getNumEntries() / (double) entries.size() > largeUpdateThreshold));
    }
    if ((double) update.getNumEntries() / ((double) entries.size() + 1) > largeUpdateThreshold) {
      return largeUpdate(update);
    } else {
      return smallUpdate(update);
    }
  }

  /**
   * Update local state of this directory with the incoming update. Used when the incoming update is
   * small with respect to this directory
   *
   * @param update
   * @return true if the update resulted in a state change; false if it is redundant
   */
  private boolean smallUpdate(DirectoryBase update) {
    boolean mutated;

    if (debug) {
      log.debug("in smallUpdate()");
    }
    mutated = false;
    if (update instanceof DirectoryInMemory) {
      for (DirectoryEntryInPlace entry : ((DirectoryInMemory) update).entries.values()) {
        mutated = update(entry) || mutated;
        if (debug) {
          log.debug("smallUpdate {}", mutated);
        }
      }
    } else {
      for (int i = 0; i < update.getNumEntries(); i++) {
        mutated = update((DirectoryEntryInPlace) update.getEntry(i)) || mutated;
        if (debug) {
          log.debug("smallUpdate {}", mutated);
        }
      }
    }
    return mutated;
  }

  /**
   * Update this directory entry with the incoming update.
   *
   * @param update
   * @return true if the update resulted in a state change; false if it is redundant
   */
  private boolean update(DirectoryEntryInPlace update) {
    DirectoryEntryInPlace entry;
    ByteString name;

    name = update.getNameAsByteString();
    entry = entries.get(name);
    if (entry == null) {
      addEntry(name, update);
      if (debug) {
        log.debug("update1 added");
      }
      return true;
    } else {
      if (debug) {
        log.debug("update2");
      }
      return entry.update(update);
    }
  }

  /**
   * Update local state of this directory with the incoming update. Used when the incoming update is
   * large with respect to this directory
   *
   * @param update
   * @return true if the update resulted in a state change; false if it is redundant
   */
  private boolean largeUpdate(DirectoryBase update) {
    int i1; // index into update
    DirectoryEntryInPlace e0; // entry in this object
    DirectoryEntry e1; // entry in update
    Iterator<DirectoryEntryInPlace> localEntries;
    int numUpdateEntries;
    List<DirectoryEntryInPlace> entriesToAdd;
    boolean mutated;
    boolean directoryInMemory;
    DirectoryEntryInPlace[] updateEntries;

    if (debug) {
      log.debug("in largeUpdate()\n");
    }
    directoryInMemory = update instanceof DirectoryInMemory;
    if (debug) {
      log.debug("update is directoryInMemory {}", directoryInMemory);
    }
    mutated = false;
    entriesToAdd = null;
    localEntries = entries.values().iterator();
    e0 = localEntries.hasNext() ? localEntries.next() : null;
    i1 = 0;

    if (directoryInMemory) {
      updateEntries = ((DirectoryInMemory) update)._createEntryIndex();
      numUpdateEntries = updateEntries.length;
    } else {
      updateEntries = null;
      numUpdateEntries = update.getNumEntries();
    }
    while (i1 < numUpdateEntries) {
      int comp;

      if (directoryInMemory) {
        e1 = updateEntries[i1];
      } else {
        e1 = update.getEntry(i1);
      }
      comp = compareForUpdate(e0, e1);
      if (debug) {
        log.debug("## {} {}  {}  {}  {}", e0, i1, e1, comp, mutated);
      }
      if (comp < 0) {
        e0 = localEntries.hasNext() ? localEntries.next() : null;
      } else if (comp > 0) {
        if (entriesToAdd == null) {
          entriesToAdd = new ArrayList<>();
        }
        entriesToAdd.add((DirectoryEntryInPlace) e1);
        // mutated set when adding the list entries below
        i1++;
      } else {
        mutated = e0.update((DirectoryEntryInPlace) e1) || mutated;
        if (debug) {
          log.debug("largeUpdate mutated {}", mutated);
        }
        e0 = localEntries.hasNext() ? localEntries.next() : null;
        i1++;
      }
    }
    if (entriesToAdd != null) {
      if (debug) {
        log.debug("largeUpdate added entries");
      }
      mutated = true;
      for (DirectoryEntryInPlace e : entriesToAdd) {
        addEntry(e.getNameAsByteString(), e);
      }
    }
    return mutated;
  }

  private int compareForUpdate(DirectoryEntryInPlace e0, DirectoryEntry e1) {
    ByteString n0;
    ByteString n1;

    n0 = getEntryNameForUpdate(e0);
    n1 = getEntryNameForUpdate(e1);
    if (n1 == null) {
      // We're spinning through update entries, so we should never find a null entry
      throw new RuntimeException("Unexpected null update entry name");
    } else {
      if (n0 == null) {
        return 1; // to force addition/iteration through updates
      } else {
        return n0.compareTo(n1);
      }
    }
  }

  private ByteString getEntryNameForUpdate(DirectoryEntry e) {
    if (e == null) {
      return null;
    } else {
      return e.getNameAsByteString();
    }
  }

  protected int computeSerializedSize() {
    return headerSize + entryBytes + computeIndexSizeBytes(entries.size());
  }

  public byte[] serialize() {
    byte[] buf;
    int offset;
    int[] indexOffsets;
    int totalBytesWritten;
    int entryIndex;
    int indexOffset;

    buf = new byte[computeSerializedSize()];
    indexOffsets = new int[entries.size()];

    // Write entries, record offsets
    entryIndex = 0;
    offset = dataOffset;
    for (Map.Entry<ByteString, DirectoryEntryInPlace> entry : entries.entrySet()) {
      // System.out.println(entry.getKey());
      // System.out.println("\t"+ entry.getValue());
      indexOffsets[entryIndex++] = offset - dataOffset;
      offset += entry.getValue().serialize(buf, offset);
    }
    // Record index offset
    indexOffset = offset - dataOffset;
    // System.out.printf("indexOffset/totalBytesWritten decimal: %d hex: %x\n", indexOffset,
    // indexOffset);
    totalBytesWritten = offset - dataOffset;
    if (totalBytesWritten != entryBytes) {
      throw new RuntimeException(
          String.format(
              "SerializationException: totalBytesWritten != entryBytes %d != %d",
              totalBytesWritten, entryBytes));
    }

    // Write index using recorded offsets
    totalBytesWritten += writeIndex(buf, indexOffset, indexOffsets);
    // System.out.printf("totalBytesWritten(1) %d %x\n", totalBytesWritten + headerSize,
    // totalBytesWritten + headerSize);

    // Write header
    totalBytesWritten +=
        writeHeader(
            buf, 0, totalBytesWritten /* does not include header */, indexOffset, entries.size());
    // System.out.printf("totalBytesWritten %d %x\n", totalBytesWritten, totalBytesWritten);

    return buf;
  }
}
