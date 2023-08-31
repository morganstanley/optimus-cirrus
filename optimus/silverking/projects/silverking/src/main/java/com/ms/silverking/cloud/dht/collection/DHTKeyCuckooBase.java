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
package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality common to all Cuckoo hash table implementations.
 *
 * <p>FUTURE: complete refactoring to leverage CuckooBase
 */
public abstract class DHTKeyCuckooBase implements Iterable<DHTKeyIntEntry> {
  // config
  private final WritableCuckooConfig config;
  protected final int totalEntries;
  protected final int numSubTables;
  protected final int entriesPerBucket;
  protected final int cuckooLimit;
  // sub tables
  private SubTableBase[] subTables;
  protected final int subTablesMask;
  protected final int entriesMask;
  protected final int subTableBuckets;

  private static Logger log = LoggerFactory.getLogger(DHTKeyCuckooBase.class);

  protected static final int offsetIndexShift = 32;

  private static final int[] base2Masks = {0, 0x0, 0x1, 0, 0x3, 0, 0, 0, 0x7};

  protected static final boolean debugCycle = false;
  protected static final boolean quickCheck = true;
  protected static final boolean debugIterator = false;

  protected static final int empty = Integer.MIN_VALUE;
  public static final int keyNotFound = empty;

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  protected DHTKeyCuckooBase(WritableCuckooConfig cuckooConfig) {
    if (cuckooConfig.getNumSubTables() < 2) {
      throw new RuntimeException("numSubTables must be >= 2");
    }
    this.config = cuckooConfig;
    this.totalEntries = cuckooConfig.getTotalEntries();
    this.numSubTables = cuckooConfig.getNumSubTables();
    this.entriesPerBucket = cuckooConfig.getEntriesPerBucket();
    this.cuckooLimit = cuckooConfig.getCuckooLimit();
    subTableBuckets = cuckooConfig.getNumSubTableBuckets();
    // subTables = new SubTableBase[numSubTables];
    subTablesMask = base2Masks[numSubTables];
    entriesMask = base2Masks[entriesPerBucket];
  }

  public WritableCuckooConfig getConfig() {
    return config;
  }

  protected void setSubTables(SubTableBase[] subTables) {
    this.subTables = subTables;
  }

  protected void initialize() {
    clear();
  }

  public int getTotalEntries() {
    return totalEntries;
  }

  int getSizeBytes() {
    return totalEntries * NumConversion.BYTES_PER_LONG;
  }

  int getNumSubTables() {
    return numSubTables;
  }

  int getEntriesPerBucket() {
    return entriesPerBucket;
  }

  void clear() {
    for (SubTableBase subTable : subTables) {
      subTable.clear();
    }
  }

  public abstract int get(DHTKey key);

  public abstract void put(DHTKey key, int offset);

  public boolean remove(DHTKey key) {
    long msl;
    long lsl;

    msl = key.getMSL();
    lsl = key.getLSL();
    for (SubTableBase subTable : subTables) {
      boolean removed;

      removed = subTable.remove(msl, lsl);
      if (removed) {
        return true;
      }
    }
    return false;
  }

  public void displaySizes() {
    log.info("totalEntries: {}", totalEntries);
    for (int i = 0; i < subTables.length; i++) {
      log.info("{} {}", i, subTables[i].size());
    }
  }

  /** CuckooBase SubTable. Each SubTable maintains a bucketed hash table. */
  abstract class SubTableBase {
    protected final int singleEntrySize;
    protected final int bufferSizeLongs;
    protected final int entriesPerBucket;
    protected final int bucketSizeLongs;
    protected final int entrySizeLongs;
    protected final int bitMask;

    protected static final int balanceShift = 20;

    SubTableBase(int numBuckets, int entriesPerBucket, int singleEntrySize) {
      if (Integer.bitCount(numBuckets) != 1) {
        throw new RuntimeException(
            "Supplied numBuckets must be a perfect power of 2: " + numBuckets);
      }
      this.singleEntrySize = singleEntrySize;
      this.entriesPerBucket = entriesPerBucket;
      entrySizeLongs = singleEntrySize;
      bucketSizeLongs = singleEntrySize * entriesPerBucket;
      this.bufferSizeLongs = numBuckets * bucketSizeLongs;
      bitMask = numBuckets - 1;
    }

    // for debugging only, critical path usage would require a fast implementation
    protected int numBuckets() {
      return bufferSizeLongs / bucketSizeLongs;
    }

    abstract void clear();

    protected abstract boolean isEmpty(int bucketIndex, int entryIndex);

    int size() {
      int size;

      size = 0;
      for (int i = 0; i < numBuckets(); i++) {
        for (int j = 0; j < entriesPerBucket; j++) {
          size += isEmpty(i, j) ? 0 : 1;
        }
      }
      return size;
    }

    /**
     * Given a bucketIndex and a bucketEntryIndex compute the hash table index in the hash table
     * array
     *
     * @param bucketIndex
     * @param bucketEntryIndex
     * @return
     */
    protected int getHTEntryIndex(int bucketIndex, int bucketEntryIndex) {
      return bucketSizeLongs * bucketIndex + entrySizeLongs * bucketEntryIndex;
      // return (index << 2) + bucketIndex; // saves about 1 ns
    }

    abstract boolean remove(long msl, long lsl);
  }

  /**
   * Base functionality used to iterate through the Cuckoo hash table skipping blank entries.
   * Concrete implementations must implement curIsEmpty() and next().
   */
  abstract class CuckooIteratorBase {
    protected int subTable;
    protected int bucket;
    protected int entry;
    protected boolean done;

    CuckooIteratorBase() {
      entry = -1;
      moveToNonEmpty();
    }

    /** true if the current entry is empty */
    abstract boolean curIsEmpty();

    public boolean hasNext() {
      return !done;
    }

    /** Move to a non-empty hash entry. Assert done if no such entry can be found. */
    void moveToNonEmpty() {
      if (debugIterator) {
        System.out.println("in moveToNonEmpty");
      }
      do {
        ++entry;
        if (entry == config.getEntriesPerBucket()) {
          entry = 0;
          ++bucket;
          if (bucket == config.getNumSubTableBuckets()) {
            bucket = 0;
            ++subTable;
            if (subTable == config.getNumSubTables()) {
              done = true;
            }
          }
        }
        if (debugIterator) {
          log.info("subTable {} bucket{} entry {}", subTable, bucket, entry);
        }
      } while (!done && curIsEmpty());
      if (debugIterator) {
        log.info("out moveToNonEmpty");
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
