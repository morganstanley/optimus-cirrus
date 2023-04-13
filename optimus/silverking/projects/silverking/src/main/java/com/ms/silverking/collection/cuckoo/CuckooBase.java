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
package com.ms.silverking.collection.cuckoo;

import com.ms.silverking.numeric.NumConversion;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Functionality common to all Cuckoo hash table implementations.
 */
public abstract class CuckooBase<T> {
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

  private static Logger log = LoggerFactory.getLogger(CuckooBase.class);

  protected static final int offsetIndexShift = 32;

  private static final int[] base2Masks = { 0, 0x0, 0x1, 0, 0x3, 0, 0, 0, 0x7 };

  protected static final boolean debug = false;
  protected static final boolean debugCycle = false;
  protected static final boolean quickCheck = true;
  protected static final boolean debugIterator = false;

  protected static final int empty = Integer.MIN_VALUE;
  public static final int keyNotFound = empty;

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  protected CuckooBase(WritableCuckooConfig cuckooConfig) {
    if (debug) {
      log.debug("{}",cuckooConfig);
    }
    if (cuckooConfig.getNumSubTables() < 2) {
      throw new RuntimeException("numSubTables must be >= 2");
    }
    this.config = cuckooConfig;
    this.totalEntries = cuckooConfig.getTotalEntries();
    this.numSubTables = cuckooConfig.getNumSubTables();
    this.entriesPerBucket = cuckooConfig.getEntriesPerBucket();
    this.cuckooLimit = cuckooConfig.getCuckooLimit();
    subTableBuckets = cuckooConfig.getNumSubTableBuckets();
    if (debug) {
      log.info("totalEntries: {}" , totalEntries);
    }
    //subTables = new SubTableBase[numSubTables];
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

  public abstract int get(T key);

  public abstract void put(T key, int offset);

  public boolean remove(T key) {

    for (SubTableBase subTable : subTables) {
      boolean removed;

      removed = subTable.remove(key);
      if (removed) {
        return true;
      }
    }
    return false;
  }

  public void displaySizes() {
    log.info("totalEntries: {}" , totalEntries);
    for (int i = 0; i < subTables.length; i++) {
      log.info("{} {}",i , subTables[i].size());
    }
  }

  public void displayOccupancy() {
    log.info("totalEntries: {}" , totalEntries);
    for (int i = 0; i < subTables.length; i++) {
      log.info("subTable {}", i);
      subTables[i].displayOccupancy();
    }
  }

  public void display() {
    log.info("display() not implemented for this type");
  }

  /**
   * CuckooBase SubTable. Each SubTable maintains a bucketed
   * hash table.
   */
  public abstract class SubTableBase {
    protected final int singleEntrySize;
    protected final int bufferSizeAtoms;
    protected final int entriesPerBucket;
    protected final int bucketSizeAtoms;
    protected final int entrySizeAtoms;
    protected final int bitMask;

    protected static final int balanceShift = 20;

    public SubTableBase(int numBuckets, int entriesPerBucket, int singleEntrySize) {
      if (debug) {
        log.info("numEntries: {} entriesPerBucket: {} " ,numBuckets , entriesPerBucket);
      }
      if (Integer.bitCount(numBuckets) != 1) {
        throw new RuntimeException("Supplied numBuckets must be a perfect power of 2: " + numBuckets);
      }
      this.singleEntrySize = singleEntrySize;
      this.entriesPerBucket = entriesPerBucket;
      entrySizeAtoms = singleEntrySize;
      bucketSizeAtoms = singleEntrySize * entriesPerBucket;
      this.bufferSizeAtoms = numBuckets * bucketSizeAtoms;
      bitMask = numBuckets - 1;
    }

    // for debugging only, critical path usage would require a fast implementation
    protected int numBuckets() {
      return bufferSizeAtoms / bucketSizeAtoms;
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
     * Given a bucketIndex and a bucketEntryIndex compute the
     * hash table index in the hash table array
     *
     * @param bucketIndex
     * @param bucketEntryIndex
     * @return
     */
    protected int getHTEntryIndex(int bucketIndex, int bucketEntryIndex) {
      return bucketSizeAtoms * bucketIndex + entrySizeAtoms * bucketEntryIndex;
      //return (index << 2) + bucketIndex; // saves about 1 ns
    }

    abstract boolean remove(T key);

    public void displayOccupancy() {
      int size;

      size = 0;
      //System.out.println("a");
      for (int i = 0; i < numBuckets(); i++) {
        log.info("%4d ", i);
        //System.out.println("b");
        for (int j = 0; j < entriesPerBucket; j++) {
          //System.out.println("c");
          size += isEmpty(i, j) ? 0 : 1;
          log.info("%c", isEmpty(i, j) ? '.' : 'X');
        }
        System.out.println();
      }
      log.info("size {}", size);
    }
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

    /**
     * true if the current entry is empty
     */
    abstract boolean curIsEmpty();

    public boolean hasNext() {
      return !done;
    }

    /**
     * Move to a non-empty hash entry. Assert done if no such entry can be found.
     */
    void moveToNonEmpty() {
      if (debugIterator) {
        log.info("in moveToNonEmpty");
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
          log.info("subTable %d bucket %d entry %d\n", subTable, bucket, entry);
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
