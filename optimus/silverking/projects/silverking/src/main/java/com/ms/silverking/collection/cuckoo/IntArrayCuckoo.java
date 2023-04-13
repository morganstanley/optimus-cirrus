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

import java.util.Iterator;

import com.ms.silverking.numeric.NumConversion;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 *
 */
public class IntArrayCuckoo extends CuckooBase<Integer> implements Iterable<IntKeyIntEntry> {
  private final SubTable[] subTables;

  private static final int empty = IntCuckooConstants.empty;
  private static final int[] extraShiftPerTable = { -1, -1, 32, -1, 16, -1, -1, -1, 8 };

  private static final boolean debug = false;
  private static final boolean debugCycle = false;

  private static Logger log = LoggerFactory.getLogger(IntArrayCuckoo.class);

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  public IntArrayCuckoo(WritableCuckooConfig cuckooConfig) {
    super(cuckooConfig);
    subTables = new SubTable[numSubTables];
    for (int i = 0; i < subTables.length; i++) {
      subTables[i] = new SubTable(i, cuckooConfig.getNumSubTableBuckets(), entriesPerBucket,
          extraShiftPerTable[numSubTables] * i);
    }
    setSubTables(subTables);
  }

  public int persistedSizeBytes() {
    int total;

    // a bit pedantic since the subtables are identical, but leave for now
    total = 0;
    for (int i = 0; i < subTables.length; i++) {
      total += subTables[i].persistedSizeBytes();
    }
    return total;
  }

  public byte[] getAsBytesWithHeader(int headerSize) {
    byte[] b;
    int curOffset;

    b = new byte[persistedSizeBytes() + headerSize];
    curOffset = headerSize;
    for (int i = 0; i < subTables.length; i++) {
      subTables[i].getAsBytes(b, curOffset);
      curOffset += subTables[i].persistedSizeBytes();
    }
    return b;
  }

  public byte[] getAsBytes() {
    return getAsBytesWithHeader(0);
  }

  public int get(Integer key) {
    int _key;

    _key = Integer.hashCode(key);
    for (SubTable subTable : subTables) {
      int rVal;

      rVal = subTable.get(_key);
      if (rVal != empty) {
        return rVal;
      }
    }
    return IntCuckooConstants.noSuchValue;
  }

  public void put(Integer key, int value) {
    int _key;

    _key = Integer.hashCode(key);
    cuckooPut(_key, value, 0);
  }

  private void cuckooPut(int key, int value, int attempt) {
    SubTable subTable;
    boolean success;

    if (attempt > cuckooLimit) {
      throw new TableFullException();
    }
    for (int i = 0; i < subTables.length; i++) {
      int subTableIndex;

      subTableIndex = (attempt + i) & subTablesMask;
      subTable = subTables[subTableIndex];
      if (subTable.put(key, value)) {
        if (debug) {
          log.info("success: " + key);
        }
        return;
      }
    }
    subTable = subTables[attempt % subTables.length];
    if (debug) {
      log.info("vacate: {} {} {}" , key ,attempt , (attempt % subTables.length));
    }
    subTable.vacate(key, attempt, Math.abs(((int) (key + attempt) % subTable.entriesPerBucket)));
    //subTable.vacate(msl, lsl, attempt, 0/*entriesPerBucket - 1*/);
    success = subTable.put(key, value);
    if (!success) {
      throw new RuntimeException("panic");
    }
  }

  public void display() {
    for (int i = 0; i < subTables.length; i++) {
      log.info("subTable {}" , i);
      subTables[i].display();
    }
  }

  class SubTable extends SubTableBase {
    private final int id;
    private final int[] buf;
    private final int[] values;
    private final int keyShift;

    private static final int keyOffset = 0;
    private static final int _singleEntrySize = 1;

    SubTable(int id, int numBuckets, int entriesPerBucket, int extraShift) {
      super(numBuckets, entriesPerBucket, _singleEntrySize);
      //System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
      this.id = id;
      buf = new int[bufferSizeAtoms];
      values = new int[numBuckets * entriesPerBucket];
      //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
      keyShift = extraShift;
      //System.out.printf("%d\t%x\n", NumUtil.log2OfPerfectPower(bufferCapacity) - 1, bitMask);
      //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
      clear();
    }

    int persistedSizeBytes() {
      return buf.length * Integer.BYTES + values.length * Integer.BYTES;
    }

    public void getAsBytes(byte[] b, int offset) {
      int o;

      o = offset;
      for (int i = 0; i < buf.length; i++) {
        NumConversion.intToBytes(buf[i], b, o);
        o += Integer.BYTES;
      }
      for (int i = 0; i < values.length; i++) {
        NumConversion.intToBytes(values[i], b, o);
        o += Integer.BYTES;
      }
    }

    void clear() {
      for (int i = 0; i < bufferSizeAtoms; i++) {
        buf[i] = 0;
      }
      for (int i = 0; i < values.length; i++) {
        values[i] = empty;
      }
    }

    @Override
    boolean remove(Integer key) {
      int bucketIndex;

      if (debug) {
        log.debug("***\t*** remove {}  {}", id, key);
      }
      bucketIndex = getBucketIndex(key);
      // now search all entries in this bucket for the given key
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;
        int rVal;

        //entryIndex = ((int)msl + i) & entriesMask;
        entryIndex = ((int) ((int) key >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (entryMatches(key, bucketIndex, entryIndex)) {
          putValue(bucketIndex, key, empty, entryIndex);
          if (debug) {
            log.debug("removed from: {}" , id);
          }
          return true;
        } else {
          if (debug) {
            log.debug("not removed");
          }
        }
      }
      if (debug) {
        log.debug("not removed");
      }
      return false;
    }

    int get(int key) {
      int bucketIndex;

      if (debug) {
        log.info("  get {}  {}", id, key);
      }
      bucketIndex = getBucketIndex(key);
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;

        //if (entryMatches(msl, lsl, index, i)) {
        entryIndex = ((int) ((int) key >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (entryMatches(key, bucketIndex, entryIndex)) {
          return getValue(bucketIndex, entryIndex);
        }
      }
      return empty;
    }

    boolean put(int key, int value) {
      int bucketIndex;

      bucketIndex = getBucketIndex(key);
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;

        entryIndex = ((int) ((int) key >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (isEmpty(bucketIndex, entryIndex)) {
          putValue(bucketIndex, key, value, entryIndex);
          return true;
        }
      }
      return false;
    }

    public void vacate(int key, int attempt, int entryIndex) {
      int bucketIndex;
      int baseOffset;

      bucketIndex = getBucketIndex(key);
      if (debugCycle || debug) {
        log.info(attempt + "\t" + key + "\t" + bucketIndex);
      }
      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      cuckooPut(buf[baseOffset + keyOffset], getValue(bucketIndex, entryIndex),
          attempt + 1);
      //System.out.println("marking as empty: "+ index +" "+ bucketIndex);
      values[bucketIndex * entriesPerBucket + entryIndex] = empty;
      buf[baseOffset + keyOffset] = 0;
    }

    private int getBucketIndex(int key) {
      //return Math.abs((int)(lsl >> keyShift)) % capacity;
      if (debug) {
        //    System.out.printf("%x\t%x\t%x\t%d\n", lsl, bitMask, ((int)(lsl >> keyShift) & bitMask), ((int)(lsl >>
        //    keyShift) & bitMask));
        log.info("bucketIndex {} -> {}", key, ((int) (key >>> keyShift) & bitMask));
      }
      return (int) (key >>> keyShift) & bitMask;
    }

    void display() {
      for (int i = 0; i < numBuckets(); i++) {
        displayBucket(i);
      }
    }

    private void displayBucket(int bucketIndex) {
      log.info("bucket {}" , bucketIndex);
      for (int i = 0; i < entriesPerBucket; i++) {
        displayEntry(bucketIndex, i);
      }
    }

    private void displayEntry(int bucketIndex, int entryIndex) {
      log.info("{}  {}" , bucketIndex ,entryString(bucketIndex, entryIndex));
    }

    private String entryString(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return "" + buf[baseOffset + keyOffset] + "\t" + values[bucketIndex];
    }

    protected final boolean isEmpty(int bucketIndex, int entryIndex) {
      return getValue(bucketIndex, entryIndex) == empty;
    }

    private boolean entryMatches(int key, int bucketIndex, int entryIndex) {
      int baseOffset;

      //displayEntry(bucketIndex, entryIndex);
      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      //System.out.println("\t"+ entryIndex +"\t"+ bucketIndex +"\t"+ baseOffset);
      //System.out.printf("%x:%x\t%x:%x\n",
      //        buf[baseOffset + mslOffset], buf[baseOffset + lslOffset],
      //        msl, lsl);
      return buf[baseOffset + keyOffset] == key;
    }

    int getValue(int bucketIndex, int entryIndex) {
      return values[bucketIndex * entriesPerBucket + entryIndex];
    }

    int getKey(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf[baseOffset + keyOffset];
    }

    private void putValue(int index, int key, int value, int entryIndex) {
      int baseOffset;

      //System.out.println(index +"\t"+ bucketIndex);
      assert entryIndex < entriesPerBucket;
      baseOffset = getHTEntryIndex(index, entryIndex);
      buf[baseOffset + keyOffset] = key;
      //System.out.println("baseOffset: "+ baseOffset +"\tmsl: "+ buf[entrySize * index + mslOffset));
      values[index * entriesPerBucket + entryIndex] = value;
    }
  }

  @Override
  public Iterator<IntKeyIntEntry> iterator() {
    return new CuckooIterator();
  }

  class CuckooIterator extends CuckooIteratorBase implements Iterator<IntKeyIntEntry> {
    CuckooIterator() {
      super();
    }

    @Override
    public IntKeyIntEntry next() {
      IntKeyIntEntry mapEntry;

      // precondition: moveToNonEmpty() has been called
      mapEntry = new IntKeyIntEntry(subTables[subTable].getKey(bucket, entry),
          subTables[subTable].getValue(bucket, entry));
      moveToNonEmpty();
      return mapEntry;
    }

    boolean curIsEmpty() {
      return subTables[subTable].getValue(bucket, entry) == empty;
    }
  }

  public static IntArrayCuckoo rehash(
      IntArrayCuckoo oldTable) {
    IntArrayCuckoo newTable;

    newTable = new IntArrayCuckoo(oldTable.getConfig().doubleEntries());
    try {
      for (IntKeyIntEntry entry : oldTable) {
        //System.out.println(entry);
        newTable.put(entry.getKey(), entry.getValue());
      }
      return newTable;
    } catch (TableFullException tfe) {
      throw new RuntimeException("Unexpected table full during rehash");
    }
  }

  public static IntArrayCuckoo rehashAndAdd(
      IntArrayCuckoo oldTable, int key, int value) {
    IntArrayCuckoo newTable;

    newTable = rehash(oldTable);
    try {
      newTable.put(key, value);
      return newTable;
    } catch (TableFullException tfe) {
      // Something is very wrong if we get here.
      // For instance, are duplicate keys being put into the table?
      log.info("old table");
      oldTable.displaySizes();
      log.info("new table");
      newTable.displaySizes();
      log.info("\n\n\n");
      oldTable.display();
      log.info("\n\n");
      newTable.display();
      log.info("{}  {}", key, value);
      throw new RuntimeException("Unexpected table full after rehash: " + key);
    }
  }
}
