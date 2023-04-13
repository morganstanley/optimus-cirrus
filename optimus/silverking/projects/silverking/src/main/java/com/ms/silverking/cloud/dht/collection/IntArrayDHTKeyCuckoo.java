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

import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.collection.cuckoo.IntCuckooConstants;
import com.ms.silverking.collection.cuckoo.TableFullException;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IntArrayDHTKeyCuckoo extends DHTKeyCuckooBase implements Iterable<DHTKeyIntEntry> {
  private final SubTable[] subTables;

  private static Logger log = LoggerFactory.getLogger(IntArrayDHTKeyCuckoo.class);

  private static final int empty = IntCuckooConstants.empty;
  private static final int[] extraShiftPerTable = { -1, -1, 32, -1, 16, -1, -1, -1, 8 };

  private static final boolean debug = false;
  private static final boolean debugCycle = false;

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  public IntArrayDHTKeyCuckoo(WritableCuckooConfig cuckooConfig) {
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

  public int get(DHTKey key) {
    long msl;
    long lsl;

    msl = key.getMSL();
    lsl = key.getLSL();
    for (SubTable subTable : subTables) {
      int rVal;

      rVal = subTable.get(msl, lsl);
      if (rVal != empty) {
        return rVal;
      }
    }
    return IntCuckooConstants.noSuchValue;
  }

  public void put(DHTKey key, int value) {
    cuckooPut(key.getMSL(), key.getLSL(), value, 0);
  }

  private void cuckooPut(long msl, long lsl, int value, int attempt) {
    SubTable subTable;
    boolean success;

    if (attempt > cuckooLimit) {
      throw new TableFullException();
    }
    for (int i = 0; i < subTables.length; i++) {
      int subTableIndex;

      subTableIndex = (attempt + i) & subTablesMask;
      subTable = subTables[subTableIndex];
      if (subTable.put(msl, lsl, value)) {
        if (debug) {
          log.debug("success: {} : {}" , msl , lsl);
        }
        return;
      }
    }
    subTable = subTables[attempt % subTables.length];
    if (debug) {
      log.debug("vacate: {} : {}   {}  {} " , msl ,lsl ,attempt , (attempt % subTables.length));
    }
    subTable.vacate(msl, lsl, attempt, Math.abs(((int) (lsl + attempt) % subTable.entriesPerBucket)));
    //subTable.vacate(msl, lsl, attempt, 0/*entriesPerBucket - 1*/);
    success = subTable.put(msl, lsl, value);
    if (!success) {
      throw new RuntimeException("panic");
    }
  }

  public void display() {
    for (int i = 0; i < subTables.length; i++) {
      log.debug("subTable {}" , i);
      subTables[i].display();
    }
  }

  class SubTable extends SubTableBase {
    private final int id;
    private final long[] buf;
    private final int[] values;
    private final int keyShift;

    private static final int mslOffset = 0;
    private static final int lslOffset = 1;
    private static final int _singleEntrySize = 2;

    SubTable(int id, int numBuckets, int entriesPerBucket, int extraShift) {
      super(numBuckets, entriesPerBucket, _singleEntrySize);
      //System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
      this.id = id;
      buf = new long[bufferSizeLongs];
      values = new int[numBuckets * entriesPerBucket];
      //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
      keyShift = extraShift;
      //System.out.printf("%d\t%x\n", NumUtil.log2OfPerfectPower(bufferCapacity) - 1, bitMask);
      //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
      clear();
    }

    int persistedSizeBytes() {
      return buf.length * Long.BYTES + values.length * Integer.BYTES;
    }

    public void getAsBytes(byte[] b, int offset) {
      int o;

      o = offset;
      for (int i = 0; i < buf.length; i++) {
        NumConversion.longToBytes(buf[i], b, o);
        o += Long.BYTES;
      }
      for (int i = 0; i < values.length; i++) {
        NumConversion.intToBytes(values[i], b, o);
        o += Integer.BYTES;
      }
    }

    void clear() {
      for (int i = 0; i < bufferSizeLongs; i++) {
        buf[i] = 0;
      }
      for (int i = 0; i < values.length; i++) {
        values[i] = empty;
      }
    }

    @Override
    boolean remove(long msl, long lsl) {
      int bucketIndex;

      if (debug) {
        log.debug("***  *** remove {}  {}:{}", id, msl, lsl);
      }
      bucketIndex = getBucketIndex(lsl);
      // now search all entries in this bucket for the given key
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;
        int rVal;

        //entryIndex = ((int)msl + i) & entriesMask;
        entryIndex = ((int) ((int) lsl >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (entryMatches(msl, lsl, bucketIndex, entryIndex)) {
          putValue(bucketIndex, msl, lsl, empty, entryIndex);
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

    int get(long msl, long lsl) {
      int bucketIndex;

      if (debug) {
        log.debug(" get {}   {}:{} ", id, msl, lsl);
      }
      bucketIndex = getBucketIndex(lsl);
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;

        //if (entryMatches(msl, lsl, index, i)) {
        entryIndex = ((int) ((int) lsl >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (entryMatches(msl, lsl, bucketIndex, entryIndex)) {
          return getValue(bucketIndex, entryIndex);
        }
      }
      return empty;
    }

    boolean put(long msl, long lsl, int value) {
      int bucketIndex;

      bucketIndex = getBucketIndex(lsl);
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;

        entryIndex = ((int) ((int) lsl >>> balanceShift) + i) & entriesMask;
        //entryIndex = i;
        if (isEmpty(bucketIndex, entryIndex)) {
          putValue(bucketIndex, msl, lsl, value, entryIndex);
          return true;
        }
      }
      return false;
    }

    public void vacate(long msl, long lsl, int attempt, int entryIndex) {
      int bucketIndex;
      int baseOffset;

      bucketIndex = getBucketIndex(lsl);
      if (debugCycle || debug) {
        log.debug("{} {} {}", attempt ,lsl ,bucketIndex);
      }
      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      cuckooPut(buf[baseOffset + mslOffset], buf[baseOffset + lslOffset], getValue(bucketIndex, entryIndex),
          attempt + 1);
      //System.out.println("marking as empty: "+ index +" "+ bucketIndex);
      values[bucketIndex * entriesPerBucket + entryIndex] = empty;
      buf[baseOffset + mslOffset] = 0;
      buf[baseOffset + lslOffset] = 0;
    }

    private int getBucketIndex(long lsl) {
      //return Math.abs((int)(lsl >> keyShift)) % capacity;
      if (debug) {
        //    System.out.printf("%x\t%x\t%x\t%d\n", lsl, bitMask, ((int)(lsl >> keyShift) & bitMask), ((int)(lsl >>
        //    keyShift) & bitMask));
        log.debug("bucketIndex {} -> {}", lsl, ((int) (lsl >>> keyShift) & bitMask));
      }
      return (int) (lsl >>> keyShift) & bitMask;
    }

    void display() {
      for (int i = 0; i < numBuckets(); i++) {
        displayBucket(i);
      }
    }

    private void displayBucket(int bucketIndex) {
      log.debug(" bucket {}" , bucketIndex);
      for (int i = 0; i < entriesPerBucket; i++) {
        displayEntry(bucketIndex, i);
      }
    }

    private void displayEntry(int bucketIndex, int entryIndex) {
      log.debug("{}   {}" , bucketIndex ,entryString(bucketIndex, entryIndex));
    }

    private String entryString(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return "" + buf[baseOffset + mslOffset] + "\t" + buf[baseOffset + lslOffset] + "\t" + values[bucketIndex];
    }

    protected final boolean isEmpty(int bucketIndex, int entryIndex) {
      return getValue(bucketIndex, entryIndex) == empty;
    }

    private boolean entryMatches(long msl, long lsl, int bucketIndex, int entryIndex) {
      int baseOffset;

      //displayEntry(bucketIndex, entryIndex);
      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      //System.out.println("\t"+ entryIndex +"\t"+ bucketIndex +"\t"+ baseOffset);
      //System.out.printf("%x:%x\t%x:%x\n",
      //        buf[baseOffset + mslOffset], buf[baseOffset + lslOffset],
      //        msl, lsl);
      return buf[baseOffset + mslOffset] == msl && buf[baseOffset + lslOffset] == lsl;
    }

    int getValue(int bucketIndex, int entryIndex) {
      return values[bucketIndex * entriesPerBucket + entryIndex];
    }

    long getMSL(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf[baseOffset + mslOffset];
    }

    long getLSL(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf[baseOffset + lslOffset];
    }

    private void putValue(int index, long msl, long lsl, int value, int entryIndex) {
      int baseOffset;

      //System.out.println(index +"\t"+ bucketIndex);
      assert entryIndex < entriesPerBucket;
      baseOffset = getHTEntryIndex(index, entryIndex);
      buf[baseOffset + mslOffset] = msl;
      //System.out.println("baseOffset: "+ baseOffset +"\tmsl: "+ buf[entrySize * index + mslOffset));
      buf[baseOffset + lslOffset] = lsl;
      values[index * entriesPerBucket + entryIndex] = value;
    }
  }

  @Override
  public Iterator<DHTKeyIntEntry> iterator() {
    return new CuckooIterator();
  }

  class CuckooIterator extends CuckooIteratorBase implements Iterator<DHTKeyIntEntry> {
    CuckooIterator() {
      super();
    }

    @Override
    public DHTKeyIntEntry next() {
      DHTKeyIntEntry mapEntry;

      // precondition: moveToNonEmpty() has been called
      mapEntry = new DHTKeyIntEntry(subTables[subTable].getMSL(bucket, entry),
          subTables[subTable].getLSL(bucket, entry), subTables[subTable].getValue(bucket, entry));
      moveToNonEmpty();
      return mapEntry;
    }

    boolean curIsEmpty() {
      return subTables[subTable].getValue(bucket, entry) == empty;
    }
  }

  public static IntArrayDHTKeyCuckoo rehash(IntArrayDHTKeyCuckoo oldTable) {
    IntArrayDHTKeyCuckoo newTable;

    newTable = new IntArrayDHTKeyCuckoo(oldTable.getConfig().doubleEntries());
    try {
      for (DHTKeyIntEntry entry : oldTable) {
        //System.out.println(entry);
        newTable.put(entry.getKey(), entry.getValue());
      }
      return newTable;
    } catch (TableFullException tfe) {
      throw new RuntimeException("Unexpected table full during rehash");
    }
  }

  public static IntArrayDHTKeyCuckoo rehashAndAdd(IntArrayDHTKeyCuckoo oldTable, DHTKey key, int value) {
    IntArrayDHTKeyCuckoo newTable;

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
      log.info("");
      oldTable.display();
      log.info("");
      newTable.display();
      String keyStr = SimpleKey.of(key).toString();
      log.info("{}  {}", keyStr, value);
      throw new RuntimeException("Unexpected table full after rehash: " + keyStr);
    }
  }
}
