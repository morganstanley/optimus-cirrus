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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;

import com.ms.silverking.cloud.dht.collection.DHTKeyCuckooBase;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.common.DHTKey;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 *
 */
public class IntBufferCuckoo extends CuckooBase<Integer> implements Iterable<IntKeyIntEntry> {
  private final SubTable[] subTables;

  private static final int empty = IntCuckooConstants.empty;
  private static final int[] extraShiftPerTable = { -1, -1, 32, -1, 16, -1, -1, -1, 8 };

  private static Logger log = LoggerFactory.getLogger(IntBufferCuckoo.class);

  private static final boolean debug = false;
  private static final boolean debugCycle = false;

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  public IntBufferCuckoo(WritableCuckooConfig cuckooConfig, ByteBuffer byteBuf) {
    super(cuckooConfig);
    subTables = new SubTable[numSubTables];
    for (int i = 0; i < subTables.length; i++) {
      IntBuffer buf;
      IntBuffer values;
      int valueBufSizeInts;

      int entrySizeAtoms;
      int bucketSizeAtoms;
      int bufferSizeAtoms;
      int bufferSizeBytes;
      int valuesSizeBytes;
      int subtableSizeBytes;
      int bufStartBytes;
      int valuesStartBytes;

      valueBufSizeInts = subTableBuckets * entriesPerBucket;
      entrySizeAtoms = SubTable._singleEntrySize;
      bucketSizeAtoms = SubTable._singleEntrySize * entriesPerBucket;
      bufferSizeAtoms = subTableBuckets * bucketSizeAtoms;
      bufferSizeBytes = bufferSizeAtoms * Integer.BYTES;
      valuesSizeBytes = valueBufSizeInts * Integer.BYTES;

      subtableSizeBytes = bufferSizeBytes + valuesSizeBytes;

      bufStartBytes = subtableSizeBytes * i;
      valuesStartBytes = subtableSizeBytes * i + bufferSizeBytes;

      buf = ((ByteBuffer) byteBuf.duplicate().position(bufStartBytes).limit(
          bufStartBytes + bufferSizeBytes)).slice().asIntBuffer();
      values = ((ByteBuffer) byteBuf.duplicate().position(valuesStartBytes).limit(
          valuesStartBytes + valuesSizeBytes)).slice().asIntBuffer();
      subTables[i] = new SubTable(i, cuckooConfig.getNumSubTableBuckets(), entriesPerBucket,
          extraShiftPerTable[numSubTables] * i, buf, values);
    }
    setSubTables(subTables);
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
    throw new UnsupportedOperationException();
  }

  public void display() {
    for (int i = 0; i < subTables.length; i++) {
      log.info("subTable {}" , i);
      subTables[i].display();
    }
  }

  class SubTable extends SubTableBase {
    private final int id;
    private final IntBuffer buf;
    private final IntBuffer values;
    private final int keyShift;

    private static final int keyOffset = 0;
    private static final int _singleEntrySize = 1;

    SubTable(int id, int numBuckets, int entriesPerBucket, int extraShift, IntBuffer buf, IntBuffer values) {
      super(numBuckets, entriesPerBucket, _singleEntrySize);
      //System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
      this.id = id;
      this.buf = buf;
      this.values = values;
      //buf = new long[bufferSizeAtoms];
      //values = new int[numBuckets * entriesPerBucket];
      keyShift = extraShift;
      //clear();
    }

    @Override
    boolean remove(Integer key) {
      throw new UnsupportedOperationException();
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
      log.info("  bucket {}" , bucketIndex);
      for (int i = 0; i < entriesPerBucket; i++) {
        displayEntry(bucketIndex, i);
      }
    }

    private void displayEntry(int bucketIndex, int entryIndex) {
      log.info("{} {}" , bucketIndex, entryString(bucketIndex, entryIndex));
    }

    private String entryString(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return "" + buf.get(baseOffset + keyOffset) + "\t" + values.get(bucketIndex);
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
      return buf.get(baseOffset + keyOffset) == key;
    }

    int getValue(int bucketIndex, int entryIndex) {
      return values.get(bucketIndex * entriesPerBucket + entryIndex);
    }

    int getKey(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf.get(baseOffset + keyOffset);
    }

    @Override
    void clear() {
      throw new UnsupportedOperationException();
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
}
