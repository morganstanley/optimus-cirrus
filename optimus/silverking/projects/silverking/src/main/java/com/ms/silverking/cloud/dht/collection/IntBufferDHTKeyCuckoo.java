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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.cuckoo.IntCuckooConstants;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;

/** */
public class IntBufferDHTKeyCuckoo extends DHTKeyCuckooBase implements Iterable<DHTKeyIntEntry> {
  private final SubTable[] subTables;

  private static final int empty = IntCuckooConstants.empty;
  private static final int[] extraShiftPerTable = {-1, -1, 32, -1, 16, -1, -1, -1, 8};

  private static final boolean debugCycle = false;

  // entry - key/value entry
  // bucket - group of entries
  // bucketSize - entriesPerBucket

  public IntBufferDHTKeyCuckoo(WritableCuckooConfig cuckooConfig, ByteBuffer byteBuf) {
    super(cuckooConfig);
    subTables = new SubTable[numSubTables];
    for (int i = 0; i < subTables.length; i++) {
      LongBuffer buf;
      IntBuffer values;
      int valueBufSizeInts;

      int entrySizeLongs;
      int bucketSizeLongs;
      int bufferSizeLongs;
      int bufferSizeBytes;
      int valuesSizeBytes;
      int subtableSizeBytes;
      int bufStartBytes;
      int valuesStartBytes;

      valueBufSizeInts = subTableBuckets * entriesPerBucket;
      entrySizeLongs = SubTable._singleEntrySize;
      bucketSizeLongs = SubTable._singleEntrySize * entriesPerBucket;
      bufferSizeLongs = subTableBuckets * bucketSizeLongs;
      bufferSizeBytes = bufferSizeLongs * Long.BYTES;
      valuesSizeBytes = valueBufSizeInts * Integer.BYTES;

      subtableSizeBytes = bufferSizeBytes + valuesSizeBytes;

      bufStartBytes = subtableSizeBytes * i;
      valuesStartBytes = subtableSizeBytes * i + bufferSizeBytes;

      buf =
          ((ByteBuffer)
                  byteBuf
                      .duplicate()
                      .position(bufStartBytes)
                      .limit(bufStartBytes + bufferSizeBytes))
              .slice()
              .asLongBuffer();
      values =
          ((ByteBuffer)
                  byteBuf
                      .duplicate()
                      .position(valuesStartBytes)
                      .limit(valuesStartBytes + valuesSizeBytes))
              .slice()
              .asIntBuffer();
      subTables[i] =
          new SubTable(
              i,
              cuckooConfig.getNumSubTableBuckets(),
              entriesPerBucket,
              extraShiftPerTable[numSubTables] * i,
              buf,
              values);
    }
    setSubTables(subTables);
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
    throw new UnsupportedOperationException();
  }

  public void display() {
    for (int i = 0; i < subTables.length; i++) {
      System.out.println("subTable " + i);
      subTables[i].display();
    }
  }

  class SubTable extends SubTableBase {
    private final int id;
    private final LongBuffer buf;
    private final IntBuffer values;
    private final int keyShift;

    private static final int mslOffset = 0;
    private static final int lslOffset = 1;
    private static final int _singleEntrySize = 2;

    SubTable(
        int id,
        int numBuckets,
        int entriesPerBucket,
        int extraShift,
        LongBuffer buf,
        IntBuffer values) {
      super(numBuckets, entriesPerBucket, _singleEntrySize);
      // System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
      this.id = id;
      this.buf = buf;
      this.values = values;
      // buf = new long[bufferSizeLongs];
      // values = new int[numBuckets * entriesPerBucket];
      keyShift = extraShift;
      // clear();
    }

    @Override
    boolean remove(long msl, long lsl) {
      throw new UnsupportedOperationException();
    }

    int get(long msl, long lsl) {
      int bucketIndex;

      bucketIndex = getBucketIndex(lsl);
      for (int i = 0; i < entriesPerBucket; i++) {
        int entryIndex;

        // if (entryMatches(msl, lsl, index, i)) {
        entryIndex = ((int) ((int) lsl >>> balanceShift) + i) & entriesMask;
        // entryIndex = i;
        if (entryMatches(msl, lsl, bucketIndex, entryIndex)) {
          return getValue(bucketIndex, entryIndex);
        }
      }
      return empty;
    }

    private int getBucketIndex(long lsl) {
      return (int) (lsl >>> keyShift) & bitMask;
    }

    void display() {
      for (int i = 0; i < numBuckets(); i++) {
        displayBucket(i);
      }
    }

    private void displayBucket(int bucketIndex) {
      System.out.println("\tbucket " + bucketIndex);
      for (int i = 0; i < entriesPerBucket; i++) {
        displayEntry(bucketIndex, i);
      }
    }

    private void displayEntry(int bucketIndex, int entryIndex) {
      System.out.println("\t\t" + bucketIndex + "\t" + entryString(bucketIndex, entryIndex));
    }

    private String entryString(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return ""
          + buf.get(baseOffset + mslOffset)
          + "\t"
          + buf.get(baseOffset + lslOffset)
          + "\t"
          + values.get(bucketIndex);
    }

    protected final boolean isEmpty(int bucketIndex, int entryIndex) {
      return getValue(bucketIndex, entryIndex) == empty;
    }

    private boolean entryMatches(long msl, long lsl, int bucketIndex, int entryIndex) {
      int baseOffset;

      // displayEntry(bucketIndex, entryIndex);
      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      // System.out.println("\t"+ entryIndex +"\t"+ bucketIndex +"\t"+ baseOffset);
      // System.out.printf("%x:%x\t%x:%x\n",
      //        buf[baseOffset + mslOffset], buf[baseOffset + lslOffset],
      //        msl, lsl);
      return buf.get(baseOffset + mslOffset) == msl && buf.get(baseOffset + lslOffset) == lsl;
    }

    int getValue(int bucketIndex, int entryIndex) {
      return values.get(bucketIndex * entriesPerBucket + entryIndex);
    }

    long getMSL(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf.get(baseOffset + mslOffset);
    }

    long getLSL(int bucketIndex, int entryIndex) {
      int baseOffset;

      baseOffset = getHTEntryIndex(bucketIndex, entryIndex);
      return buf.get(baseOffset + lslOffset);
    }

    @Override
    void clear() {
      throw new UnsupportedOperationException();
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
      mapEntry =
          new DHTKeyIntEntry(
              subTables[subTable].getMSL(bucket, entry),
              subTables[subTable].getLSL(bucket, entry),
              subTables[subTable].getValue(bucket, entry));
      moveToNonEmpty();
      return mapEntry;
    }

    boolean curIsEmpty() {
      return subTables[subTable].getValue(bucket, entry) == empty;
    }
  }
}
