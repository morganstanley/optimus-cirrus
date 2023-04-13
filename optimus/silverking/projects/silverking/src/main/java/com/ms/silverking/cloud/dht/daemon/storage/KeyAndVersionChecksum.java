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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.SimpleKey;

/**
 * Key and version checksum pair for use in convergence.
 */
public class KeyAndVersionChecksum implements Comparable<KeyAndVersionChecksum> {
  private final DHTKey key;
  private final long versionChecksum; // may need to expand this for multi-version
  // note that we don't care about the segment number for equality; we simply carry this information for efficiency
  // in retrieval
  private final long segmentNumber; // we only need 32 bits of this, but use the full long for now as that is
  // expected by serialization

  public KeyAndVersionChecksum(DHTKey key, long versionChecksum, long segmentNumber) {
    this.key = SimpleKey.of(key);
    this.versionChecksum = versionChecksum;
    this.segmentNumber = segmentNumber;
  }

  public DHTKey getKey() {
    return key;
  }

  public long getVersionChecksum() {
    return versionChecksum;
  }

  public long getSegmentNumber() {
    return segmentNumber;
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ (int) versionChecksum;
    // do not include segment number
  }

  @Override
  public boolean equals(Object other) {
    KeyAndVersionChecksum oKVC;

    oKVC = (KeyAndVersionChecksum) other;
    return key.equals(oKVC.key) && (versionChecksum == oKVC.versionChecksum);
    // do not include segment number
  }

  @Override
  public String toString() {
    return key.toString() + " " + Long.toHexString(versionChecksum) + " " + segmentNumber;
  }

  @Override
  public int compareTo(KeyAndVersionChecksum o) {
    int comp;

    comp = DHTKeyComparator.dhtKeyComparator.compare(key, o.key);
    if (comp == 0) {
      return Long.compare(versionChecksum, o.versionChecksum);
    } else {
      return comp;
    }
    // do not include segment number
  }

  ///////////////////////////////////////////////////////////////////////////

  private static final int mslOffset = 0;
  private static final int lslOffset = 1;
  private static final int checksumOffset = 2;
  private static final int segmentNumberOffset = 3;
  private static final int serializedSizeLongs = 4;

  public static long[] listToArray(List<KeyAndVersionChecksum> kvcList) {
    long[] kvcArray;
    int i;

    // For now, we ignore the version checksum as we only support write-once
    // FUTURE - support full bitemporal convergence
    kvcArray = new long[kvcList.size() * serializedSizeLongs];
    i = 0;
    for (KeyAndVersionChecksum kvc : kvcList) {
      kvcArray[i + mslOffset] = kvc.getKey().getMSL();
      kvcArray[i + lslOffset] = kvc.getKey().getLSL();
      kvcArray[i + checksumOffset] = kvc.getVersionChecksum();
      kvcArray[i + segmentNumberOffset] = kvc.getSegmentNumber();
      i += serializedSizeLongs;
    }
    return kvcArray;
  }

  public static List<KeyAndVersionChecksum> arrayToList(long[] kvcArray) {
    List<KeyAndVersionChecksum> kvcList;

    // For now, we ignore the version checksum as we only support write-once
    // FUTURE - support full bitemporal convergence
    kvcList = new ArrayList<>(kvcArray.length / serializedSizeLongs);
    for (int i = 0; i < kvcArray.length; i += serializedSizeLongs) {
      kvcList.add(new KeyAndVersionChecksum(new SimpleKey(kvcArray[i + mslOffset], kvcArray[i + lslOffset]),
          kvcArray[i + checksumOffset], kvcArray[i + segmentNumberOffset]));
    }
    return kvcList;
  }

  public static int entriesInArray(long[] kvcArray) {
    assert kvcArray.length % serializedSizeLongs == 0;
    return kvcArray.length / serializedSizeLongs;
  }

  public static Iterator<KeyAndVersionChecksum> getKVCArrayIterator(long[] kvcArray) {
    return new KVCArrayIterator(kvcArray);
  }

  private static class KVCArrayIterator implements Iterator<KeyAndVersionChecksum> {
    private final long[] kvcArray;
    private int index;

    public KVCArrayIterator(long[] kvcArray) {
      this.kvcArray = kvcArray;
    }

    @Override
    public boolean hasNext() {
      return index < kvcArray.length;
    }

    @Override
    public KeyAndVersionChecksum next() {
      KeyAndVersionChecksum kvc;

      kvc = kvcAt(kvcArray, index);
      index += serializedSizeLongs;
      return kvc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static boolean isValidKVCIndex(long[] kvcArray, int index) {
    return index < kvcArray.length && (index % serializedSizeLongs) == 0;
  }

  public static KeyAndVersionChecksum kvcAt(long[] kvcArray, int index) {
    assert isValidKVCIndex(kvcArray, index);

    return new KeyAndVersionChecksum(new SimpleKey(kvcArray[index + mslOffset], kvcArray[index + lslOffset]),
        kvcArray[index + checksumOffset], kvcArray[index + segmentNumberOffset]);
  }
    
    /*
     * 
     * the commented out methods are a start of an implementation of a faster pruning algorithm
     * 
    private static int getKVCIndexForCoordinate(long[] kvcArray, long p) {
        int    size;
        
        size = entriesInArray(kvcArray);
        for (int i = 0; i < size; i++) {
            KeyAndVersionChecksum    kvc;
            long    kvcP;
            
            kvc = kvcAt(kvcArray, i);
            kvcP = KeyUtil.keyToCoordinate(kvc.getKey());
        }
    }
    
    public static long[] getKVCArrayForRegion(long[] kvcArray, RingRegion region) {
        int    i0;
        int    i1;
        int    newArraySize;
        long[]    newArray;
        
        i0 = 0;
        i1 = 0;
        newArraySize = i1 - i0 + 1;
        newArray = new long[newArraySize];
        System.arraycopy(kvcArray, i0 * serializedSizeLongs, newArray, 0, newArraySize * serializedSizeLongs);
        return newArray;
    }
    */
}
