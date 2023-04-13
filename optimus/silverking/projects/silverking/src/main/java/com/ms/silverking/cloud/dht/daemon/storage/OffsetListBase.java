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

import java.util.Iterator;

import javax.annotation.Nonnull;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * List of offsets used in cases where we have multiple values stored for a particular key.
 * Extended concretely by RAMOffsetList - used in non-persisted segments - and
 * BufferOffsetList - for persisted segments.
 */
abstract class OffsetListBase implements OffsetList, Iterable<Integer> {
  final boolean supportsStorageTime;
  final int entrySizeInts;

  static final int persistedHeaderSizeInts = 2;
  static final int persistedHeaderSizeBytes = persistedHeaderSizeInts * NumConversion.BYTES_PER_INT;

  static final int entrySizeInts_noStorageTime = 3;
  static final int entrySizeInts_supportsStorageTime = 5;
  protected static final int versionOffset = 0;
  static final int offsetOffset = 2;
  static final int storageTimeOffset = 3;

  protected static Logger log = LoggerFactory.getLogger(OffsetListBase.class);

  /*
   * Note: Care must be taken when working with sizes and indices. There are three units that are
   * used in the appropriate places: entries, ints, and bytes.
   */


  /*
   * Persisted format:
   *
   * List:
   *      header
   *      entries...
   *
   * Header:
   *      <unused>                4 bytes // DEPRECATED FIELD, NOT USED. Was previously the index of the list.
   *      list size in entries    4 bytes
   *
   * Entry (no storage time support):
   *      version 8 bytes
   *      offset  4 bytes
   *
   * Entry (supportsStorageTime):
   *      version     8 bytes
   *      offset      4 bytes
   *      storageTime 8 bytes
   *
   */

  OffsetListBase(boolean supportsStorageTime) {
    this.supportsStorageTime = supportsStorageTime;
    if (supportsStorageTime) {
      entrySizeInts = entrySizeInts_supportsStorageTime;
    } else {
      entrySizeInts = entrySizeInts_noStorageTime;
    }
  }

  int entryBaseOffset(int index) {
    if (index < 0) {
      log.error("Invalid negative index value {}", index);
      throw new IllegalArgumentException("Expected non-negative index value but got " + index);
    }
    return index * entrySizeInts;
  }

  protected abstract long getVersion(int index);

  protected abstract int getOffset(int index);

  protected abstract long getStorageTime(int index);

  public abstract void putOffset(long version, int offset, long storageTime, boolean isRecovery);

  /*
   * for newest:
   * binary search to find the max
   *
   * for oldest:
   * binary search to find the oldest
   *
   * special case for the most recent
   *
   */
  @Override
  public int getOffset(VersionConstraint vc, ValidityVerifier validityVerifier) {
    // FUTURE - improve efficiency
    if (vc.equals(VersionConstraint.greatest) && !supportsStorageTime && validityVerifier == null) {
      return getLastOffset();
    } else {
      return getOffset_linear(vc, validityVerifier);
    }
  }

  private int getOffset_linear(VersionConstraint vc, ValidityVerifier validityVerifier) {
    long bestMatchVersion;
    int bestMatchIndex;

    bestMatchIndex = Integer.MIN_VALUE;
    if (vc.getMode() == VersionConstraint.Mode.GREATEST) {
      bestMatchVersion = Long.MIN_VALUE;
    } else {
      bestMatchVersion = Long.MAX_VALUE;
    }
    // FUTURE - replace linear search
    for (int i = 0; i < size(); i++) {
      long curVersion;

      // StorageTimes are increasing. Exit this loop if we have exceeded the maxStorageTime.
      if (supportsStorageTime &&
          vc.getMaxCreationTime() != VersionConstraint.noCreationTimeLimit &&
          vc.getMaxCreationTime() < getStorageTime(i)) {
        break;
      }

      curVersion = getVersion(i);
      assert curVersion >= 0;
      if (vc.matches(curVersion)) {
        if (vc.getMode() == VersionConstraint.Mode.LEAST) {
          if (curVersion <= bestMatchVersion) {
            if (validityVerifier == null || validityVerifier.isValid(getOffset(i) + DHTKey.BYTES_PER_KEY)) {
              bestMatchIndex = i;
              bestMatchVersion = curVersion;
            }
          }
        } else {
          if (curVersion >= bestMatchVersion) {
            if (validityVerifier == null || validityVerifier.isValid(getOffset(i) + DHTKey.BYTES_PER_KEY)) {
              bestMatchIndex = i;
              bestMatchVersion = curVersion;
            }
          }
        }
      }
    }
    if (bestMatchIndex < 0) {
      return NO_MATCH_FOUND;
    } else {
      return getOffset(bestMatchIndex);
    }
  }

  public int getLastOffset() {
    int lastIndex;

    lastIndex = lastIndex();
    if (lastIndex >= 0) {
      return getOffset(lastIndex);
    } else {
      return -1;
    }
  }

  @Override
  public long getLatestVersion() {
    int lastIndex;

    lastIndex = lastIndex();
    if (lastIndex >= 0) {
      return getVersion(lastIndex);
    } else {
      return -1;
    }
  }

  void checkIndex(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(index + " < 0");
    }
    if (index >= size()) {
      throw new IndexOutOfBoundsException(index + " index >= " + size());
    }
  }

  private int lastIndex() {
    return size() - 1;
  }

  private void displayEntry(int i) {
    System.out.printf("%d\t%d\n", getVersion(i), getOffset(i));
  }

  @Override
  public void displayForDebug() {
    System.out.println("*** list start *** " + size());
    for (int i = 0; i < size(); i++) {
      displayEntry(i);
    }
    System.out.println("*** list end ***\n");
  }

  @Override @Nonnull
  public Iterator<Integer> iterator() {
    return new OffsetListIterator();
  }

  protected abstract class OffsetListIteratorBase<T> implements Iterator<T> {
    protected int index;

    OffsetListIteratorBase() {
    }

    @Override
    public boolean hasNext() {
      return index <= lastIndex();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private class OffsetListIterator extends OffsetListIteratorBase<Integer> {
    OffsetListIterator() {
    }

    @Override
    public Integer next() {
      if (hasNext()) {
        return getOffset(index++);
      } else {
        return null;
      }
    }
  }

  @Override
  public Iterable<Triple<Integer, Long, Long>> offsetVersionAndStorageTimeIterable() {
    return new OffsetVersionAndStorageTimeIterator();
  }

  private class OffsetVersionAndStorageTimeIterator extends OffsetListIteratorBase<Triple<Integer, Long, Long>>
      implements Iterable<Triple<Integer, Long, Long>> {
    OffsetVersionAndStorageTimeIterator() {
    }

    @Override
    public Triple<Integer, Long, Long> next() {
      if (hasNext()) {
        Triple<Integer, Long, Long> t;

        t = new Triple<>(
            getOffset(index),
            getVersion(index),
            supportsStorageTime
            ? getStorageTime(index)
            : 0);
        index++;
        return t;
      } else {
        return null;
      }
    }

    @Override @Nonnull
    public Iterator<Triple<Integer, Long, Long>> iterator() {
      return this;
    }
  }
}
