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

import com.ms.silverking.code.Constraint;
import com.ms.silverking.numeric.NumConversion;

public class CuckooConfig {
  // specified
  private final int totalEntries;
  private final int numSubTables;
  private final int entriesPerBucket;
  // derived
  private final int numSubTableBuckets;

  private static final int totalEntriesOffset = 0;
  private static final int numSubTablesOffset = totalEntriesOffset + NumConversion.BYTES_PER_INT;
  private static final int entriesPerBucketOffset = numSubTablesOffset + NumConversion.BYTES_PER_INT;
  public static final int BYTES = entriesPerBucketOffset + NumConversion.BYTES_PER_INT;

  public CuckooConfig(int totalEntries, int numSubTables, int entriesPerBucket) {
    Constraint.ensureNonZero(totalEntries);
    Constraint.ensureNonZero(numSubTables);
    Constraint.ensureNonZero(entriesPerBucket);
    this.totalEntries = totalEntries;
    this.numSubTables = numSubTables;
    this.entriesPerBucket = entriesPerBucket;
    if (totalEntries < numSubTables * entriesPerBucket) {
      throw new RuntimeException("Invalid configuration: totalEntries < numSubTables * entriesPerBucket "
        + totalEntries +" "+  numSubTables +" "+ entriesPerBucket);
    }
    if (totalEntries % (numSubTables * entriesPerBucket) != 0) {
      throw new RuntimeException("Invalid configuration: totalEntries % (numSubTables * entriesPerBucket) != 0 "
        + totalEntries +" "+  numSubTables +" "+ entriesPerBucket);
    }
    numSubTableBuckets = totalEntries / (numSubTables * entriesPerBucket);
    quickCheck();
  }

  private void quickCheck() {
    if (numSubTables < 2) {
      throw new RuntimeException("numSubTables must be >= 2");
    }
    if (Integer.bitCount(entriesPerBucket) != 1) {
      throw new RuntimeException("Supplied entriesPerBucket must be a perfect power of 2");
    }
    if (Integer.bitCount(numSubTables) != 1) {
      throw new RuntimeException("Supplied numSubTables must be a perfect power of 2");
    }
  }

  public int getTotalEntries() {
    return totalEntries;
  }

  public int getNumSubTables() {
    return numSubTables;
  }

  public int getEntriesPerBucket() {
    return entriesPerBucket;
  }

  public int getNumSubTableBuckets() {
    return numSubTableBuckets;
  }

  public void persist(ByteBuffer buf) {
    persist(buf, 0);
  }

  public void persist(ByteBuffer buf, int offset) {
    buf.putInt(offset + totalEntriesOffset, totalEntries);
    buf.putInt(offset + numSubTablesOffset, numSubTables);
    buf.putInt(offset + entriesPerBucketOffset, entriesPerBucket);
  }

  public static CuckooConfig read(ByteBuffer buf) {
    return read(buf, 0);
  }

  public static CuckooConfig read(ByteBuffer buf, int offset) {
    int totalEntries;
    int numSubTables;
    int entriesPerBucket;

    totalEntries = buf.getInt(offset + totalEntriesOffset);
    numSubTables = buf.getInt(offset + numSubTablesOffset);
    entriesPerBucket = buf.getInt(offset + entriesPerBucketOffset);
    return new CuckooConfig(totalEntries, numSubTables, entriesPerBucket);
  }

  @Override
  public String toString() {
    return totalEntries + ":" + numSubTables + ":" + entriesPerBucket + ":" + numSubTableBuckets;
  }
}
