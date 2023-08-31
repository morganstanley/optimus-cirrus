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

public class WritableCuckooConfig extends CuckooConfig {
  private final int cuckooLimit;

  public WritableCuckooConfig(
      int totalEntries, int numSubTables, int entriesPerBucket, int cuckooLimit) {
    super(totalEntries, numSubTables, entriesPerBucket);
    this.cuckooLimit = cuckooLimit;
  }

  public WritableCuckooConfig(CuckooConfig cuckooConfig, int cuckooLimit) {
    this(
        cuckooConfig.getTotalEntries(),
        cuckooConfig.getNumSubTables(),
        cuckooConfig.getEntriesPerBucket(),
        cuckooLimit);
  }

  public static WritableCuckooConfig nonWritableConfig(
      int totalEntries, int numSubTables, int entriesPerBucket) {
    return new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, -1);
  }

  public static WritableCuckooConfig nonWritableConfig(CuckooConfig cuckooConfig) {
    return nonWritableConfig(
        cuckooConfig.getTotalEntries(),
        cuckooConfig.getNumSubTables(),
        cuckooConfig.getEntriesPerBucket());
  }

  public WritableCuckooConfig doubleEntries() {
    return new WritableCuckooConfig(
        getTotalEntries() * 2, getNumSubTables(), getEntriesPerBucket(), getCuckooLimit());
  }

  public WritableCuckooConfig newTotalEntries(int totalEntries) {
    return new WritableCuckooConfig(
        totalEntries, getNumSubTables(), getEntriesPerBucket(), getCuckooLimit());
  }

  public int getCuckooLimit() {
    return cuckooLimit;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + cuckooLimit;
  }
}
