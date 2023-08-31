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

/** Tracks a storage entry that has been modified or deleted by the compactor */
public class CompactorModifiedEntry {
  private final long version;
  private final int rawSegmentNumber;
  private final long creationTime;
  private final int newSegmentNumber;

  public static final int REMOVED = -1;

  private CompactorModifiedEntry(
      long version, int rawSegmentNumber, long creationTime, int newSegmentNumber) {
    this.version = version;
    this.rawSegmentNumber = rawSegmentNumber;
    this.creationTime = creationTime;
    this.newSegmentNumber = newSegmentNumber;
  }

  public static CompactorModifiedEntry newRemovedEntry(
      long version, int sourceSegmentNumber, long creationTime) {
    return new CompactorModifiedEntry(version, sourceSegmentNumber, creationTime, REMOVED);
  }

  public static CompactorModifiedEntry newModifiedEntry(
      long version, int sourceSegmentNumber, long creationTime, int newSegmentNumber) {
    return new CompactorModifiedEntry(version, sourceSegmentNumber, creationTime, newSegmentNumber);
  }

  public long getVersion() {
    return version;
  }

  public int getRawSegmentNumber() {
    return rawSegmentNumber;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getNewSegmentNumber() {
    return newSegmentNumber;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(version)
        ^ Integer.hashCode(rawSegmentNumber)
        ^ Long.hashCode(creationTime)
        ^ Integer.hashCode(newSegmentNumber);
  }

  @Override
  public boolean equals(Object other) {
    CompactorModifiedEntry o;

    o = (CompactorModifiedEntry) other;
    return version == o.version
        && rawSegmentNumber == o.rawSegmentNumber
        && creationTime == o.creationTime
        && newSegmentNumber == o.newSegmentNumber;
  }

  @Override
  public String toString() {
    return version + ":" + rawSegmentNumber + ":" + creationTime + ":" + newSegmentNumber;
  }
}
