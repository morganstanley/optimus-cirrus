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

public class CompactionCheckResult {
  private final int validEntries;
  private final int invalidEntries;
  private final int retainedBytes;
  private final boolean consideredStoredLengths;

  public CompactionCheckResult(int validEntries, int invalidEntries, int retainedBytes,
      boolean consideredStoredLengths) {
    this.validEntries = validEntries;
    this.invalidEntries = invalidEntries;
    this.retainedBytes = retainedBytes;
    this.consideredStoredLengths = consideredStoredLengths;
  }

  public int getValidEntries() {
    return validEntries;
  }

  public int getInvalidEntries() {
    return invalidEntries;
  }

  public boolean hasInvalidEntries() {
    return invalidEntries > 0;
  }

  public int getTotalEntries() {
    return validEntries + invalidEntries;
  }

  public double getInvalidFraction() {
    return (double) invalidEntries / (double) getTotalEntries();
  }

  public int getRetainedBytes() {
    return retainedBytes;
  }

  @Override
  public String toString() {
    String bytesStr = consideredStoredLengths ? String.format("- bytes retained %d", retainedBytes) : "";
    return String.format("Entries Retained: %d - deleted %d %s", validEntries, invalidEntries, bytesStr);
  }
}
