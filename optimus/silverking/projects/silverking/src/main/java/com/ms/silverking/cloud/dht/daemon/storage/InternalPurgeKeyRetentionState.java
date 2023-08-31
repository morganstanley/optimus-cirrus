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

import com.ms.silverking.cloud.dht.ValueRetentionState;

/** Only used in server side (within package scope) */
class InternalPurgeKeyRetentionState implements ValueRetentionState {
  private long count;
  private long latestCreationTimeNanos;
  private long latestVersion;

  InternalPurgeKeyRetentionState() {
    reset();
  }

  void reset() {
    this.count = 0;
    this.latestCreationTimeNanos = -1;
    this.latestVersion = -1;
  }

  void keyPurged(long creationTimeNanos, long version) {
    count++;
    if (creationTimeNanos > latestCreationTimeNanos) {
      latestCreationTimeNanos = creationTimeNanos;
    }
    if (version > latestVersion) {
      latestVersion = version;
    }
  }

  long getCount() {
    return count;
  }

  /**
   * Caller shall be careful about its return value
   *
   * @return latest creationTimeNanos of all purged versions; Or negative long if nothing purged
   *     (getCount() == 0)
   */
  long getLatestCreationTimeNanos() {
    return latestCreationTimeNanos;
  }

  /**
   * Caller shall be careful about its return value
   *
   * @return latest version of all purged versions; Or negative long if nothing purged (getCount()
   *     == 0)
   */
  long getLatestVersion() {
    return latestVersion;
  }
}
