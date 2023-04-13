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
package com.ms.silverking.cloud.dht.daemon.storage.management;

public class PurgeResult {
  public final long versionsCount;
  public final long latestCreationTimeNanos;
  public final long latestVersion;
  public final int segmentsReaped;
  public final int segmentsDeleted;

  public PurgeResult(long versionsCount,
                     long latestCreationTimeNanos,
                     long latestVersion,
                     int segmentsReaped,
                     int segmentsDeleted) {
    this.versionsCount = versionsCount;
    this.latestCreationTimeNanos = latestCreationTimeNanos;
    this.latestVersion = latestVersion;
    this.segmentsReaped = segmentsReaped;
    this.segmentsDeleted = segmentsDeleted;
  }

  public static final int noVersion = -1;

  public static PurgeResult empty() {
    return new PurgeResult(0, noVersion, noVersion, 0, 0);
  }
}
