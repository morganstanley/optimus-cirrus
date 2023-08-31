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
package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class NanosVersionRetentionPolicy implements ValueRetentionPolicy {
  private final long invalidatedRetentionIntervalSeconds;
  private final long maxRetentionIntervalSeconds;

  public static final long NO_MAX_RETENTION_INTERVAL = 0;

  static final NanosVersionRetentionPolicy template = new NanosVersionRetentionPolicy(0, 0);

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public NanosVersionRetentionPolicy(
      long invalidatedRetentionIntervalSeconds, long maxRetentionIntervalSeconds) {
    this.invalidatedRetentionIntervalSeconds = invalidatedRetentionIntervalSeconds;
    this.maxRetentionIntervalSeconds = maxRetentionIntervalSeconds;
  }

  @OmitGeneration
  public NanosVersionRetentionPolicy(long invalidatedRetentionIntervalSeconds) {
    this(invalidatedRetentionIntervalSeconds, NO_MAX_RETENTION_INTERVAL);
  }

  public long getInvalidatedRetentionIntervalSeconds() {
    return invalidatedRetentionIntervalSeconds;
  }

  public long getMaxRetentionIntervalSeconds() {
    return maxRetentionIntervalSeconds;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(invalidatedRetentionIntervalSeconds)
        ^ Long.hashCode(maxRetentionIntervalSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    NanosVersionRetentionPolicy other;

    other = (NanosVersionRetentionPolicy) o;
    return invalidatedRetentionIntervalSeconds == other.invalidatedRetentionIntervalSeconds
        && maxRetentionIntervalSeconds == other.maxRetentionIntervalSeconds;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static NanosVersionRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(NanosVersionRetentionPolicy.class, def);
  }
}
