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

import java.util.Set;

import com.google.common.collect.Sets;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

/** Simple LRU value retention policy. LRU is per key. */
public class LRURetentionPolicy extends CapacityBasedRetentionPolicy {
  private static final LRURetentionPolicy template;
  private static final Set<String> optionalFields;
  public static final int DO_NOT_PERSIST = -1;

  static {
    template = new LRURetentionPolicy();
    optionalFields = Sets.newHashSet("persistenceIntervalSecs");
    ObjectDefParser2.addParserWithOptionalFields(template, optionalFields);
  }

  private final int persistenceIntervalSecs;

  @OmitGeneration
  public LRURetentionPolicy() {
    this(0, 300);
  }

  public LRURetentionPolicy(long capacityBytes, int persistenceIntervalSecs) {
    super(capacityBytes);
    this.persistenceIntervalSecs = persistenceIntervalSecs;
  }

  public int getPersistenceIntervalSecs() {
    return persistenceIntervalSecs;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(capacityBytes) ^ Integer.hashCode(persistenceIntervalSecs);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (o.getClass() != getClass()) return false;

    LRURetentionPolicy other;

    other = (LRURetentionPolicy) o;
    return other.capacityBytes == capacityBytes
        && other.persistenceIntervalSecs == persistenceIntervalSecs;
  }

  public static LRURetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRURetentionPolicy.class, def);
  }
}
