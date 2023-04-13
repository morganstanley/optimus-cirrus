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

/**
 * Least-recently written value retention policy. LRW is per key.
 */
public class LRWRetentionPolicy extends CapacityBasedRetentionPolicy {
  static final LRWRetentionPolicy template = new LRWRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public LRWRetentionPolicy(long capacityBytes) { super(capacityBytes); }

  private LRWRetentionPolicy() { this(0); }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static LRWRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRWRetentionPolicy.class, def);
  }
}
