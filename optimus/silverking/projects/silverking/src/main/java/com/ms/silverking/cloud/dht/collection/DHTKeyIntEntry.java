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
package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * A map entry from a Key-Int hash table. Pairs the DHTKey with the integer that the key maps to in the hash table.
 */
public class DHTKeyIntEntry extends DHTKeyEntryBase {
  private final int value;

  /**
   * Construct a DHTKeyIntEntry from a key msl, lsl and the value that the key maps to.
   *
   * @param msl
   * @param lsl
   * @param value
   */
  public DHTKeyIntEntry(long msl, long lsl, int value) {
    super(msl, lsl);
    this.value = value;
  }

  /**
   * Construct a DHTKeyIntEntry from a key and the value that the key maps to.
   *
   * @param key
   * @param value
   */
  public DHTKeyIntEntry(DHTKey key, int value) {
    this(key.getMSL(), key.getLSL(), value);
  }

  /**
   * Return the value mapped to by the key.
   *
   * @return the value mapped to by the key
   */
  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%x:%x:%d", getMSL(), getLSL(), value);
  }
}