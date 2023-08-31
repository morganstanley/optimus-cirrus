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
 * A map entry from a DHTKey-based hash table. Pairs the DHTKey with the value that the key maps to
 * in the hash table.
 */
abstract class DHTKeyEntryBase implements DHTKey {
  private final long msl;
  private final long lsl;

  DHTKeyEntryBase(long msl, long lsl) {
    this.msl = msl;
    this.lsl = lsl;
  }

  @Override
  public long getMSL() {
    return msl;
  }

  @Override
  public long getLSL() {
    return lsl;
  }

  @Override
  public int hashCode() {
    return (int) lsl;
  }

  @Override
  public boolean equals(Object o) {
    DHTKey oKey;

    oKey = (DHTKey) o;
    return lsl == oKey.getLSL() && msl == oKey.getMSL();
  }

  /**
   * Return the key from this entry.
   *
   * @return the key of this entry
   */
  public DHTKey getKey() {
    return this;
  }
}
