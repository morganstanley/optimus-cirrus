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
package com.ms.silverking.cloud.dht.common;

public class SimpleNSKey {
  private final long key_msl;
  private final long key_lsl;
  private final long ns;

  public SimpleNSKey(Namespace ns, DHTKey key) {
    key_msl = key.getMSL();
    key_lsl = key.getLSL();
    this.ns = ns.contextAsLong();
  }

  @Override
  public int hashCode() {
    return (int) (key_lsl ^ ns);
  }

  @Override
  public boolean equals(Object o) {
    SimpleNSKey other;

    other = (SimpleNSKey) o;
    return this.key_lsl == other.key_lsl && this.key_msl == other.key_msl && this.ns == other.ns;
  }
}
