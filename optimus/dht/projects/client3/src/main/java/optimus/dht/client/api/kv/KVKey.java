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
package optimus.dht.client.api.kv;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.hash.HashCalculator;

public class KVKey implements Key {

  private final byte[] key;
  private volatile byte[] hash;

  public KVKey(byte[] key) {
    this(key, null);
  }

  public KVKey(byte[] key, byte[] hash) {
    this.key = key;
    this.hash = hash;
  }

  public byte[] key() {
    return key;
  }

  public byte[] hash() {
    return hash;
  }

  public KVKey hash(byte[] hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void ensureHash(HashCalculator hashCalculator) {
    if (hash == null) {
      hash = hashCalculator.computeHash(key);
    }
  }
}
