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

import java.time.Instant;

public class KVValue {

  private final byte[] bytes;
  private final String entryInfo;
  private final Instant expiry;

  public KVValue(byte[] bytes) {
    this(bytes, null, null);
  }

  public KVValue(byte[] bytes, String entryInfo, Instant expiry) {
    this.bytes = bytes;
    this.entryInfo = entryInfo;
    this.expiry = expiry;
  }

  public byte[] bytes() {
    return bytes;
  }

  public String entryInfo() {
    return entryInfo;
  }

  public Instant expiry() {
    return expiry;
  }
}
