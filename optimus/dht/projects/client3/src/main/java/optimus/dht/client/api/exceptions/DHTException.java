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
package optimus.dht.client.api.exceptions;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.common.util.StringTool;

public class DHTException extends RuntimeException {

  public DHTException() {
    super();
  }

  public DHTException(String message, Throwable cause) {
    super(message, cause);
  }

  public DHTException(String message) {
    super(message);
  }

  public DHTException(Throwable cause) {
    super(cause);
  }

  protected static String formatKeys(List<? extends Key> keys) {
    return keys.stream()
        .map(key -> StringTool.formatByteHash(key.hash()))
        .collect(Collectors.joining(",", "(", ")"));
  }

  protected static List<SerializableKey> copyKeys(List<? extends Key> keys) {
    return keys.stream()
        .map(k -> new SerializableKey(k.key(), k.hash()))
        .collect(Collectors.toList());
  }

  protected static class SerializableKey implements Key, Serializable {

    private final byte[] key;
    private final byte[] hash;

    public SerializableKey(byte[] key, byte[] hash) {
      this.key = key;
      this.hash = hash;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] hash() {
      return hash;
    }

    @Override
    public void ensureHash(HashCalculator hashCalculator) {}
  }
}
