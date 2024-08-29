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

import java.util.List;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.servers.ServerConnection;

public class DHTConnectionLostException extends DHTException {

  private final ServerConnection server;
  private final List<SerializableKey> keys;

  public DHTConnectionLostException(
      String msg, ServerConnection server, List<? extends Key> keys, Throwable cause) {
    super(msg, cause);
    this.server = server;
    this.keys = copyKeys(keys);
  }

  public DHTConnectionLostException(
      ServerConnection server, List<? extends Key> keys, Throwable cause) {
    this("Lost connection to " + server + " for keys " + formatKeys(keys), server, keys, cause);
  }

  public ServerConnection server() {
    return server;
  }

  public List<? extends Key> keys() {
    return keys;
  }
}
