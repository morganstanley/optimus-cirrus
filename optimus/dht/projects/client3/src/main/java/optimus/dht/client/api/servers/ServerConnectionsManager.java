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
package optimus.dht.client.api.servers;

import java.util.Iterator;
import java.util.List;

import optimus.dht.client.api.Key;

public interface ServerConnectionsManager {

  enum ServersState {
    NO_SERVERS,
    ALL_UNREACHABLE,
    MAYBE_REACHABLE
  }

  ServersState currentState();

  List<ServerConnection> getAllServers();

  List<ServerConnection> getActiveServers();

  ServerConnection getPrimaryServerForKey(Key key);

  Iterator<ServerConnection> getServersForKey(Key key);
}
