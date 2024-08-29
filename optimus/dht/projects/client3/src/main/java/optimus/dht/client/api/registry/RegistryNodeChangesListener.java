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
package optimus.dht.client.api.registry;

import java.util.Set;

import javax.annotation.Nullable;

import optimus.dht.client.api.servers.ServerConnection;

/** Interface implemented by listeners that receive notifications about changes in dht servers. */
public interface RegistryNodeChangesListener {

  /**
   * Called when servers set has changed.
   *
   * @param newServers a new set of servers.
   */
  void nodesRegistryChanged(Set<ServerConnection.ServerIdentity> newServers);

  /**
   * Called when registry becomes unavailable (e.g. connection to the underlying registry is lost).
   *
   * @param e exception that caused registry to become unavailable, if known
   */
  void nodesRegistryUnavailable(@Nullable Exception e);

  /** Called when registry becomes available again. */
  void nodesRegistryAvailable();
}
