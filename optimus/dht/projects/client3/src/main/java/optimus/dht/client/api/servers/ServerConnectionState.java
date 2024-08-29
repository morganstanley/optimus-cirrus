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

public enum ServerConnectionState {
  /** Initial state */
  NEW(true, true), // technically we are not connected here, but we expect to connect when requested
  /** We try to connect for the first time */
  CONNECTING(true, true),
  /** Connection to server established */
  CONNECTED(true, true),
  /**
   * Connection was lost, but server is still present in the registry. Reconnect attempts might be
   * running in the background
   */
  DISCONNECTED(false, false),
  /** Connection to server still valid, but server was removed from the registry */
  ORPHANED(false, true),
  /** During connection establishment, server responded with unexpected identifier */
  UNEXPECTED_ID(false, false),
  /** Connection establishment failed (e.g. unsupported protocol versions or kerberos mismatch) */
  WRONG_PROTOCOL(false, false),
  /** Internal error occurred during connection establishment */
  INTERNAL_ERROR(false, false),
  /** Hostname cannot be resolved */
  BAD_HOSTNAME(false, false),
  /** Terminal state, after server was completely removed from local view */
  REMOVED(false, false),
  /** Terminal state, connection was reestablished, and is represented by a different object */
  REINCARNATED(false, false);

  private final boolean active;
  private final boolean connected;

  ServerConnectionState(boolean active, boolean connected) {
    this.active = active;
    this.connected = connected;
  }

  public boolean active() {
    return active;
  }

  public boolean connected() {
    return connected;
  }
}
