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
package optimus.dht.common.api.transport;

import optimus.dht.common.api.RemoteInstanceIdentity;
import optimus.dht.common.internal.transport.AuthenticatedConnection;

public class RemoteConnectionDetails {

  private final RemoteInstanceIdentity remoteInstanceIdentity;
  private final AuthenticatedConnection authenticatedConnection;

  public RemoteConnectionDetails(
      RemoteInstanceIdentity remoteInstanceIdentity,
      AuthenticatedConnection authenticatedConnection) {
    this.remoteInstanceIdentity = remoteInstanceIdentity;
    this.authenticatedConnection = authenticatedConnection;
  }

  public RemoteInstanceIdentity remoteInstanceIdentity() {
    return remoteInstanceIdentity;
  }

  public AuthenticatedConnection authenticatedConnection() {
    return authenticatedConnection;
  }

  @Override
  public String toString() {
    return "[remoteInstanceIdentity="
        + remoteInstanceIdentity
        + ", auth="
        + authenticatedConnection
        + "]";
  }
}
