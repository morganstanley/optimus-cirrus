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
package optimus.dht.client.internal.kv.stream;

import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.ClientInitialStreamHandler;
import optimus.dht.client.api.transport.ClientProtocolVersionHandler;
import optimus.dht.common.api.transport.InitialExchangeException;
import optimus.dht.common.api.transport.RemoteConnectionDetails;
import optimus.dht.common.util.DHTVersion;

public class KVClientProtocolVersionHandler implements ClientProtocolVersionHandler {

  private final CallbackRegistry callbackRegistry;

  public KVClientProtocolVersionHandler(CallbackRegistry callbackRegistry) {
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  public int clientPreferredProtocolVersion() {
    return 1;
  }

  @Override
  public ClientInitialStreamHandler buildInitialHandler(
      int protocolVersion,
      ServerConnection serverNode,
      RemoteConnectionDetails remoteConnectionDetails)
      throws InitialExchangeException {
    switch (protocolVersion) {
      case 1:
        return new KVClientInitialStreamHandlerV1(callbackRegistry);
      default:
        throw new InitialExchangeException("Only version 1 of KV protocol is supported");
    }
  }

  @Override
  public String codeVersion() {
    return DHTVersion.getVersion();
  }
}
