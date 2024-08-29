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
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import optimus.dht.common.api.transport.InitialExchangeException;

public class KVClientInitialStreamHandlerV1 implements ClientInitialStreamHandler {

  private final CallbackRegistry callbackRegistry;

  public KVClientInitialStreamHandlerV1(CallbackRegistry callbackRegistry) {
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  public byte[] initialEstablished() {
    return null;
  }

  @Override
  public byte[] received(byte[] reveived) throws InitialExchangeException {
    return null;
  }

  @Override
  public boolean isInitialFinished() {
    return true;
  }

  @Override
  public EstablishedStreamHandler createEstablishedHandler(ServerConnection serverNode) {
    return new KVClientEstablishedStreamHandlerV1(serverNode, callbackRegistry);
  }

  @Override
  public void connectionClosed(Throwable exception) {}
}
