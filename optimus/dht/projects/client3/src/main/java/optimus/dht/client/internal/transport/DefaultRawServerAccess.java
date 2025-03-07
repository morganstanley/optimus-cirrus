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
package optimus.dht.client.internal.transport;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.ConnectionsManager;
import optimus.dht.client.api.transport.RawServerAccess;
import optimus.dht.client.api.transport.RequestIdGenerator;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.SentMessageMetrics;

@Singleton
public class DefaultRawServerAccess implements RawServerAccess {

  private final RequestIdGenerator requestIdGenerator;
  private final CallbackRegistry callbackRegistry;
  private final ConnectionsManager connectionsManager;

  @Inject
  public DefaultRawServerAccess(
      RequestIdGenerator requestIdGenerator,
      CallbackRegistry callbackRegistry,
      ConnectionsManager connectionsManager) {
    this.requestIdGenerator = requestIdGenerator;
    this.callbackRegistry = callbackRegistry;
    this.connectionsManager = connectionsManager;
  }

  @Override
  public void send(
      ServerConnection server,
      short moduleId,
      MessageGeneratorFactory messageGeneratorFactory,
      Duration operationTimeout,
      Callback callback) {
    long requestId = requestIdGenerator.next();
    MessageGenerator messageGenerator = messageGeneratorFactory.build(requestId);
    AtomicReference<SentMessageMetrics> sentMetrics = new AtomicReference<>();
    callbackRegistry.registerCallbackWithTimeout(
        server, requestId, operationTimeout, ctx -> callback.act(sentMetrics.get(), ctx), null);
    connectionsManager.sendMessage(server, moduleId, messageGenerator, sentMetrics::set);
  }
}
