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
package optimus.dht.client.internal.module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;
import jakarta.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;

import optimus.dht.client.api.module.ClientModuleSPI;
import optimus.dht.client.api.module.ClientModuleSpecification;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.ClientProtocolVersionHandler;
import optimus.dht.client.api.transport.ConnectionsManager;
import optimus.dht.client.api.transport.MessageSender;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.SentMessageMetrics;

public class ClientModulesSupport {

  public static ClientModulesRegistry createRegistry(
      Injector injector, List<ClientModuleSPI<?>> moduleSpis) {
    Map<Class<?>, Object> moduleClients = new HashMap<>();
    Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers = new HashMap<>();

    for (ClientModuleSPI<?> moduleSpi : moduleSpis) {
      short moduleId = moduleSpi.getModuleId();
      Injector childInjector = injector.createChildInjector(new ModuleModule(moduleId));
      ClientModuleSpecification<?> moduleSpecification =
          moduleSpi.createModuleSpecification(childInjector);
      if (moduleSpi.getModuleClientClass() != null) {
        moduleClients.put(moduleSpi.getModuleClientClass(), moduleSpecification.getModuleClient());
      }
      protocolVersionHandlers.put(
          Integer.valueOf(moduleId), moduleSpecification.buildProtocolVersionHandler());
    }

    return new ClientModulesRegistry(moduleClients, protocolVersionHandlers);
  }

  private static class ModuleModule extends AbstractModule {
    private final short moduleId;

    public ModuleModule(short moduleId) {
      this.moduleId = moduleId;
    }

    @Provides
    @Singleton
    public MessageSender messageSender(ConnectionsManager connectionsManager) {
      return new ModuleMessageSender(moduleId, connectionsManager);
    }
  }

  private static class ModuleMessageSender implements MessageSender {

    private final short moduleId;
    private final ConnectionsManager connectionsManager;

    public ModuleMessageSender(short moduleId, ConnectionsManager connectionsManager) {
      this.moduleId = moduleId;
      this.connectionsManager = connectionsManager;
    }

    @Override
    public void sendMessage(
        ServerConnection node,
        MessageGenerator messageGenerator,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      connectionsManager.sendMessage(node, moduleId, messageGenerator, metricsCallback);
    }

    @Override
    public boolean offerMessage(
        ServerConnection node,
        MessageGenerator messageGenerator,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      return connectionsManager.offerMessage(node, moduleId, messageGenerator, metricsCallback);
    }
  }
}
