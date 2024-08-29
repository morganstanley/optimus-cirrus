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
package optimus.dht.client.internal;

import java.time.Duration;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import com.google.inject.Injector;

import optimus.dht.client.api.ClientIdentity;
import optimus.dht.client.api.DHTClient;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.module.ModuleClient;
import optimus.dht.client.api.registry.RegistryObserver;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.transport.RawServerAccess;
import optimus.dht.client.internal.module.ClientModulesAware;
import optimus.dht.client.internal.module.ClientModulesRegistry;
import optimus.dht.common.internal.lifecycle.ShutdownManager;

@Singleton
public class DefaultDHTClient implements DHTClient, ClientModulesAware {

  private final Injector injector;
  private final ClientIdentity clientIdentity;
  private final HashCalculator hashCalculator;
  private final ServerConnectionsManager serverModesManager;
  private final RawServerAccess rawServerAccess;
  private final RegistryObserver registryObserver;
  private final ShutdownManager shutdownManager;

  private volatile Map<Class<?>, Object> moduleClients;

  @Inject
  public DefaultDHTClient(
      Injector injector,
      ClientIdentity clientIdentity,
      HashCalculator hashCalculator,
      ServerConnectionsManager serverModesManager,
      RawServerAccess rawServerAccess,
      RegistryObserver registryObserver,
      ShutdownManager shutdownManager) {
    this.injector = injector;
    this.clientIdentity = clientIdentity;
    this.hashCalculator = hashCalculator;
    this.serverModesManager = serverModesManager;
    this.rawServerAccess = rawServerAccess;
    this.registryObserver = registryObserver;
    this.shutdownManager = shutdownManager;
  }

  @Override
  public void initWithModulesRegistry(ClientModulesRegistry registry) {
    this.moduleClients = registry.moduleClients();
  }

  @Override
  public <T extends ModuleClient> T getModule(Class<T> clientClass) {
    Object moduleClient = moduleClients.get(clientClass);
    if (moduleClient == null) {
      throw new IllegalArgumentException("No client known for " + clientClass);
    }
    return (T) moduleClient;
  }

  @Override
  public ClientIdentity getClientIdentity() {
    return clientIdentity;
  }

  @Override
  public HashCalculator getHashCalculator() {
    return hashCalculator;
  }

  @Override
  public ServerConnectionsManager getServerConnectionsManager() {
    return serverModesManager;
  }

  @Override
  public RawServerAccess getRawServerAccess() {
    return rawServerAccess;
  }

  @Override
  public void waitForConsistentRegistry(Duration timeout) throws InterruptedException {
    registryObserver.waitUntilConsistentForFirstTime(timeout);
  }

  @Override
  public void shutdown(boolean gracefully) {
    shutdownManager.shutdown(gracefully);
  }

  @Override
  public Injector experimentalGetInjector() {
    return injector;
  }
}
