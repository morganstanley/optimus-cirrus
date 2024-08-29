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
package optimus.dht.client.api;

import java.time.Duration;

import com.google.inject.Injector;

import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.module.ModuleClient;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.transport.RawServerAccess;

public interface DHTClient {

  <T extends ModuleClient> T getModule(Class<T> clientClass);

  ClientIdentity getClientIdentity();

  HashCalculator getHashCalculator();

  ServerConnectionsManager getServerConnectionsManager();

  RawServerAccess getRawServerAccess();

  void waitForConsistentRegistry(Duration timeout) throws InterruptedException;

  void shutdown(boolean gracefully);

  /**
   * EXPERIMENTAL. Do not use without contacting the Optimus Distribution team first.
   *
   * @return an injector that provides unrestricted access to all internal components.
   */
  Injector experimentalGetInjector();
}
