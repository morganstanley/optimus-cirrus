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

import java.util.Map;

import optimus.dht.client.api.transport.ClientProtocolVersionHandler;

public class ClientModulesRegistry {

  private final Map<Class<?>, Object> moduleClients;
  private final Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers;

  public ClientModulesRegistry(
      Map<Class<?>, Object> moduleClients,
      Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers) {
    this.moduleClients = moduleClients;
    this.protocolVersionHandlers = protocolVersionHandlers;
  }

  public Map<Class<?>, Object> moduleClients() {
    return moduleClients;
  }

  public Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers() {
    return protocolVersionHandlers;
  }
}
