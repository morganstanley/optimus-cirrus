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
package optimus.dht.client.internal.coreprotocol;

import optimus.dht.client.api.module.ClientModuleSPI;
import optimus.dht.client.api.module.ClientModuleSpecification;
import optimus.dht.client.api.module.ModuleClient;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.ClientInitialStreamHandler;
import optimus.dht.client.api.transport.ClientProtocolVersionHandler;
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import optimus.dht.common.api.transport.InitialExchangeException;
import optimus.dht.common.api.transport.RemoteConnectionDetails;
import optimus.dht.common.util.DHTVersion;
import com.google.inject.Injector;

public class CoreProtocolModuleSPI implements ClientModuleSPI<ModuleClient> {

  @Override
  public short getModuleId() {
    return 0;
  }

  @Override
  public ClientModuleSpecification<ModuleClient> createModuleSpecification(Injector coreInjector) {
    return new CoreProtocolClientModuleSpecification();
  }

  @Override
  public Class<ModuleClient> getModuleClientClass() {
    return null;
  }

  static class CoreProtocolClientModuleSpecification
      implements ClientModuleSpecification<ModuleClient> {

    @Override
    public ModuleClient getModuleClient() {
      return null;
    }

    @Override
    public ClientProtocolVersionHandler buildProtocolVersionHandler() {
      return new CoreProtocolClientProtocolVersionHandler();
    }
  }

  static class CoreProtocolClientProtocolVersionHandler implements ClientProtocolVersionHandler {

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
      return new CoreProtocolClientInitialStreamHandlerV1();
    }

    @Override
    public String codeVersion() {
      return DHTVersion.getVersion();
    }
  }

  static class CoreProtocolClientInitialStreamHandlerV1 implements ClientInitialStreamHandler {

    @Override
    public byte[] initialEstablished() {
      return null;
    }

    @Override
    public EstablishedStreamHandler createEstablishedHandler(ServerConnection serverNode) {
      return new CoreProtocolEstablishedStreamHandlerV1();
    }

    @Override
    public byte[] received(byte[] received) throws InitialExchangeException {
      return null;
    }

    @Override
    public boolean isInitialFinished() {
      return true;
    }
  }
}
