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
package optimus.dht.client.internal.kv;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.kv.KVClient;
import optimus.dht.client.api.module.ClientModuleSpecification;
import optimus.dht.client.api.transport.ClientProtocolVersionHandler;
import optimus.dht.client.internal.kv.stream.KVClientProtocolVersionHandler;

@Singleton
public class KVModuleSpecification implements ClientModuleSpecification<KVClient> {

  private final KVClient kvClient;
  private final CallbackRegistry callbackRegistry;

  @Inject
  public KVModuleSpecification(KVClient kvClient, CallbackRegistry callbackRegistry) {
    this.kvClient = kvClient;
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  public KVClient getModuleClient() {
    return kvClient;
  }

  @Override
  public ClientProtocolVersionHandler buildProtocolVersionHandler() {
    return new KVClientProtocolVersionHandler(callbackRegistry);
  }
}
