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

import jakarta.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;

import optimus.dht.client.api.kv.KVClient;
import optimus.dht.client.api.module.ClientModuleSPI;
import optimus.dht.client.api.module.ClientModuleSpecification;

public class KVModuleSPI implements ClientModuleSPI<KVClient> {

  public static final short MODULE_ID = 1;

  @Override
  public short getModuleId() {
    return MODULE_ID;
  }

  @Override
  public ClientModuleSpecification<KVClient> createModuleSpecification(Injector coreInjector) {
    return coreInjector
        .createChildInjector(new KVModule())
        .getInstance(KVModuleSpecification.class);
  }

  @Override
  public Class<KVClient> getModuleClientClass() {
    return KVClient.class;
  }

  protected class KVModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(KVClient.class).to(DefaultKVClient.class).in(Singleton.class);
    }
  }
}
