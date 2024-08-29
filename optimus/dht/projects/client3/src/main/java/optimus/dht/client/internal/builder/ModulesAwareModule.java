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
package optimus.dht.client.internal.builder;

import java.util.HashSet;
import java.util.Set;

import optimus.dht.client.internal.module.ClientModulesAware;
import optimus.dht.client.internal.module.ClientModulesRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.ProvisionListener;

public class ModulesAwareModule extends AbstractModule {

  private final Set<ClientModulesAware> modulesAware = new HashSet<>();

  @Override
  protected void configure() {
    bindListener(
        Matchers.any(),
        new ProvisionListener() {
          @Override
          public <T> void onProvision(ProvisionInvocation<T> provision) {
            T provisionValue = provision.provision();
            if (provisionValue instanceof ClientModulesAware) {
              modulesAware.add((ClientModulesAware) provisionValue);
            }
          }
        });
  }

  public Set<ClientModulesAware> getModulesAware() {
    return modulesAware;
  }

  public void init(ClientModulesRegistry registry) {
    for (ClientModulesAware modulesAware : modulesAware) {
      modulesAware.initWithModulesRegistry(registry);
    }
  }
}
