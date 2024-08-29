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
package optimus.dht.common.internal.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.ProvisionListener;

import optimus.dht.common.api.ShutdownAware;
import optimus.dht.common.api.lifecycle.ExternallyProvided;

public class ShutdownAwareListenerModule extends AbstractModule {

  private static final Logger logger = LoggerFactory.getLogger(ShutdownAwareListenerModule.class);

  private final List<ShutdownAware> shutdownAwareComponents = new CopyOnWriteArrayList<>();
  private final List<Shutdownable> customActions = new CopyOnWriteArrayList<>();
  private final List<ExecutorService> executorServices = new CopyOnWriteArrayList<>();

  private static class Shutdownable implements Comparable<Shutdownable> {

    private final int order;
    private final Consumer<Boolean> consumer;

    public Shutdownable(int order, Consumer<Boolean> consumer) {
      this.order = order;
      this.consumer = consumer;
    }

    @Override
    public int compareTo(Shutdownable o) {
      return Integer.compare(order, o.order);
    }
  }

  @Override
  protected void configure() {
    bindListener(
        Matchers.any(),
        new ProvisionListener() {
          @Override
          public <T> void onProvision(ProvisionInvocation<T> provision) {
            T provisioned = provision.provision();
            if (provisioned instanceof ShutdownAware) {
              shutdownAwareComponents.add((ShutdownAware) provisioned);
            } else if ((provisioned instanceof ExecutorService)
                && !(provisioned instanceof ExternallyProvided)) {
              executorServices.add((ExecutorService) provisioned);
            }
          }
        });
  }

  @Provides
  @Singleton
  public ShutdownManager shutdownManager() {
    return new ShutdownManager() {

      @Override
      public void registerCustomAction(int order, Consumer<Boolean> consumer) {
        customActions.add(new Shutdownable(order, consumer));
      }

      @Override
      public void shutdown(boolean gracefully) {
        List<Shutdownable> list = new ArrayList<>(customActions);
        shutdownAwareComponents.forEach(
            component ->
                list.add(new Shutdownable(component.shutdownOrder(), component::shutdown)));
        Collections.sort(list);
        for (Shutdownable shutdownable : list) {
          try {
            shutdownable.consumer.accept(gracefully);
          } catch (Exception e) {
            logger.warn("Exception while shutting down", e);
          }
        }
        for (ExecutorService executorService : executorServices) {
          if (gracefully) {
            executorService.shutdown();
          } else {
            executorService.shutdownNow();
          }
        }
      }
    };
  }
}
