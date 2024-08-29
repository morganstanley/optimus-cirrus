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
package optimus.dht.common.api.lifecycle;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.ForwardingExecutorService;

public class ExternallyProvidedExecutorService extends ForwardingExecutorService
    implements ExternallyProvided {

  private final ExecutorService executorService;

  public ExternallyProvidedExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  protected ExecutorService delegate() {
    return executorService;
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("Cannot call shutdown on externally provided executor");
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException(
        "Cannot call shutdownNow on externally provided executor");
  }
}
