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
package optimus.dht.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple executor wrapper that enforces execution in sequential order, but allows to use
 * multi-threaded executor underneath.
 */
public class SequentialDelegatingExecutor implements Executor {

  private static final Logger logger = LoggerFactory.getLogger(SequentialDelegatingExecutor.class);

  private final Executor underlyingExecutor;
  private final List<Runnable> queue = new ArrayList<>();
  private boolean scheduledOnUnderlying = false;

  public SequentialDelegatingExecutor(Executor executor) {
    this.underlyingExecutor = executor;
  }

  @Override
  public void execute(Runnable command) {
    synchronized (queue) {
      queue.add(command);
      if (!scheduledOnUnderlying) {
        underlyingExecutor.execute(this::runInExecutor);
        scheduledOnUnderlying = true;
      }
    }
  }

  private void runInExecutor() {
    // run until queue is empty
    while (true) {
      List<Runnable> toRun;

      // first copy elements, to not execute under the lock
      synchronized (queue) {
        toRun = new ArrayList<>(queue);
        queue.clear();
      }

      for (Runnable runnable : toRun) {
        try {
          runnable.run();
        } catch (Exception e) {
          logger.warn("Unhandled exception while executing sequentially", e);
        }
      }

      // reacquire lock and only exit if queue is still empty
      synchronized (queue) {
        if (queue.isEmpty()) {
          scheduledOnUnderlying = false;
          return;
        }
      }
    }
  }
}
