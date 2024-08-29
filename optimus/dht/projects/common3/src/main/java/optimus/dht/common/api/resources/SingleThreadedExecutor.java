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
package optimus.dht.common.api.resources;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded executor with bounded queue. Caller blocks if the queue
 * is full.
 */

public class SingleThreadedExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SingleThreadedExecutor.class);

  private static final Runnable POISON_PILL = new Runnable() {
    @Override
    public void run() {
    }
  };

  private final BlockingQueue<Runnable> queue;
  private final Thread thread;

  public SingleThreadedExecutor(final boolean exitJvmOnFatal, int queueSize) {
    this.queue = new ArrayBlockingQueue<Runnable>(queueSize);
    this.thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            try {
              Runnable runnable = queue.take();
              if (runnable == POISON_PILL) {
                return;
              }
              runnable.run();
            } catch (Exception ex) {
              LOG.error("got exception", ex);
            }
          }
        } catch (Throwable e) {
          if (exitJvmOnFatal) {
            LOG.error("Caught a throwable, will exit the JVM", e);
            System.exit(1);
          } else {
            LOG.error("Caught a throwable, will rethrow", e);
            throw e;
          }
        }
      }
    }, "dht-processing-thread");

    this.thread.start();
  }

  public void execute(Runnable runnable) {
    try {
      queue.put(runnable);
    } catch (InterruptedException ex) {
      // should never happen
      LOG.error("Unexpected exception caught", ex);
    }
  }

  public void close() {
    LOG.info("Stopping SingleThreadedExecutor");
    try {
      queue.put(POISON_PILL);
    } catch (InterruptedException e) {
      LOG.error("Unexpected exception caught", e);
    }
  }
}
