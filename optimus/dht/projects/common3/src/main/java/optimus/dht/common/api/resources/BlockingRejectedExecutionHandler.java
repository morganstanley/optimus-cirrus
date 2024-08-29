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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {

  private static final Logger logger = LoggerFactory.getLogger(BlockingRejectedExecutionHandler.class);

  private static final String STACK_TRACE_INDENT = "\tat ";

  private final Duration warningThreshold;
  private final Duration threadDumpThreshold;

  private Instant lastWarning = Instant.MIN;
  private Instant lastThreadDump;

  public BlockingRejectedExecutionHandler(Duration warningThreshold, Duration threadDumpThreshold) {
    this.warningThreshold = warningThreshold;
    this.threadDumpThreshold = threadDumpThreshold;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    try {
      while (!executor.isShutdown() &&
          !executor.getQueue().offer(r, warningThreshold.toMillis(), TimeUnit.MILLISECONDS)) {

        boolean needsWarning = false;
        boolean needsThreadsDump = false;
        synchronized (this) {
          Instant now = Instant.now();
          if (lastWarning.plus(warningThreshold).isBefore(now)) {
            lastWarning = now;
            needsWarning = true;
          }
          if (lastThreadDump == null) {
            lastThreadDump = now.minus(warningThreshold);
          }
          if (lastThreadDump.plus(threadDumpThreshold).isBefore(now)) {
            lastThreadDump = now;
            needsThreadsDump = true;
          }
        }

        if (needsThreadsDump) {
          logger.warn(
              "Unable to submit a new task to the blocking queue after={}. Stacktraces of all threads:\n{}",
              warningThreshold,
              getAllStackTraces());
        } else if (needsWarning) {
          logger.warn("Unable to submit a new task to the blocking queue after={}", warningThreshold);
        }

      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RejectedExecutionException("Executor was interrupted while the task was waiting to put on work queue", e);
    }
  }

  private String getAllStackTraces() {
    StringBuilder out = new StringBuilder(1024);

    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> threadEntry : stackTraces.entrySet()) {
      Thread thread = threadEntry.getKey();
      StackTraceElement[] stackTrace = threadEntry.getValue();

      out.append("Logging stacktrace for thread ")
          .append(thread.toString())
          .append(" (")
          .append(thread.getState())
          .append(")\n");

      for (StackTraceElement frame : stackTrace) {
        out.append(STACK_TRACE_INDENT).append(frame).append("\n");
      }
    }

    return out.toString();
  }

}
