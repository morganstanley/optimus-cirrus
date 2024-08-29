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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class ExecutorsUtil {

  private ExecutorsUtil() {}

  public static final ScheduledExecutorService newSingleThreadDaemonScheduledExecutor(
      String nameFormat) {
    return Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
  }

  public static final ScheduledExecutorService newDaemonScheduledExecutor(
      int threads, String nameFormat) {
    return Executors.newScheduledThreadPool(
        threads, new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
  }

  public static final ExecutorService newExecutor(int threads, String nameFormat) {
    return Executors.newFixedThreadPool(
        threads, new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
  }

  /**
   * Creates an executor wrapper that appends given suffix to the thread name while executing a
   * task.
   */
  public static final ExecutorService threadRenamingExecutorService(
      ExecutorService wrapped, final String nameSuffix) {
    return new WrappingExecutorService(wrapped) {
      @Override
      protected <T> Callable<T> wrapTask(Callable<T> callable) {
        return threadRenaming(callable, nameSuffix);
      }
    };
  }

  /**
   * Creates an scheduled executor wrapper that appends given suffix to the thread name while
   * executing a task.
   */
  public static final ScheduledExecutorService threadRenamingScheduledExecutorService(
      ScheduledExecutorService wrapped, final String nameSuffix) {
    return new WrappingScheduledExecutorService(wrapped) {
      @Override
      protected <T> Callable<T> wrapTask(Callable<T> callable) {
        return threadRenaming(callable, nameSuffix);
      }
    };
  }

  private static String threadName(String oldName, String nameSuffix) {
    return oldName + ":[" + nameSuffix + "]";
  }

  private static <T> Callable<T> threadRenaming(
      final Callable<T> callable, final String nameSuffix) {
    checkNotNull(nameSuffix);
    checkNotNull(callable);
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        Thread currentThread = Thread.currentThread();
        String oldName = currentThread.getName();
        boolean restoreName = trySetName(threadName(oldName, nameSuffix), currentThread);
        try {
          return callable.call();
        } finally {
          if (restoreName) {
            trySetName(oldName, currentThread);
          }
        }
      }
    };
  }

  private static boolean trySetName(final String threadName, Thread currentThread) {
    try {
      currentThread.setName(threadName);
      return true;
    } catch (SecurityException e) {
      return false;
    }
  }
}
