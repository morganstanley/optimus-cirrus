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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sibling of SequentialDelegatingExecutor. It implements ScheduledExecutorService using an
 * underlying ScheduledExecutorService instance, but guarantees that all scheduled tasks are
 * executed sequentially, even if underlying executor uses multiple threads.
 *
 * <p>This class only implements a subset of all methods.
 */
public class SequentialDelegatingScheduledExecutorService implements ScheduledExecutorService {

  private static final Logger logger =
      LoggerFactory.getLogger(SequentialDelegatingScheduledExecutorService.class);

  private final ScheduledExecutorService underlyingExecutor;
  private final Queue<QueueElement<?>> queue = new ArrayDeque<>();
  private boolean scheduledOnUnderlying = false;

  private static class QueueElement<E> {

    private final Callable<E> callable;
    private final ScheduledFutureWrapper<E> futureWrapper;

    public QueueElement(Callable<E> callable, ScheduledFutureWrapper<E> futureWrapper) {
      this.callable = callable;
      this.futureWrapper = futureWrapper;
    }

    public void run() {
      try {
        E e = callable.call();
        futureWrapper.complete(e);
      } catch (Exception e) {
        logger.warn("Exception while executing sequentially", e);
        futureWrapper.completeExceptionally(e);
      }
    }
  }

  public SequentialDelegatingScheduledExecutorService(ScheduledExecutorService underlyingExecutor) {
    this.underlyingExecutor = underlyingExecutor;
  }

  private void runInExecutor() {
    // we only execute one element from the queue per run, as we don't want to starve other waiting
    // tasks
    QueueElement element;
    synchronized (queue) {
      element = queue.remove();
      while (element.futureWrapper.isCancelled() && !queue.isEmpty()) {
        element = queue.remove();
      }
    }
    try {
      if (!element.futureWrapper.isCancelled()) {
        element.run();
      }
    } finally {
      synchronized (queue) {
        if (queue.isEmpty()) {
          scheduledOnUnderlying = false;
        } else {
          synchronized (element.futureWrapper) {
            element.futureWrapper.setFuture(underlyingExecutor.submit(this::runInExecutor));
          }
        }
      }
    }
  }

  private void addElementAndSchedule(QueueElement<?> element) {
    synchronized (queue) {
      synchronized (element.futureWrapper) {
        element.futureWrapper.setScheduledFuture(null);
        queue.add(element);
        if (!scheduledOnUnderlying) {
          element.futureWrapper.setFuture(underlyingExecutor.submit(this::runInExecutor));
          scheduledOnUnderlying = true;
        }
      }
    }
  }

  private Callable<Void> runnableToCallable(Runnable runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }

  @Override
  public void execute(Runnable command) {
    addElementAndSchedule(
        new QueueElement<Void>(runnableToCallable(command), new ScheduledFutureWrapper<>()));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    ScheduledFutureWrapper<T> futureWrapper = new ScheduledFutureWrapper<>();
    addElementAndSchedule(new QueueElement<>(task, futureWrapper));
    return futureWrapper;
  }

  @Override
  public Future<?> submit(Runnable task) {
    ScheduledFutureWrapper<Void> futureWrapper = new ScheduledFutureWrapper<>();
    addElementAndSchedule(new QueueElement<>(runnableToCallable(task), futureWrapper));
    return futureWrapper;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ScheduledFutureWrapper<Void> futureWrapper = new ScheduledFutureWrapper<>();
    synchronized (futureWrapper) {
      QueueElement<Void> element = new QueueElement<>(runnableToCallable(command), futureWrapper);
      ScheduledFuture<?> scheduledFuture =
          underlyingExecutor.schedule(() -> addElementAndSchedule(element), delay, unit);
      futureWrapper.setScheduledFuture(scheduledFuture);
    }
    return futureWrapper;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ScheduledFutureWrapper<V> futureWrapper = new ScheduledFutureWrapper<>();
    synchronized (futureWrapper) {
      QueueElement<V> element = new QueueElement<>(callable, futureWrapper);
      ScheduledFuture<?> scheduledFuture =
          underlyingExecutor.schedule(() -> addElementAndSchedule(element), delay, unit);
      futureWrapper.setScheduledFuture(scheduledFuture);
    }
    return futureWrapper;
  }

  @Override
  public boolean isShutdown() {
    return underlyingExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return underlyingExecutor.isTerminated();
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  private static class ScheduledFutureWrapper<V> implements ScheduledFuture<V> {

    private final CompletableFuture<V> completableFuture = new CompletableFuture<>();
    private ScheduledFuture<V> scheduledFuture;
    private Future<V> future;

    private boolean cancelled;

    public void setScheduledFuture(ScheduledFuture scheduledFuture) {
      this.scheduledFuture = scheduledFuture;
    }

    public void setFuture(Future future) {
      this.future = future;
    }

    public boolean complete(V value) {
      return completableFuture.complete(value);
    }

    public boolean completeExceptionally(Throwable ex) {
      return completableFuture.completeExceptionally(ex);
    }

    @Override
    public synchronized long getDelay(TimeUnit unit) {
      return scheduledFuture != null ? scheduledFuture.getDelay(unit) : 0;
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = true;
      completableFuture.cancel(mayInterruptIfRunning);
      if (scheduledFuture != null) {
        return scheduledFuture.cancel(mayInterruptIfRunning);
      } else if (future != null) {
        return future.cancel(mayInterruptIfRunning);
      }
      return true;
    }

    @Override
    public synchronized boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return completableFuture.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return completableFuture.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return completableFuture.get(timeout, unit);
    }
  }
}
