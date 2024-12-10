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
/*
package msjava.logbackutils.async;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import msjava.logbackutils.async.internal.AsyncDispatcherBasePatch;

import ch.qos.logback.core.Appender;

/** A bounded BlockingQueue backed implementation of the AsynDispatcher interface
public class AsyncDispatcherQueueImplPatch<E> extends AsyncDispatcherBasePatch<E>
    implements AsyncDispatcher<E> {

  /** List of appenders which will be used by this dispatcher
  protected final List<Appender<E>> appenderList = new ArrayList<>();

  /** The produce consumer queue.
  protected BlockingQueue<E> queue;

  /** The dispatch thread
  BaseDispatchThread dispatchThread;

  /**
   * Flag indicating whether the Logging thread have to wait until space become available on the
   * queue<br>
   * or it should return immediately.
  private boolean blocking = true;

  private static final AtomicInteger instanceId = new AtomicInteger();

  public AsyncDispatcherQueueImplPatch() {
    this("AsyncDispatcherQueueImpl-" + instanceId.incrementAndGet());
  }

  /**
   * @param metricsId a unique ID for naming published metrics, not null and not empty; use
   *     parameterless constructor for default
  public AsyncDispatcherQueueImplPatch(String metricsId) {
    super(metricsId);
  }

  /**
   * Creates a bounded linkedblockingqueue implementation of the AsyncDispatcher interface<br>
   * with the size of bufferSize.
   *
   * @param bufferSize the size of the message queue
   * @throws IllegalArgumentException if the bufferSize is less then 1
  public AsyncDispatcherQueueImplPatch(int bufferSize) throws IllegalArgumentException {
    this(bufferSize, "AsyncDispatcherQueueImpl-" + instanceId.incrementAndGet());
  }

  /**
   * Creates a bounded linkedblockingqueue implementation of the AsyncDispatcher interface<br>
   * with the size of bufferSize.
   *
   * @param bufferSize the size of the message queue
   * @param metricsId a unique ID for naming published metrics, not null and not empty; use
   *     parameterless constructor for default
   * @throws IllegalArgumentException if the bufferSize is less then 1
  public AsyncDispatcherQueueImplPatch(int bufferSize, String metricsId)
      throws IllegalArgumentException {
    super(metricsId);
    if (bufferSize < 1) {
      throw new IllegalArgumentException("The size of the buffer must be greater than 0.");
    }
    setBufferSize(bufferSize);
    if (bufferSize > 10000) setUseListQueue(true);
  }

  /**
   * This function is hidden from the client of the logger during runtime, hence no synchronisation
   * is required.
   *
   * @return True when the logging thread have to wait when the queue is full.
  @Override
  public boolean isBlocking() {
    return blocking;
  }

  /**
   * This function is hidden from the client of the logger during runtime, hence no synchronisation
   * is required.
   *
   * @param blocking the blocking to set
  @Override
  public void setBlocking(boolean blocking) {
    addInfo("Blocking flag set to: " + blocking);
    this.blocking = blocking;
  }

  private boolean useListQueue = false;

  public void setUseListQueue(boolean useListedQueue) {
    if (isStarted()) {
      throw new IllegalStateException(
          "The buffer size cannot be set after the dispatcher is started started!");
    }
    addInfo("setUseListedQueue set to: " + useListedQueue);
    this.useListQueue = useListedQueue;
  }

  /**
   * Flushing all messages from the queue to the loggers. All log messages currently on the queue is
   * logged in the current thread,<br>
   * and on the dispatch thread as well. This function guarantees only that, at most queue.size()
   * messages will be flushed.<br>
   * New messages may arrive and will be present on the queue after flushing.
  @Override
  public synchronized void flush() {
    flushing();
  }

  /**
   * This function will put the message onto the queue. If the queue is full, than the current
   * thread will block on the queue.put() call if blocking is set to true.
  @Override
  public void add(E e) {
    if (Thread.currentThread() instanceof AsyncDispatcherQueueImplPatch.BaseDispatchThread) {
      addWarn("Swallowing circular log message: " + e.toString());
      return;
    }

    try {
      if (queue.offer(e)) {
        eventQueued(e, false);
      } else {
        emitQueueFullWarning(e, !blocking);
        if (blocking) {
          queue.put(e);
          eventQueued(e, true);
        }
      }
    } catch (InterruptedException ex) {
      addWarn(
          "logging thread: "
              + Thread.currentThread().getName()
              + " was interrupted while waiting for dispatching, log event was discarded: "
              + e,
          ex);
      eventInterrupted(e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @return the current number of queued messages
   *
  @VisibleForTesting
  int getQueueSize() {
    return queue.size();
  }

  @VisibleForTesting
  BlockingQueue<E> getQueue() {
    return queue;
  }

  @Override
  public synchronized void start() {
    if (isStarted) {
      return;
    }
    super.start();
    startDispatchThread();
    isStarted = true;
    addInfo("Async dispatcher started");
  }

  /** Starts up the dispatcher thread
  protected void startDispatchThread() {
    if (queue == null) {
      queue =
          useListQueue
              ? new LinkedBlockingQueue<E>(getBufferSize())
              : new ArrayBlockingQueue<E>(getBufferSize());
    }
    if (dispatchThread == null) {
      dispatchThread = newDispatchThread();
      dispatchThread.start();
    }
  }

  /** Flag indicating the status of this dispatcher
  private volatile boolean isStarted = false;

  /** Stops the dispatcher thread, and logs all the messages on the current thread.
  @Override
  public synchronized void stop() {
    stopDispatchThread();
    flushing();
    isStarted = false;
    appenderList.clear();
    addInfo("Async dispatcher stopped");
  }

  /** If this appender is started
  @Override
  public boolean isStarted() {
    return isStarted;
  }

  /** Stops the dispatcher thread
  private void stopDispatchThread() {
    if (dispatchThread != null) {
      try {
        dispatchThread.interrupt();
        dispatchThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        dispatchThread = null;
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getName();
  }

  @Override
  public synchronized void setAppenders(Iterator<Appender<E>> appenderIterator) {
    appenderList.clear();
    if (appenderIterator != null) {
      while (appenderIterator.hasNext()) {
        appenderList.add(appenderIterator.next());
      }
    }
  }

  /** Flushes all messages in the current thread.
  private void flushing() {
    addInfo("Flushing is just about to start in the AsyncDispatcher");
    AtomicReference<Exception> ex = new AtomicReference<>();
    Thread flush =
        new DispatchThreadImpl() {
          public void run() {
            try {
              int queueSize = queue.size();
              int i = 0;
              E event = null;

              while (i++ < queueSize && (event = queue.poll()) != null) {
                callAppenders(event);
              }
            } catch (Exception e) {
              ex.set(e);
            }
          }
        };
    flush.start();
    try {
      flush.join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (ex.get() != null) throw new RuntimeException(ex.get());

    addInfo("Flushing finished in the AsyncDispatcher");
  }

  private void doCallAppenders(E e) {
    Appender<E> appender;

    int size = appenderList.size();
    for (int i = 0; i < size; i++) {
      appender = appenderList.get(i);
      try {
        appender.doAppend(e);
      } catch (Throwable t) {
        addError(t.getMessage(), t);
      }
    }
  }

  public abstract static class BaseDispatchThread extends Thread {
    public BaseDispatchThread() {
      setDaemon(true);
      setName("Logback dispatch thread #" + hashCode());
    }
  }

  protected BaseDispatchThread newDispatchThread() {
    return new DispatchThreadImpl();
  }
  /** The dispatcher thread implementation. It runs in a loop until it is interrupted.
  public class DispatchThreadImpl extends BaseDispatchThread {

    /** Creates the dispatcher thread
    public DispatchThreadImpl() {}

    /** Logs everything to Logback. If queue is empty, it will block on the queue.take() call.
    @Override
    public void run() {
      while (!isInterrupted()) {
        try {
          callAppenders(queue.take());
        } catch (InterruptedException e) {
          break;
        } catch (Throwable e) {
          addError("A runtime exception occurred during logging.", e);
        }
      }
    }

    protected void callAppenders(E e) {
      try {
        doCallAppenders(e);
      } finally {
        eventFlushed(e);
      }
    }
  }
}
*/