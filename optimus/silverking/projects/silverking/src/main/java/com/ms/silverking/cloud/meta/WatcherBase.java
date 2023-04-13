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
package com.ms.silverking.cloud.meta;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.zookeeper.CancelableObserver;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.util.SafeTimer;
import com.ms.silverking.util.SafeTimerTask;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WatcherBase implements Watcher, CancelableObserver {
  protected final MetaClientCore metaClientCore;
  protected final String basePath;
  protected volatile boolean active;
  private final SafeTimerTask timerTask;
  private final Lock lock;
  private final long minIntervalMillis;
  private final long intervalMillis;
  private final AtomicLong lastCheckMillis;

  private static final Timer _timer;

  private static Logger log = LoggerFactory.getLogger(WatcherBase.class);

  private static final String timerName = "WatcherBaseTimer";

  private static final int minSleepMS = 500;
  private static final int minIntervalReductionMillis = 100;

  private static final LightLinkedBlockingQueue<EventAndWatcher> watchedEventQueue;
  private static final int watchedEventQueueTimeoutSeconds = 10;
  private static final int processRunnerThreads = 6;

  public static AtomicReference<ProcessRunner> processRunner = new AtomicReference();

  static {
    _timer = new SafeTimer(timerName, true);
    watchedEventQueue = new LightLinkedBlockingQueue<>();
    startProcessRunner();
  }

  public WatcherBase(MetaClientCore metaClientCore, Timer timer, String basePath, long intervalMillis,
      long maxInitialSleep) {
    this.metaClientCore = metaClientCore;
    lock = new ReentrantLock();
    if (timer == null) {
      timer = _timer;
    }
    active = true;
    this.basePath = basePath;
    this.intervalMillis = intervalMillis;
    this.minIntervalMillis = Math.max(intervalMillis - minIntervalReductionMillis, 0);
    lastCheckMillis = new AtomicLong();
    timerTask = new SafeTimerTask(new WatcherTimerTask());
    timer.schedule(timerTask, ThreadLocalRandom.current().nextInt(Math.max(minSleepMS, (int) maxInitialSleep + 1)),
        intervalMillis);
  }

  public WatcherBase(MetaClientCore metaClientCore, String basePath, long intervalMillis, long maxInitialSleep) {
    this(metaClientCore, _timer, basePath, intervalMillis, maxInitialSleep);
  }

  public void stop() {
    lock.lock();
    try {
      active = false;
      timerTask.cancel();
    } finally {
      lock.unlock();
    }
  }

  public boolean isActive() {
    return active;
  }

  protected abstract void _doCheck() throws KeeperException;

  protected void doCheck(long curTimeMillis) throws KeeperException {
    if (active) {
      lastCheckMillis.set(curTimeMillis);
      _doCheck();
    }
  }

  protected void doCheck() throws KeeperException {
    if (active) {
      doCheck(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    }
  }

  public void timerRang() throws KeeperException {
    long curTimeMillis;

    curTimeMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
    if (curTimeMillis - lastCheckMillis.get() < minIntervalMillis) {
      log.debug("Ignoring doCheck() as last call was < intervalMillis");
    } else {
      doCheck(curTimeMillis);
    }
  }

  class WatcherTimerTask extends TimerTask {
    WatcherTimerTask() {
    }

    @Override
    public void run() {
      lock.lock();
      try {
        if (active) {
          timerRang();
        }
      } catch (Exception e) {
        log.error("",e);
      } finally {
        lock.unlock();
      }
    }
  }

  public void process(WatchedEvent event) {
    if (active) {
      try {
        watchedEventQueue.put(new EventAndWatcher(event, this));
      } catch (InterruptedException e) {
        log.error("",e);
      }
    }
  }

  static class ProcessRunner implements Runnable {
    private boolean running;

    ProcessRunner() {
      running = true;
      for (int i = 0; i < processRunnerThreads; i++) {
        new SafeThread(this, "WB.ProcessRunner." + i, true).start();
      }
    }

    public void run() {
      while (running) {
        try {
          EventAndWatcher ew;

          ew = watchedEventQueue.poll(watchedEventQueueTimeoutSeconds, TimeUnit.SECONDS);
          if (ew != null) {
            ew.watcherBase.processSafely(ew.event);
          } else {
            // FUTURE - size a pool, or use lwt
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    public void shutdown() {
      running = false;
    }
  }

  public static void stopProcessRunner() {
    ProcessRunner pr = processRunner.getAndSet(null);
    if (pr != null) {
      pr.shutdown();
    }
  }

  public static void startProcessRunner() {
    processRunner.compareAndSet(null, new ProcessRunner());
  }

  static class EventAndWatcher {
    final WatchedEvent event;
    final WatcherBase watcherBase;

    EventAndWatcher(WatchedEvent event, WatcherBase watcherBase) {
      this.event = event;
      this.watcherBase = watcherBase;
    }
  }

  public void processSafely(WatchedEvent event) {
    log.debug("{}",event);
    if (active) {
      switch (event.getType()) {
      case None:
        if (event.getState() == KeeperState.SyncConnected) {
          log.debug("Connected");
          connected(event);
        }
        break;
      case NodeCreated:
        nodeCreated(event);
        break;
      case NodeDeleted:
        nodeDeleted(event);
        break;
      case NodeDataChanged:
        nodeDataChanged(event);
        break;
      case NodeChildrenChanged:
        nodeChildrenChanged(event);
        break;
      default:
        log.info("Unknown event type: {}", event.getType());
      }
    } else {
      log.debug("Ignoring. Not active.");
    }
  }

  public void connected(WatchedEvent event) {
    log.debug("Ignoring connected");
  }

  public void nodeCreated(WatchedEvent event) {
    log.debug("Ignoring nodeCreated");
  }

  public void nodeDeleted(WatchedEvent event) {
    log.debug("Ignoring nodeDeleted");
  }

  public void nodeDataChanged(WatchedEvent event) {
    log.debug("Ignoring nodeDataChanged");
  }

  public void nodeChildrenChanged(WatchedEvent event) {
    log.debug("Ignoring nodeChildrenChanged");
  }
}
