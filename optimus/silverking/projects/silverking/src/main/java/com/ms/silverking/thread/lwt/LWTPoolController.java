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
package com.ms.silverking.thread.lwt;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches LWTThreadPools and adjusts the number of threads in each pool.
 */
class LWTPoolController implements Runnable {
  private final List<LWTPoolImpl> pools;
  private boolean running;
  private Lock lock;
  private Condition cv;

  private static Logger log = LoggerFactory.getLogger(LWTPoolController.class);

  private static final int checkIntervalMillis = PropertiesHelper.systemHelper.getInt(
      LWTConstants.lwtControllerCheckInterval, 1000);
  private static final boolean debugPool = false;

  LWTPoolController(String name) {
    if (LWTConstants.enableLogging) {
      log.debug("LWTPoolController()");
    }
    pools = new CopyOnWriteArrayList<>();
    running = true;
    lock = new ReentrantLock();
    cv = lock.newCondition();
    Thread t = ThreadUtil.newDaemonThread(this, "LWTPoolController." + name);
    t.setUncaughtExceptionHandler(new LWTUncaughtExceptionHandler());
    t.start();
  }

  public void check(LWTPoolImpl pool) {
    lock.lock();
    try {
      cv.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void addPool(LWTPoolImpl pool) {
    pools.add(pool);
  }

  private void checkPool(LWTPoolImpl pool) {
    pool.checkThreadLevel();
    if (debugPool) {
      pool.debug();
    }
  }

  private void checkAllPools() {
    if (LWTConstants.enableLogging) {
      log.debug("LWTPoolController.checkAllPools()");
    }
    for (LWTPoolImpl pool : pools) {
      checkPool(pool);
    }
  }

  public void run() {
    if (LWTConstants.enableLogging) {
      log.debug("LWTPoolController.run()");
    }
    while (running) {
      try {
        lock.lock();
        try {
          cv.await(checkIntervalMillis, TimeUnit.MILLISECONDS);
        } finally {
          lock.unlock();
        }
        checkAllPools();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void stop() {
    running = false;
  }
}
