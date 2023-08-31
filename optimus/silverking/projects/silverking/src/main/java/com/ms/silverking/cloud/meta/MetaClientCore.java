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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClientCore {
  protected final ZooKeeperConfig zkConfig;
  private SilverKingZooKeeperClient zk; // only used if not shareZK

  private static Logger log = LoggerFactory.getLogger(MetaClientCore.class);

  private static final int sessionTimeout;

  static {
    sessionTimeout =
        PropertiesHelper.systemHelper.getInt(
            DHTConstants.zookeeperSessionTimeoutProperty, 4 * 60 * 1000);
    log.info("{}  {}", DHTConstants.zookeeperSessionTimeoutProperty, sessionTimeout);
  }

  private static final int connectAttempts = 4;
  private static final double connectionLossSleepSeconds = 5.0;

  private static final int defaultGetZKSleepUnit = 8;
  private static final int defaultGetZKMaxAttempts = 15;

  private static final boolean shareZK = true;
  protected static final ConcurrentMap<ZooKeeperConfig, Lock> lockMap;
  protected static final ConcurrentMap<ZooKeeperConfig, SilverKingZooKeeperClient> zkMap;

  static {
    if (shareZK) {
      lockMap = new ConcurrentHashMap<>();
      zkMap = new ConcurrentHashMap<>();
    } else {
      zkMap = null;
      lockMap = null;
    }
  }

  private Lock acquireLockIfShared(ZooKeeperConfig zkConfig) {
    Lock lock;

    if (shareZK) {
      lock = lockMap.get(zkConfig);
      if (lock == null) {
        lockMap.putIfAbsent(zkConfig, new ReentrantLock());
        lock = lockMap.get(zkConfig);
      }
      lock.lock();
    } else {
      lock = null;
    }
    return lock;
  }

  private void releaseLockIfShared(Lock lock) {
    if (lock != null) {
      lock.unlock();
    }
  }

  private void setZK(ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    Lock lock;
    SilverKingZooKeeperClient _zk;

    lock = acquireLockIfShared(zkConfig);
    try {
      if (shareZK) {
        _zk = zkMap.get(zkConfig);
      } else {
        _zk = null;
      }
      if (_zk == null) {
        log.debug("Getting SilverKingZooKeeperClient for {}", zkConfig);
        zk =
            SilverKingZooKeeperClient.getZooKeeperWithRetries(
                zkConfig, sessionTimeout, connectAttempts);
        log.debug("Done getting SilverKingZooKeeperClient for {}", zkConfig);
        if (shareZK) {
          zkMap.putIfAbsent(zkConfig, zk);
        }
      } else {
        zk = _zk;
      }
    } finally {
      releaseLockIfShared(lock);
    }
  }

  public MetaClientCore(ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this.zkConfig = zkConfig;
    setZK(zkConfig);
  }

  public SilverKingZooKeeperClient _getZooKeeper() {
    if (shareZK) {
      return zkMap.get(zkConfig);
    } else {
      return zk;
    }
  }

  public SilverKingZooKeeperClient getZooKeeper(int getZKMaxAttempts, int getZKSleepUnit)
      throws KeeperException {
    SilverKingZooKeeperClient _zk;
    int attemptIndex;

    assert getZKMaxAttempts > 0;
    assert getZKSleepUnit > 0;
    _zk = null;
    attemptIndex = 0;
    while (_zk == null) {
      _zk = _getZooKeeper();
      if (_zk == null) {
        if (attemptIndex < getZKMaxAttempts - 1) {
          ThreadUtil.randomSleep(getZKSleepUnit << attemptIndex);
          ++attemptIndex;
        } else {
          log.info("getZooKeeper() failed after {} attempts", (attemptIndex + 1));
          throw KeeperException.forMethod("getZooKeeper", new OperationTimeoutException());
        }
      }
    }
    return _zk;
  }

  public SilverKingZooKeeperClient getZooKeeper() throws KeeperException {
    return getZooKeeper(defaultGetZKMaxAttempts, defaultGetZKSleepUnit);
  }

  public void closeZkExtendeed() {
    _getZooKeeper().close();
  }

  public static void clearZkMap() {
    for (SilverKingZooKeeperClient zk : zkMap.values()) {
      zk.close();
    }
    zkMap.clear();
  }

  public void close() {
    // zk.close(); // TODO (OPTIMUS-0000): to be completed
  }
}
