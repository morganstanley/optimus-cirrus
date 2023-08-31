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
package com.ms.silverking.cloud.toporing.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.ms.silverking.cloud.meta.MetaClientCore;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches a ring configuration for changes. Notifies listeners when changes occur. */
public class RingConfigWatcher implements VersionListener {
  private final String ringName;
  private final long intervalMillis;
  private final MetaClientCore mc;
  private volatile VersionWatcher configVersionWatcher;
  private final List<RingChangeListener> listeners;
  private final boolean enableLogging;

  private static Logger log = LoggerFactory.getLogger(RingConfigWatcher.class);

  private static final int zkReadAttempts = 300;
  private static final int zkReadRetryIntervalMillis = 2 * 1000;

  public RingConfigWatcher(
      ZooKeeperConfig zkConfig,
      String ringName,
      long intervalMillis,
      boolean enableLogging,
      RingChangeListener initialListener)
      throws IOException, KeeperException {
    listeners = Collections.synchronizedList(new ArrayList<RingChangeListener>());
    if (initialListener != null) {
      addListener(initialListener);
    }
    this.ringName = ringName;
    this.intervalMillis = intervalMillis;
    mc = new MetaClientCore(zkConfig);
    configVersionWatcher =
        new VersionWatcher(mc, MetaPaths.getRingConfigPath(ringName), this, intervalMillis, 0);
    this.enableLogging = enableLogging;
  }

  public RingConfigWatcher(ZooKeeperConfig zkConfig, String ringName, long intervalMillis)
      throws IOException, KeeperException {
    this(zkConfig, ringName, intervalMillis, true, null);
  }

  public void stop() {
    configVersionWatcher.stop();
  }

  public void addListener(RingChangeListener listener) {
    listeners.add(listener);
  }

  @Override
  public void newVersion(String basePath, long version) {
    if (enableLogging) {
      log.info("RingConfigWatcher.newVersion() {} {}", basePath, version);
    }
    /*
     * We can reach here for two reasons:
     *   1 - A new ring configuration has been stored
     *   2 - A new configuration instance has been stored
     */
    if (basePath.equals(MetaPaths.getRingConfigPath(ringName))) {
      // We have a new ring configuration
      if (enableLogging) {
        log.info("New config path for ring: {}", ringName);
      }
      startConfigurationWatch(version);
    } else {
      Pair<Long, Long> versionPair;
      long ringConfigVersion;
      long creationTime;
      int attemptIndex;
      boolean ringIsValid;

      // We have a new configuration instance
      ringConfigVersion = getRingConfigVersionFromPath(basePath);
      versionPair = new Pair<>(ringConfigVersion, version);
      if (enableLogging) {
        log.info("New ring instance: {} {}", ringName, versionPair);
      }

      ringIsValid = false;
      attemptIndex = 0;
      do {
        try {
          MetaClient _mc;

          _mc =
              MetaClient.createMetaClient(
                  ringName, ringConfigVersion, mc.getZooKeeper().getZKConfig());
          ringIsValid = SingleRingZK.treeIsValid(_mc, versionPair);
        } catch (KeeperException | IOException e) {
          log.error("", e);
        }
        if (!ringIsValid) {
          ThreadUtil.sleep(zkReadRetryIntervalMillis);
        }
        ++attemptIndex;
      } while (!ringIsValid && attemptIndex < zkReadAttempts);
      if (!ringIsValid) {
        log.info("Validity verification timed out: {} {}", ringName, versionPair);
        return;
      }

      creationTime = Long.MIN_VALUE;
      attemptIndex = 0;
      do {
        try {
          creationTime =
              mc.getZooKeeper()
                  .getCreationTime(SilverKingZooKeeperClient.padVersionPath(basePath, version));
        } catch (KeeperException e) {
          log.error("", e);
        }
        if (creationTime < 0) {
          ThreadUtil.sleep(zkReadRetryIntervalMillis);
        }
        ++attemptIndex;
      } while (creationTime < 0 && attemptIndex < zkReadAttempts);
      if (creationTime < 0) {
        log.info("Ignoring ring due to zk exceptions: {} {}", ringName, versionPair);
        return;
      }

      informRingListeners(basePath, versionPair, creationTime);
    }
  }

  private long getRingConfigVersionFromPath(String path) {
    int configIndex;
    int instanceIndex;

    configIndex = path.indexOf("config");
    if (configIndex < 0) {
      throw new RuntimeException("No config in: " + path);
    }
    instanceIndex = path.indexOf("instance", configIndex);
    if (instanceIndex < 0) {
      throw new RuntimeException("No instance in: " + path);
    }
    return Long.parseLong(path.substring(configIndex + "config".length() + 1, instanceIndex - 1));
  }

  private void informRingListeners(String basePath, Pair<Long, Long> version, long creationTime) {
    for (RingChangeListener listener : listeners) {
      listener.ringChanged(ringName, basePath, version, creationTime);
    }
  }

  private void startConfigurationWatch(long version) {
    synchronized (this) {
      if (configVersionWatcher != null) {
        configVersionWatcher.stop();
      }
      configVersionWatcher =
          new VersionWatcher(
              mc, MetaPaths.getRingConfigInstancePath(ringName, version), this, intervalMillis);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        log.info("args: <zkConfig> <ringName> <intervalSeconds>");
      } else {
        ZooKeeperConfig zkConfig;
        RingConfigWatcher rw;
        String ringName;
        long intervalMillis;

        zkConfig = new ZooKeeperConfig(args[0]);
        ringName = args[1];
        intervalMillis = Integer.parseInt(args[2]) * 1000;
        rw = new RingConfigWatcher(zkConfig, ringName, intervalMillis);
        ThreadUtil.sleepForever();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
