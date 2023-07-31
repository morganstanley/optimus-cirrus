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
package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingChangeListener;
import com.ms.silverking.cloud.toporing.meta.RingConfigWatcher;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observers ZooKeeper for changes in metadata that this DHT is dependent upon.
 * <p>
 * Currently, this comprises two types of data: the DHT configuration, and
 * the ring tree (only one currently supported) that this DHT uses.
 */
public class DHTMetaWatcher implements VersionListener, RingChangeListener {
  private final MetaClient mc;
  private final MetaPaths mp;
  //private final com.ms.silverking.cloud.toporing.meta.MetaPaths   topoMP;
  private final ZooKeeperConfig zkConfig;
  private volatile RingConfigWatcher ringConfigWatcher;
  private final long intervalMillis;
  private volatile DHTConfiguration dhtConfig;
  private final boolean enableLogging;
  private final List<DHTMetaUpdateListener> dhtMetaUpdateListeners;

  private static Logger log = LoggerFactory.getLogger(DHTMetaWatcher.class);

  public DHTMetaWatcher(ZooKeeperConfig zkConfig, String dhtName, long intervalMillis, boolean enableLogging)
      throws IOException, KeeperException {
    mc = new MetaClient(dhtName, zkConfig);
    this.intervalMillis = intervalMillis;
    mp = mc.getMetaPaths();
    this.zkConfig = zkConfig;
    new VersionWatcher(mc, mp.getInstanceConfigPath(), this, intervalMillis, 0);
    //new VersionWatcher(mc.getZooKeeper(), mp.getTopologyPath(), this, intervalMillis);
    this.enableLogging = enableLogging;
    dhtMetaUpdateListeners = Collections.synchronizedList(new ArrayList<DHTMetaUpdateListener>());
  }

  public DHTMetaWatcher(ZooKeeperConfig zkConfig, String dhtName, long intervalMillis)
      throws IOException, KeeperException {
    this(zkConfig, dhtName, intervalMillis, true);
  }

  public ZooKeeperConfig getZKConfig() {
    return zkConfig;
  }

  public MetaClient getMetaClient() {
    return mc;
  }

  public void waitForDHTConfiguration() {
    synchronized (this) {
      while (dhtConfig == null) {
        try {
          this.wait();
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  public void addListener(DHTMetaUpdateListener listener) {
    dhtMetaUpdateListeners.add(listener);
  }

  public DHTConfiguration getDHTConfiguration() {
    return dhtConfig;
  }

  /**
   * Called when a new DHT configuration is found. Reads the configuration and starts a ring configuration
   * watcher for it. Stops the old ring configuration watcher if there is one.
   */
  private void newDHTConfiguration() {
    try {
      synchronized (this) {
        dhtConfig = mc.getDHTConfiguration();
        if (enableLogging) {
          log.info("DHTMetaWatcher.newDHTConfiguration: {}" , dhtConfig);
        }
        if (dhtConfig != null) {
          this.notifyAll();
        } else {
          log.info("Ignoring null dhtConfig");
        }
      }
      //readRing(dhtConfig.getRingName(), dhtConfig.getZKID());
      startRingConfigWatcher(dhtConfig.getRingName());
    } catch (Exception e) {
      log.warn("",e);
    }
  }

  /**
   * Start a new ring configuration watcher, stopping the old if it exists.
   */
  private void startRingConfigWatcher(String ringName) {
    if (enableLogging) {
      log.info("DHTMetaWatcher.startRingConfigWatcher: {}" , ringName);
    }
    synchronized (this) {
      try {
        if (ringConfigWatcher != null) {
          ringConfigWatcher.stop();
        }
        ringConfigWatcher = new RingConfigWatcher(zkConfig, ringName, intervalMillis, enableLogging, this);
      } catch (Exception e) {
        log.warn("",e);
      }
    }
  }

  @Override
  public void ringChanged(String ringName, String basePath, Pair<Long, Long> version, long creationTimeMillis) {
    try {
      if (enableLogging) {
        log.info("ringChanged: {}", ringName);
      }
      // We use MAX_VALUE below because if we happen to read a newer ring, it's not a problem
      // TODO (OPTIMUS-0000): however versioning in readRing needs to be considered
      // We want to make sure that we read consistent rings and don't read the same ring twice
      readRing(dhtConfig.getRingName(), Long.MAX_VALUE);
    } catch (Exception e) {
      log.warn("",e);
    }
  }

  // TODO (OPTIMUS-0000): think about zkidLimit
  public void readRing(String ringName, long zkidLimit) throws KeeperException, IOException {
    com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;
    //com.ms.silverking.cloud.meta.MetaClient             cloudMC;
    NamedRingConfiguration namedRingConfig;
    RingConfiguration ringConfig;
    InstantiatedRingTree ringTree;
    long ringConfigVersion;
    long configInstanceVersion;
    DHTMetaUpdate dhtMetaUpdate;
    int readAttemptIndex;
    int ringReadAttempts = 20;
    int ringReadRetryInvervalSeconds = 2;
    SilverKingZooKeeperClient zk;

    //cloudMC = new com.ms.silverking.cloud.meta.MetaClient(ringConfig.getCloudConfiguration(),
    //                                                      zkConfig);

    zk = mc.getZooKeeper();

    // unresolved
    namedRingConfig = new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate);
    ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);
    ringConfigVersion = zk.getVersionPriorTo(ringMC.getMetaPaths().getConfigPath(), zkidLimit);
    ringConfig = new RingConfigurationZK(ringMC).readFromZK(ringConfigVersion, null);

    // resolved
    namedRingConfig = new NamedRingConfiguration(ringName, ringConfig);
    ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);
    if (enableLogging) {
      log.info("ringConfig  {}", ringConfig);
    }

    configInstanceVersion = zk.getVersionPriorTo(ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion),
        zkidLimit);
    if (enableLogging) {
      log.info("configInstanceVersion: {}" , configInstanceVersion);
    }
    if (configInstanceVersion == -1) {
      configInstanceVersion = 0;
    }

    if (DHTConstants.isDaemon || log.isDebugEnabled()) {
      log.debug("Waiting until valid {}  {}" , ringMC.getMetaPaths().getConfigInstancePath(
          ringConfigVersion) ,configInstanceVersion);
    }
    SingleRingZK.waitUntilValid(ringMC, ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion),
        configInstanceVersion);
    if (DHTConstants.isDaemon || log.isDebugEnabled()) {
      log.debug("Valid");
    }

    ringTree = null;
    readAttemptIndex = 0;
    while (ringTree == null) {
      try {
        ringTree = SingleRingZK.readTree(ringMC, ringConfigVersion, configInstanceVersion);//, weightsVersion);
      } catch (Exception e) {
        if (++readAttemptIndex >= ringReadAttempts) {
          throw new RuntimeException("Ring read failed", e);
        } else {
          ThreadUtil.sleepSeconds(ringReadRetryInvervalSeconds);
        }
      }
    }
    if (enableLogging) {
      log.info("    ###  {}  {}" , ringConfigVersion , configInstanceVersion);
    }
    dhtMetaUpdate = new DHTMetaUpdate(dhtConfig, namedRingConfig, ringTree, mc);
    notifyListeners(dhtMetaUpdate);
    //ringUpdateListener.newRingTree(new NamedRingConfiguration(ringName, ringConfig), ringTree);
    //ringUpdateListener.newRingTree(ringTree.getMap(ringConfig.getRingParentName()));
  }

  /*
   * Called when the dht configuration changes.
   */
  @Override
  public void newVersion(String basePath, long version) {
    if (enableLogging) {
      log.debug("DHTMetaWatcher.newVersion: {}  {}" , basePath , version);
    }
    if (basePath.equals(mp.getInstanceConfigPath())) {
      newDHTConfiguration();
    } else {
      log.info("Unexpected update in DHTMetaWatcher: {}" , basePath);
    }
  }

  // FUTURE - provide an ordering guarantee
  private void notifyListeners(DHTMetaUpdate metaUpdate) {
    for (DHTMetaUpdateListener listener : dhtMetaUpdateListeners) {
      listener.dhtMetaUpdate(metaUpdate);
    }
  }

  /**
   */

  static class RingUpdateListenerTest implements DHTMetaUpdateListener {
    @Override
    public void dhtMetaUpdate(DHTMetaUpdate dhtMetaUpdate) {
      log.info("      *** newRing");
      log.info("{}",dhtMetaUpdate);
    }
  }

  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        System.out.println("args: <zkConfig> <mapConfig> <intervalSeconds>>");
      } else {
        ZooKeeperConfig zkConfig;
        DHTMetaWatcher dw;
        String dhtName;
        long intervalMillis;

        zkConfig = new ZooKeeperConfig(args[0]);
        dhtName = args[1];
        intervalMillis = Integer.parseInt(args[2]) * 1000;
        dw = new DHTMetaWatcher(zkConfig, dhtName, intervalMillis);
        dw.addListener(new RingUpdateListenerTest());
        Thread.sleep(60 * 60 * 1000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
