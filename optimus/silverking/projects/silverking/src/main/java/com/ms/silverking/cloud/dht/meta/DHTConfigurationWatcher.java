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

import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches for new DHTConfigurations */
public class DHTConfigurationWatcher implements VersionListener {
  private final MetaClient mc;
  private final MetaPaths mp;
  private final ZooKeeperConfig zkConfig;
  private volatile DHTConfiguration dhtConfig;
  private final boolean enableLogging;
  private final List<DHTConfigurationListener> dhtConfigurationListeners;

  private static Logger log = LoggerFactory.getLogger(DHTConfigurationWatcher.class);

  public DHTConfigurationWatcher(
      ZooKeeperConfig zkConfig, String dhtName, long intervalMillis, boolean enableLogging)
      throws IOException, KeeperException {
    mc = new MetaClient(dhtName, zkConfig);
    mp = mc.getMetaPaths();
    this.zkConfig = zkConfig;
    new VersionWatcher(mc, mp.getInstanceConfigPath(), this, intervalMillis, 0);
    this.enableLogging = enableLogging;
    dhtConfigurationListeners = Collections.synchronizedList(new ArrayList<>());
  }

  public DHTConfigurationWatcher(ZooKeeperConfig zkConfig, String dhtName, long intervalMillis)
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

  public void addListener(DHTConfigurationListener listener) {
    dhtConfigurationListeners.add(listener);
  }

  public DHTConfiguration getDHTConfiguration() {
    return dhtConfig;
  }

  /** Called when a new DHT configuration is found. Reads the configuration notifies listeners. */
  private void newDHTConfiguration() {
    try {
      synchronized (this) {
        DHTConfiguration _dhtConfig;

        _dhtConfig = mc.getDHTConfiguration();
        dhtConfig = _dhtConfig;
        if (enableLogging) {
          log.info("DHTConfigurationWatcher.newDHTConfiguration: {}", dhtConfig);
        }
        if (dhtConfig != null) {
          this.notifyAll();
        } else {
          log.info("Ignoring null dhtConfig");
        }
        notifyListeners(_dhtConfig);
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }

  /*
   * Called when the dht configuration changes.
   */
  @Override
  public void newVersion(String basePath, long version) {
    if (enableLogging) {
      log.debug("DHTConfigurationWatcher.newVersion: {}  {}", basePath, version);
    }
    if (basePath.equals(mp.getInstanceConfigPath())) {
      newDHTConfiguration();
    } else {
      log.info("Unexpected update in DHTConfigurationWatcher: {}", basePath);
    }
  }

  // FUTURE - provide an ordering guarantee
  private void notifyListeners(DHTConfiguration dhtConfiguration) {
    for (DHTConfigurationListener listener : dhtConfigurationListeners) {
      listener.newDHTConfiguration(dhtConfiguration);
    }
  }
}
