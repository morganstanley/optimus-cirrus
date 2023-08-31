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
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads new rings when necessary. Presents these as DHTMetaUpdates for compatibility with legacy
 * code.
 */
public class DHTMetaReader {
  private final MetaClient mc;
  private final MetaPaths mp;
  private final ZooKeeperConfig zkConfig;
  private final boolean enableLogging;
  private DHTConfiguration dhtConfig;

  private static Logger log = LoggerFactory.getLogger(DHTMetaReader.class);

  public DHTMetaReader(ZooKeeperConfig zkConfig, String dhtName, boolean enableLogging)
      throws IOException, KeeperException {
    mc = new MetaClient(dhtName, zkConfig);
    mp = mc.getMetaPaths();
    this.zkConfig = zkConfig;
    this.enableLogging = enableLogging;
    dhtConfig = mc.getDHTConfiguration();
  }

  public DHTMetaReader(ZooKeeperConfig zkConfig, String dhtName)
      throws IOException, KeeperException {
    this(zkConfig, dhtName, true);
  }

  public MetaClient getMetaClient() {
    return mc;
  }

  public void setDHTConfig(DHTConfiguration dhtConfig) {
    this.dhtConfig = dhtConfig;
  }

  public DHTConfiguration getDHTConfig() {
    return dhtConfig;
  }

  public DHTMetaUpdate readRing(String curRing, Pair<Long, Long> ringVersionPair)
      throws KeeperException, IOException {
    return readRing(curRing, ringVersionPair.getV1(), ringVersionPair.getV2());
  }

  public DHTMetaUpdate readRing(Triple<String, Long, Long> ringAndVersionPair)
      throws KeeperException, IOException {
    return readRing(
        ringAndVersionPair.getV1(), ringAndVersionPair.getV2(), ringAndVersionPair.getV3());
  }

  public DHTMetaUpdate readRing(String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException, IOException {
    com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;
    NamedRingConfiguration namedRingConfig;
    RingConfiguration ringConfig;
    InstantiatedRingTree ringTree;
    int readAttemptIndex;
    int ringReadAttempts = 20;
    int ringReadRetryInvervalSeconds = 2;

    // unresolved
    namedRingConfig = new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate);
    ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);
    try {
      ringConfig = new RingConfigurationZK(ringMC).readFromZK(ringConfigVersion, null);
    } catch (Exception e) {
      log.info("Ignoring: ", e);
      ringConfig =
          new RingConfiguration(
              new CloudConfiguration(null, null, null), null, null, null, null, null);
    }

    // resolved
    namedRingConfig = new NamedRingConfiguration(ringName, ringConfig);
    if (enableLogging) {
      log.info("ringConfig  {}", ringConfig);
    }
    ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);

    if (enableLogging) {
      log.info("configInstanceVersion:: {}", configInstanceVersion);
    }
    if (configInstanceVersion < 0) {
      throw new RuntimeException("Invalid configInstanceVersion: " + configInstanceVersion);
    }

    // FUTURE - we shouldn't get here unless it's valid. Think about error messages if invalid,
    // instead of waiting.
    log.info(
        "Waiting until valid {}",
        ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion)
            + " "
            + configInstanceVersion);
    SingleRingZK.waitUntilValid(
        ringMC,
        ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion),
        configInstanceVersion);
    log.info("Valid");

    ringTree = null;
    readAttemptIndex = 0;
    while (ringTree == null) {
      try {
        ringTree = SingleRingZK.readTree(ringMC, ringConfigVersion, configInstanceVersion);
      } catch (Exception e) {
        if (++readAttemptIndex >= ringReadAttempts) {
          throw new RuntimeException("Ring read failed", e);
        } else {
          ThreadUtil.sleepSeconds(ringReadRetryInvervalSeconds);
        }
      }
    }
    if (enableLogging) {
      log.info("    ### {}  {}", ringConfigVersion, configInstanceVersion);
    }
    return new DHTMetaUpdate(dhtConfig, namedRingConfig, ringTree, mc);
  }
}
