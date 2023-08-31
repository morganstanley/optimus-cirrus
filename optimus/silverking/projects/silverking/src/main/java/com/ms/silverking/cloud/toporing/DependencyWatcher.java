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
package com.ms.silverking.cloud.toporing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.toporing.meta.StoragePolicyGroupZK;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.cloud.toporing.meta.WeightsZK;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.thread.ThreadUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches ring dependencies and builds a new ring if any changes are detected. */
public class DependencyWatcher implements VersionListener {
  private final DependencyWatcherOptions options;
  private final MetaClient mc;
  private final com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;
  private final com.ms.silverking.cloud.meta.MetaClient cloudMC;
  private final MetaPaths mp;
  private final boolean ignoreFeasibility;
  private final boolean ignoreSource;
  private final Set<String> updatesReceived;
  private final boolean exitAfterBuild;
  private final SKGridConfiguration gridConfig;
  private Map<String, Long> lastBuild;
  private final BlockingQueue<Map<String, Long>> buildQueue;
  private final int consecutiveUpdateGuardSeconds;
  private final int _requiredInitialUpdates;

  private static Logger log = LoggerFactory.getLogger(DependencyWatcher.class);

  private static final int buildDelayMillis = 2 * 1000;
  private static final int invalidTopologyDelayMillis = 2 * 1000;
  private static final int requiredInitialUpdates = 6;

  private static final String logFileNameSuffix = "DependencyWatcher.out";

  public DependencyWatcher(SKGridConfiguration gridConfig, DependencyWatcherOptions options)
      throws IOException, KeeperException {
    NamedRingConfiguration ringConfig;
    ZooKeeperConfig zkConfig;
    long intervalMillis;

    this.options = options;
    this.gridConfig = gridConfig;
    exitAfterBuild = options.exitAfterBuild;
    intervalMillis = options.watchIntervalSeconds * 1000;
    this.ignoreFeasibility = options.ignoreFeasibility;
    this.ignoreSource = options.ignoreSource;
    dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gridConfig);
    zkConfig = dhtMC.getZooKeeper().getZKConfig();
    consecutiveUpdateGuardSeconds =
        options.exitAfterBuild ? 0 : options.consecutiveUpdateGuardSeconds;

    _requiredInitialUpdates =
        options.ignoreInstanceExclusions ? requiredInitialUpdates - 1 : requiredInitialUpdates;

    lastBuild = new HashMap<>();
    buildQueue = new LinkedBlockingQueue<>();

    ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gridConfig);
    mc = new MetaClient(ringConfig, zkConfig);
    cloudMC = mc.createCloudMC();

    /*
     * updatesReceived is used to ensure that we have an update from every version before we trigger a build
     */
    updatesReceived = new ConcurrentSkipListSet<>();

    new SafeThread(new Builder(), "Builder").start();

    mp = mc.getMetaPaths();
    new VersionWatcher(mc, mp.getExclusionsPath(), this, intervalMillis);
    new VersionWatcher(mc, mp.getTopologyPath(), this, intervalMillis);
    new VersionWatcher(mc, mp.getWeightsPath(), this, intervalMillis);
    new VersionWatcher(mc, mp.getStoragePolicyGroupPath(), this, intervalMillis);
    new VersionWatcher(mc, mp.getConfigPath(), this, intervalMillis);
    if (!options.ignoreInstanceExclusions) {
      new VersionWatcher(
          mc, dhtMC.getMetaPaths().getInstanceExclusionsPath(), this, intervalMillis);
    }
  }

  @Override
  public void newVersion(String basePath, long version) {
    if (TopoRingConstants.verbose) {
      log.info("newVersion {} {}", basePath, version);
    }
    updatesReceived.add(basePath);
    if (updatesReceived.size() == _requiredInitialUpdates) {
      triggerBuild();
    }
  }

  private void triggerBuild() {
    try {
      SilverKingZooKeeperClient zk;

      zk = mc.getZooKeeper();
      buildQueue.put(createBuildMap(zk));
    } catch (Exception e) {
      log.error("", e);
    }
  }

  private void build(Map<String, Long> curBuild) {
    try {
      boolean buildOK;
      long ringConfigVersion;

      log.info("New build triggered");
      buildOK = false;
      ringConfigVersion = -1;
      try {
        if (TopoRingConstants.verbose) {
          log.info("Building tree");
        }
        RingTreeRecipe recipe;
        Topology topology;
        WeightSpecifications weightSpecs;
        ExclusionSet exclusionSet;
        ExclusionSet instanceExclusionSet;
        StoragePolicyGroup storagePolicyGroup;
        long topologyVersion;
        long weightsVersion;
        long exclusionVersion;
        long instanceExclusionVersion;
        long storagePolicyGroupVersion;
        RingConfiguration ringConfig;
        SilverKingZooKeeperClient zk;
        HostGroupTable hostGroupTable;
        long hostGroupTableVersion;
        ExclusionSet mergedExclusionSet;

        zk = mc.getZooKeeper();

        topologyVersion = curBuild.get(mp.getTopologyPath());
        weightsVersion = curBuild.get(mp.getWeightsPath());
        exclusionVersion = curBuild.get(mp.getExclusionsPath());
        instanceExclusionVersion = curBuild.get(dhtMC.getMetaPaths().getInstanceExclusionsPath());
        storagePolicyGroupVersion = curBuild.get(mp.getStoragePolicyGroupPath());
        ringConfigVersion = curBuild.get(mp.getConfigPath());

        topology = new TopologyZK(cloudMC).readFromZK(topologyVersion, null);
        weightSpecs = new WeightsZK(mc).readFromZK(weightsVersion, null);

        exclusionSet =
            new ExclusionSet(
                new ServerSetExtensionZK(mc, mc.getMetaPaths().getExclusionsPath())
                    .readFromZK(exclusionVersion, null));
        if (options.ignoreInstanceExclusions) {
          instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
        } else {
          try {
            instanceExclusionSet =
                new ExclusionSet(
                    new ServerSetExtensionZK(mc, dhtMC.getMetaPaths().getInstanceExclusionsPath())
                        .readFromZK(instanceExclusionVersion, null));
          } catch (Exception e) {
            log.info("No instance ExclusionSet found");
            instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
          }
        }
        mergedExclusionSet = ExclusionSet.union(exclusionSet, instanceExclusionSet);

        storagePolicyGroup =
            new StoragePolicyGroupZK(mc).readFromZK(storagePolicyGroupVersion, null);
        ringConfig = new RingConfigurationZK(mc).readFromZK(ringConfigVersion, null);

        log.info("ringConfiguration {}", ringConfig);

        hostGroupTableVersion = zk.getLatestVersion(cloudMC.getMetaPaths().getHostGroupPath());
        hostGroupTable = new HostGroupTableZK(cloudMC).readFromZK(hostGroupTableVersion, null);

        try {
          recipe =
              new RingTreeRecipe(
                  topology,
                  ringConfig.getRingParentName(),
                  weightSpecs,
                  mergedExclusionSet,
                  storagePolicyGroup,
                  ringConfig.getStoragePolicyName(),
                  hostGroupTable,
                  ringConfig.getHostGroups(),
                  ringConfigVersion,
                  DHTUtil.currentTimeMillis());
          log.info("Recipe.ringParent: {}", recipe.ringParent);
        } catch (RuntimeException re) {
          re.printStackTrace(System.out);
          log.info("ringConfig: {}", ringConfig, re);
          log.error("", re);
          throw re;
        }

        RingTree ringTree;
        RingTree prevRingTree;
        long configInstanceVersion;
        String newInstancePath;

        configInstanceVersion = mc.getLatestConfigInstanceVersion(ringConfigVersion);
        if (configInstanceVersion >= 0 && !ignoreSource) {
          prevRingTree = SingleRingZK.readTree(mc, ringConfigVersion, configInstanceVersion);
        } else {
          prevRingTree = null;
        }
        if (prevRingTree == null
            || ignoreFeasibility
            || RingTreeBuilder.convergenceFeasible(
                prevRingTree,
                storagePolicyGroup,
                ringConfig.getStoragePolicyName(),
                ringConfig.getRingParentName(),
                exclusionSet)) {
          ringTree = RingTreeBuilder.create(recipe, prevRingTree);
          // ringTree = RingTreeBuilder.create(recipe, null); // for testing without movement
          // reduction
          newInstancePath = mc.createConfigInstancePath(ringConfigVersion);
          SingleRingZK.writeTree(mc, topologyVersion, newInstancePath, ringTree);

          if (TopoRingConstants.verbose) {
            log.info(".............................");
            log.info(".............................");
            log.info("{}", ringTree);
            log.info(".............................");
            log.info("{}", ringConfigVersion);
            log.info("{}", configInstanceVersion);
            log.info("{}", topologyVersion);
            log.info(newInstancePath);
            log.info("Building complete");
          }
          buildOK = true;
        } else {
          log.info(
              "Convergence is infeasible. A region in prevTree does not have a viable server.");
        }
      } catch (IOException ioe) {
        log.error("", ioe);
      } catch (KeeperException ke) {
        log.error("", ke);
      }
      if (exitAfterBuild) {
        if (buildOK) {
          try {
            if (ringConfigVersion < 0) {
              throw new RuntimeException("ringConfigVersion < 0");
            }
            setRing(ringConfigVersion);
          } catch (KeeperException ke) {
            log.error("", ke);
            buildOK = false;
          }
        }
        System.exit(buildOK ? 0 : -1);
      }
    } catch (RuntimeException re) {
      re.printStackTrace();
    } finally {
      log.info("Leaving build");
    }
  }

  private Map<String, Long> createBuildMap(SilverKingZooKeeperClient zk) throws KeeperException {
    Map<String, Long> b;
    long topologyVersion;
    long weightsVersion;
    long exclusionVersion;
    long instanceExclusionVersion;
    long storagePolicyGroupVersion;
    long ringConfigVersion;

    topologyVersion = zk.getLatestVersion(mp.getTopologyPath());
    weightsVersion = zk.getLatestVersion(mp.getWeightsPath());
    exclusionVersion = zk.getLatestVersion(mp.getExclusionsPath());
    instanceExclusionVersion =
        zk.getLatestVersion(dhtMC.getMetaPaths().getInstanceExclusionsPath());
    storagePolicyGroupVersion = zk.getLatestVersion(mp.getStoragePolicyGroupPath());
    ringConfigVersion = zk.getLatestVersion(mp.getConfigPath());

    b = new HashMap<>();
    b.put(mp.getTopologyPath(), topologyVersion);
    b.put(mp.getWeightsPath(), weightsVersion);
    b.put(mp.getExclusionsPath(), exclusionVersion);
    b.put(dhtMC.getMetaPaths().getInstanceExclusionsPath(), instanceExclusionVersion);
    b.put(mp.getStoragePolicyGroupPath(), storagePolicyGroupVersion);
    b.put(mp.getConfigPath(), ringConfigVersion);
    return b;
  }

  private void setRing(long ringConfigVersion) throws KeeperException {
    long configInstanceVersion;
    String newInstancePath;
    DHTRingCurTargetZK dhtRingCurTargetZK;
    String ringName;

    configInstanceVersion = mc.getLatestConfigInstanceVersion(ringConfigVersion);
    mc.getLatestConfigInstanceVersion(ringConfigVersion);
    dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtMC.getDHTConfiguration());
    ringName = dhtMC.getDHTConfiguration().getRingName();
    dhtRingCurTargetZK.setCurRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion);
    dhtRingCurTargetZK.setTargetRingAndVersionPair(
        ringName, ringConfigVersion, configInstanceVersion);
    dhtRingCurTargetZK.setManagerRingAndVersionPair(
        ringName, ringConfigVersion, configInstanceVersion);
    log.info("Ring {} config {} instance {}", ringName, ringConfigVersion, configInstanceVersion);
  }

  private class Builder implements Runnable {
    Builder() {}

    @Override
    public void run() {
      while (true) {
        try {
          Map<String, Long> curBuild;
          Map<String, Long> _curBuild;

          curBuild = buildQueue.take();
          log.info("Received new build");
          log.info("Checking for consecutive update");
          _curBuild = buildQueue.poll(consecutiveUpdateGuardSeconds, TimeUnit.SECONDS);
          while (_curBuild != null) {
            log.info("Received new build consecutively. Ignoring last received.");
            log.info("Checking for consecutive update");
            curBuild = _curBuild;
            _curBuild = buildQueue.poll(consecutiveUpdateGuardSeconds, TimeUnit.SECONDS);
          }
          if (!lastBuild.equals(curBuild)) {
            build(curBuild);
            lastBuild = curBuild;
          }
        } catch (Exception e) {
          log.error("", e);
          ThreadUtil.pauseAfterException();
        }
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      CmdLineParser parser;
      DependencyWatcherOptions options;
      DependencyWatcher dw;
      SKGridConfiguration gc;

      options = new DependencyWatcherOptions();
      parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
        gc = SKGridConfiguration.parseFile(options.gridConfig);
        TopoRingConstants.setVerbose(true);
        dw = new DependencyWatcher(gc, options);
        ThreadUtil.sleepForever();
      } catch (CmdLineException cle) {
        log.info(cle.getMessage());
        parser.printUsage(System.err);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
