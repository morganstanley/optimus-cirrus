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

import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.thread.ThreadUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches ring dependencies and builds a new ring if any changes are detected. Always builds with
 * respect to a "manager" ring.
 */
public class ManagerModeDependencyWatcher implements VersionListener {
  private final MetaClient mc;
  private final MetaPaths mp;
  private final com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;
  private final RingTree managerRingTree;
  private final Topology topology;
  private final long ringConfigVersion;
  private final NamedRingConfiguration ringConfig;
  private ResolvedReplicaMap existingReplicaMap;
  private final int consecutiveUpdateGuardSeconds;
  private final BlockingQueue<Map<String, Long>> buildQueue;
  private Map<String, Long> lastBuild;
  private final Set<String> updatesReceived;
  private final int _requiredInitialUpdates;

  private static Logger log = LoggerFactory.getLogger(ManagerModeDependencyWatcher.class);

  public static final boolean verbose = true;

  private static final String logFileName = "ManagerModeDependencyWatcher.out";

  private static final int requiredInitialUpdates = 2;

  public ManagerModeDependencyWatcher(
      SKGridConfiguration gridConfig, ManagerModeDependencyWatcherOptions options)
      throws IOException, KeeperException {
    ZooKeeperConfig zkConfig;
    long intervalMillis;
    long topologyVersion;
    long configInstanceVersion;
    RingTree existingTree;
    Pair<RingTree, Triple<String, Long, Long>> managerRingTreeReadPair;
    Triple<String, Long, Long> managerRingAndVersionPair;

    dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gridConfig);
    zkConfig = dhtMC.getZooKeeper().getZKConfig();
    ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gridConfig);
    mc = new MetaClient(ringConfig, zkConfig);
    mp = mc.getMetaPaths();

    _requiredInitialUpdates =
        options.ignoreInstanceExclusions ? requiredInitialUpdates - 1 : requiredInitialUpdates;

    consecutiveUpdateGuardSeconds = options.consecutiveUpdateGuardSeconds;
    buildQueue = new LinkedBlockingQueue<>();
    lastBuild = new HashMap<>();

    managerRingTreeReadPair = readManagerRingTree(dhtMC, dhtMC.getDHTConfiguration());
    managerRingTree = managerRingTreeReadPair.getV1();
    managerRingAndVersionPair = managerRingTreeReadPair.getV2();
    ringConfigVersion = managerRingAndVersionPair.getV2();

    topologyVersion = dhtMC.getZooKeeper().getLatestVersion(mp.getTopologyPath());
    topology = new TopologyZK(mc.createCloudMC()).readFromZK(topologyVersion, null);

    configInstanceVersion =
        dhtMC.getZooKeeper().getLatestVersion(mp.getConfigInstancePath(ringConfigVersion));
    existingTree = SingleRingZK.readTree(mc, ringConfigVersion, configInstanceVersion);
    existingReplicaMap =
        existingTree.getResolvedMap(
            ringConfig.getRingConfiguration().getRingParentName(), new ReplicaNaiveIPPrioritizer());

    /*
     * updatesReceived is used to ensure that we have an update from every version before we trigger a build
     */
    updatesReceived = new ConcurrentSkipListSet<>();

    intervalMillis = options.watchIntervalSeconds * 1000;
    // We don't need to watch everything that a full DependencyWatcher needs to watch
    new VersionWatcher(mc, mp.getExclusionsPath(), this, intervalMillis);
    new VersionWatcher(
        dhtMC, dhtMC.getMetaPaths().getInstanceExclusionsPath(), this, intervalMillis);

    new Builder();
  }

  private Pair<RingTree, Triple<String, Long, Long>> readManagerRingTree(
      com.ms.silverking.cloud.dht.meta.MetaClient dhtMC, DHTConfiguration dhtConfig)
      throws KeeperException, IOException {
    DHTRingCurTargetZK dhtRingCurTargetZK;
    Triple<String, Long, Long> managerRingAndVersionPair;
    RingTree ringTree;

    dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
    managerRingAndVersionPair = dhtRingCurTargetZK.getManagerRingAndVersionPair();
    if (managerRingAndVersionPair == null) {
      throw new RuntimeException("Can't find manager ring");
    } else {
      ringTree =
          SingleRingZK.readTree(
              mc, managerRingAndVersionPair.getV2(), managerRingAndVersionPair.getV3());
      return new Pair<>(ringTree, managerRingAndVersionPair);
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

  private Map<String, Long> createBuildMap(SilverKingZooKeeperClient zk) throws KeeperException {
    Map<String, Long> b;
    long exclusionVersion;
    long instanceExclusionVersion;

    exclusionVersion = zk.getLatestVersion(mp.getExclusionsPath());
    instanceExclusionVersion =
        zk.getLatestVersion(dhtMC.getMetaPaths().getInstanceExclusionsPath());

    b = new HashMap<>();
    b.put(mp.getExclusionsPath(), exclusionVersion);
    b.put(dhtMC.getMetaPaths().getInstanceExclusionsPath(), instanceExclusionVersion);
    return b;
  }

  /**
   * Build a new ring based off of the Manager ring
   *
   * @param curBuild
   */
  private void build(Map<String, Long> curBuild) {
    try {
      ExclusionSet exclusionSet;
      ExclusionSet instanceExclusionSet;
      long exclusionVersion;
      long instanceExclusionVersion;
      SilverKingZooKeeperClient zk;
      ExclusionSet mergedExclusionSet;
      RingTree newRingTree;
      String newInstancePath;
      ResolvedReplicaMap newReplicaMap;

      zk = mc.getZooKeeper();
      exclusionVersion = curBuild.get(mp.getExclusionsPath());
      instanceExclusionVersion = curBuild.get(dhtMC.getMetaPaths().getInstanceExclusionsPath());

      exclusionSet =
          new ExclusionSet(
              new ServerSetExtensionZK(mc, mc.getMetaPaths().getExclusionsPath())
                  .readFromZK(exclusionVersion, null));
      try {
        instanceExclusionSet =
            new ExclusionSet(
                new ServerSetExtensionZK(mc, dhtMC.getMetaPaths().getInstanceExclusionsPath())
                    .readFromZK(instanceExclusionVersion, null));
      } catch (Exception e) {
        log.info("No instance ExclusionSet found");
        instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
      }
      mergedExclusionSet = ExclusionSet.union(exclusionSet, instanceExclusionSet);
      newRingTree = RingTreeBuilder.removeExcludedNodes(managerRingTree, mergedExclusionSet);

      newReplicaMap =
          newRingTree.getResolvedMap(
              ringConfig.getRingConfiguration().getRingParentName(),
              new ReplicaNaiveIPPrioritizer());
      if (!existingReplicaMap.equals(newReplicaMap)) {
        newInstancePath = mc.createConfigInstancePath(ringConfigVersion);
        SingleRingZK.writeTree(mc, topology, newInstancePath, newRingTree);
        log.info("RingTree written to ZK: {}", newInstancePath);
        existingReplicaMap = newReplicaMap;
      } else {
        log.info("RingTree unchanged. No ZK update.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      log.error("handleExclusionChange() failed ", e);
    }
  }

  private class Builder implements Runnable {
    Builder() {
      new Thread(this).start();
    }

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

  public static void main(String[] args) {
    try {
      ManagerModeDependencyWatcherOptions options;
      CmdLineParser parser;

      options = new ManagerModeDependencyWatcherOptions();
      parser = new CmdLineParser(options);
      try {
        ManagerModeDependencyWatcher dw;
        SKGridConfiguration gc;

        parser.parseArgument(args);
        TopoRingConstants.setVerbose(true);
        gc = SKGridConfiguration.parseFile(options.gridConfig);
        dw = new ManagerModeDependencyWatcher(gc, options);
        ThreadUtil.sleepForever();
      } catch (CmdLineException cle) {
        log.error(cle.getMessage());
        parser.printUsage(System.err);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
