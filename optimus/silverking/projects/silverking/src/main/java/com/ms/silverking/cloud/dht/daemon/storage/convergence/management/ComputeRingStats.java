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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.MetaUtil;
import com.ms.silverking.cloud.dht.management.MetaUtilOptions;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to compute two things: 1) The probability that a given ring will lose data with a given
 * number of randomly failed servers (with permanently lost data on those servers). 2) Whether or
 * not the permanent loss of data on a specific set of servers will cause data loss for a specific
 * ring.
 */
public class ComputeRingStats {
  private final SKGridConfiguration gc;
  private final PrintStream out;
  private final MetaUtil metaUtil;
  private final com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;
  private final String ringParentName; // FUTURE - remove to make functional

  private static Logger log = LoggerFactory.getLogger(ComputeRingStats.class);

  private final ResolvedReplicaMap sMap;
  private final List<IPAndPort> replicas;

  public ComputeRingStats(SKGridConfiguration gc, Triple<String, Long, Long> sourceRing)
      throws IOException, KeeperException {
    this.gc = gc;
    this.out = System.out;

    metaUtil =
        new MetaUtil(
            gc.getClientDHTConfiguration().getName(),
            gc.getClientDHTConfiguration().getZKConfig(),
            MetaUtilOptions.dhtVersionUnspecified);
    dhtMC = metaUtil.getDHTMC();
    ringParentName = metaUtil.getRingConfiguration().getRingParentName();

    sMap = readReplicaMap(sourceRing);

    replicas = ImmutableList.copyOf(sMap.allReplicas());
  }

  private ResolvedReplicaMap readReplicaMap(Triple<String, Long, Long> ring)
      throws IOException, KeeperException {
    return readTree(ring).getResolvedMap(ringParentName, new ReplicaNaiveIPPrioritizer());
  }

  private InstantiatedRingTree readTree(Triple<String, Long, Long> ring)
      throws IOException, KeeperException {
    MetaClient ringMC;
    long ringConfigVersion;
    long configInstanceVersion;
    InstantiatedRingTree ringTree;

    ringConfigVersion = ring.getTail().getV1();
    configInstanceVersion = ring.getTail().getV2();

    ringMC = metaUtil.getRingMC();

    ringTree = SingleRingZK.readTree(ringMC, ringConfigVersion, configInstanceVersion);
    return ringTree;
  }

  private static Triple<String, Long, Long> getRingAndVersionPair(String ringNameAndVersionPair) {
    String[] s;

    s = ringNameAndVersionPair.split(",");
    return new Triple<>(s[0], Long.parseLong(s[1]), Long.parseLong(s[2]));
  }

  private static Triple<String, Long, Long> parseRing(String s) {
    String ringName;

    ringName = s.substring(0, s.indexOf(','));
    return Triple.of(ringName, getVersionPair(s.substring(s.indexOf(',') + 1)));
  }

  private static Pair<Long, Long> getVersionPair(String versionPair) {
    String[] s;

    s = versionPair.split(",");
    return new Pair<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
  }

  private double computeLossProbability(int s) {
    return computeLossProbability(s, 100);
  }

  private double computeLossProbability(int s, int simulations) {
    int losses;

    losses = 0;
    for (int i = 0; i < simulations; i++) {
      if (lossTrial(s)) {
        ++losses;
      }
    }
    return (double) losses / (double) simulations;
  }

  private boolean lossTrial(int s) {
    Set<IPAndPort> lostServers;

    lostServers = generateLostServers(s);
    for (Set<IPAndPort> replicaSet : this.sMap.getReplicaSets()) {
      boolean lost;

      lost = true;
      for (IPAndPort replica : replicaSet) {
        if (!lostServers.contains(replica)) {
          lost = false;
          break;
        }
      }
      if (lost) {
        return true;
      }
    }
    return false;
  }

  private Set<IPAndPort> generateLostServers(int s) {
    Set<IPAndPort> lostServers;

    // Currently a simple algorithm appropriate only when s <<< |replicas|
    lostServers = new HashSet<>();
    while (lostServers.size() < s) {
      int index;
      IPAndPort replica;

      index = ThreadLocalRandom.current().nextInt(replicas.size());
      replica = replicas.get(index);
      if (!lostServers.contains(replica)) {
        lostServers.add(replica);
      }
    }
    return lostServers;
  }

  public void computeLossProbability(int[] serversLost, int simulations) {
    log.info("*******************************");
    for (int s : serversLost) {
      double lp;

      lp = computeLossProbability(s, simulations);
      log.info("%d\t%.7f\n", s, lp);
    }
  }

  ///////////////////////////////////////////////////////////

  public boolean lostData(File lostServerFile) throws IOException {
    return lostData(new FileInputStream(lostServerFile));
  }

  private boolean lostData(FileInputStream in) throws IOException {
    Set<IPAndPort> lostServers;
    BufferedReader reader;
    String line;

    reader = new BufferedReader(new InputStreamReader(in));
    lostServers = new HashSet<>();
    do {
      line = reader.readLine();
      if (line != null) {
        line = line.trim();
        if (line.length() > 0) {
          lostServers.add(new IPAndPort(line, 0 /*gc.getClientDHTConfiguration().getPort()*/));
        }
      }
    } while (line != null);
    return lostData(lostServers);
  }

  public boolean lostData(Set<IPAndPort> lostServers) {
    // out.println(lostServers);
    for (Set<IPAndPort> replicaSet : this.sMap.getReplicaSets()) {
      boolean lost;

      lost = true;
      // out.println(replicaSet);
      for (IPAndPort replica : replicaSet) {
        // out.println(replica);
        if (!lostServers.contains(replica)) {
          lost = false;
          break;
        }
      }
      if (lost) {
        return true;
      }
    }
    return false;
  }

  public static void main(String[] args) {
    if (args.length < 3) {
      log.error("args: <gridConfig> <sourceRing> <simulations> <serversLost...>");
      log.error("args: <gridConfig> <sourceRing> <serverFile>");
    } else {
      try {
        ComputeRingStats crs;
        SKGridConfiguration gc;
        Triple<String, Long, Long> sourceRing;
        int simulations;
        int[] serversLost;

        gc = SKGridConfiguration.parseFile(args[0]);
        sourceRing = parseRing(args[1]);
        crs = new ComputeRingStats(gc, sourceRing);
        if (args.length > 3) {
          serversLost = new int[args.length - 3];
          simulations = Integer.parseInt(args[2]);
          for (int i = 3; i < args.length; i++) {
            serversLost[i - 3] = Integer.parseInt(args[i]);
          }
          crs.computeLossProbability(serversLost, simulations);
        } else {
          log.info("Result: {}", crs.lostData(new File(args[2])));
        }
        System.exit(0);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
