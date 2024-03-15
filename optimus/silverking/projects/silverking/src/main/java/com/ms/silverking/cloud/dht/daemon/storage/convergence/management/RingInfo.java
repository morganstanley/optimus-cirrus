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

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.MetaUtil;
import com.ms.silverking.cloud.dht.management.MetaUtilOptions;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingInfo {
  private final SKGridConfiguration gc;
  private final PrintStream out;
  private final MetaUtil metaUtil;
  private final com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;
  private final String ringParentName; // FUTURE - remove to make functional
  private final DHTRingCurTargetZK dhtRingCurTargetZK;
  private final Triple<String, Long, Long> curRingAndVersionPair;

  private static Logger log = LoggerFactory.getLogger(RingInfo.class);

  public RingInfo(SKGridConfiguration gc) throws IOException, KeeperException, ClientException {
    this.gc = gc;
    this.out = System.out;

    metaUtil =
        new MetaUtil(
            gc.getClientDHTConfiguration().getName(),
            gc.getClientDHTConfiguration().getZKConfig(),
            MetaUtilOptions.dhtVersionUnspecified);
    dhtMC = metaUtil.getDHTMC();
    ringParentName = metaUtil.getRingConfiguration().getRingParentName();

    dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtMC.getDHTConfiguration());
    curRingAndVersionPair = dhtRingCurTargetZK.getCurRingAndVersionPair();
  }

  public void getRingInfo(Triple<String, Long, Long> ring)
      throws IOException, KeeperException, ClientException {
    ResolvedReplicaMap rMap;
    Map<IPAndPort, Long> aMap;

    if (ring == null) {
      ring = curRingAndVersionPair;
    }
    rMap = readReplicaMap(ring);
    rMap.display();
    aMap = getAllocationMap(rMap);
    for (Map.Entry<IPAndPort, Long> e : aMap.entrySet()) {
      log.info("{} {}", e.getKey(), e.getValue());
    }
  }

  private Map<IPAndPort, Long> getAllocationMap(ResolvedReplicaMap map) {
    Map<IPAndPort, Long> am;

    am = new HashMap<>();
    for (RingEntry entry : map.getEntries()) {
      for (IPAndPort replica : entry.getOwnersIPList(OwnerQueryMode.Primary)) {
        Long curAllocation;

        curAllocation = am.get(replica);
        if (curAllocation == null) {
          curAllocation = Long.valueOf(0);
        }
        curAllocation += entry.getRegion().getSize();
        am.put(replica, curAllocation);
      }
    }
    return am;
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

  private static Pair<Long, Long> getVersionPair(String versionPair) {
    String[] s;

    s = versionPair.split(",");
    return new Pair<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      log.error("args: <gridConfig> [ring...]");
    } else {
      try {
        SKGridConfiguration gc;
        RingInfo ringInfo;

        gc = SKGridConfiguration.parseFile(args[0]);
        ringInfo = new RingInfo(gc);
        if (args.length == 1) {
          ringInfo.getRingInfo(null);
        } else {
          for (int i = 1; i < args.length; i++) {
            ringInfo.getRingInfo(getRingAndVersionPair(args[i]));
          }
        }
        System.exit(0);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
