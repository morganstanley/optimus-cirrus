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

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.net.IPAndPort;
import optimus.utils.zookeeper.ReadOnlyDistributedValue;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Used to track nodes that are suspected of being bad. Each node in an instance may accuse other
 * nodes of being suspicious. For each node, a list of suspects is maintained. Thus the
 * mapping in zookeeper is accuser->list of suspects
 */
public class SuspectsZK {
  private static long timerPeriodMillis = Long.getLong("com.ms.silverking.cloud.dht.meta.SuspectsZK.timerPeriodMillis",
                                                       10000);
  private static final char suspectsDelimiter = ',';
  private static final String suspectsDelimiterString = "" + suspectsDelimiter;

  private static Logger log = LoggerFactory.getLogger(SuspectsZK.class);

  private static final String emptySetDef = "<empty>";

  private TimerTask suspectsTimerTask;
  private Timer suspectsTimer;

  private SilverKingZooKeeperClient zkClient;
  private String suspectsPath;
  private IPAndPort ipAndPort;

  public SuspectsZK(SilverKingZooKeeperClient zkClient,
                    String suspectsPath,
                    IPAndPort ipAndPort,
                    StrongSuspectSet strongSuspectSet) {

    this.zkClient = zkClient;
    this.suspectsPath = suspectsPath;
    this.ipAndPort = ipAndPort;

    String path = suspectsPath + "/" + ipAndPort;
    ReadOnlyDistributedValue<Set<IPAndPort>> zkSuspectSet = new ReadOnlyDistributedValue<>(zkClient.getCurator()
                                                                                                   .getCurator(),
                                                                                           zkClient.getCurator()
                                                                                                   .getResolvedPath(path),
                                                                                           this::deserializeZkSuspectsList,
                                                                                           false) {
      @Override
      public void onNodeChange(Option<Set<IPAndPort>> set) {
        Set<IPAndPort> suspectSet;
        suspectSet = set.isEmpty() ? new HashSet<>() : set.get();
        log.warn("Current Suspects: {}", CollectionUtil.toString(suspectSet));
      }
    };

    this.suspectsTimerTask = new TimerTask() {
      @Override
      public void run() {
        startZkUpdate();
        Set<IPAndPort> localSuspects = strongSuspectSet.computeCurrentStrongSuspects();
        Option<Set<IPAndPort>> zkSuspects = zkSuspectSet.value();
        if (zkSuspects.isEmpty() || localSuspects != zkSuspects.get()) {
          writeSuspectsToZK(ipAndPort, localSuspects);
        }
        finishZkUpdate();
      }
    };
    this.suspectsTimer = new Timer();
    suspectsTimer.scheduleAtFixedRate(suspectsTimerTask, 0, timerPeriodMillis);
  }

  // Test method
  protected void startZkUpdate() { }

  // Test method
  protected void finishZkUpdate() { }

  private Set<IPAndPort> deserializeZkSuspectsList(byte[] nodeData) {
    Set<IPAndPort> suspectSet;

    if (nodeData != null) {
      String suspects = new String(nodeData, StandardCharsets.UTF_8);
      suspectSet = nodeToIPAndPortSet(suspects);
    } else {
      suspectSet = new HashSet<>();
    }
    return suspectSet;
  }

  private static Set<IPAndPort> nodeToIPAndPortSet(String suspectsDef) {
    Set<IPAndPort> suspects = new HashSet<>();
    if (!suspectsDef.equals(emptySetDef)) {
      suspectsDef.trim();
      suspectsDef = suspectsDef.substring(1, suspectsDef.length() - 1);
      for (String suspect : suspectsDef.split(suspectsDelimiterString)) {
        suspects.add(new IPAndPort(suspect));
      }
    }
    return suspects;
  }

  /**
   * Read accuser->suspects map from zookeeper.
   *
   * @return accuser->suspects map
   * @throws KeeperException
   */
  public static Pair<Set<IPAndPort>, SetMultimap<IPAndPort, IPAndPort>> readAccuserSuspectsFromZK(
      SilverKingZooKeeperClient zkClient,
      String suspectsPath) throws KeeperException {
    List<IPAndPort> accusers;
    SetMultimap<IPAndPort, IPAndPort> accuserSuspectsMap;

    accusers = IPAndPort.list(zkClient.getChildren(suspectsPath));
    accuserSuspectsMap = HashMultimap.create();
    for (IPAndPort accuser : accusers) {
      Set<IPAndPort> suspects;

      suspects = readSuspectsFromZK(zkClient, suspectsPath, accuser);
      accuserSuspectsMap.putAll(accuser, suspects);
    }
    return new Pair<>(ImmutableSet.copyOf(accusers), accuserSuspectsMap);
  }

  public static Set<IPAndPort> readActiveNodesFromZK(SilverKingZooKeeperClient zkClient, String suspectsPath)
      throws KeeperException {
    return ImmutableSet.copyOf(IPAndPort.list(zkClient.getChildren(suspectsPath)));
  }

  private void writeSuspectsToZK(IPAndPort accuser, Set<IPAndPort> suspects) {
    try {
      String path;
      path = suspectsPath + "/" + accuser;
      if (zkClient.exists(path)) {
        zkClient.setString(path, CollectionUtil.toString(suspects, suspectsDelimiter));
      } else {
        zkClient.createString(path, CollectionUtil.toString(suspects, suspectsDelimiter), CreateMode.EPHEMERAL);
      }
    } catch (KeeperException ke) {
      log.error("Unable to write suspects for: {}", accuser, ke);
    }
  }

  public Set<IPAndPort> readSuspectsFromZK() {
    return readSuspectsFromZK(zkClient, suspectsPath, ipAndPort);
  }

  public static Set<IPAndPort> readSuspectsFromZK(SilverKingZooKeeperClient zkClient,
                                                  String suspectsPath,
                                                  IPAndPort accuser) {
    Set<IPAndPort> suspects;
    String suspectsDef;

    suspects = new HashSet<>();
    try {
      suspectsDef = zkClient.getString(suspectsPath + "/" + accuser.toString());
      if (suspectsDef != null) {
        suspects = nodeToIPAndPortSet(suspectsDef);
      }
    } catch (KeeperException ke) {
      log.error("Unable to read suspects for: {}, zk: {}", accuser, zkClient, ke);
    }
    return suspects;
  }
}
