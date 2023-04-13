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
package com.ms.silverking.cloud.dht.daemon;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.StrongSuspectSet;
import com.ms.silverking.cloud.dht.meta.SuspectsZK;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.SuspectAddressListener;
import com.ms.silverking.net.async.SuspectProblem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerHealthMonitor implements SuspectAddressListener, StrongSuspectSet {
  /*
   For all peers, we store a health status by the daemon IPAndPort.
   A peer that is unhealthy is a "suspect". We divide suspects into two classes:
    1) Strong suspects. High confidence that they are in a bad state.
    2) Weak suspects. Not acting normally, but are not known to be in a bad state. E.g. slow peers.
   Code that uses the term "suspect" without qualification, refers to strong suspects.
   */
  private final ConcurrentMap<IPAndPort, PeerHealthStatus> healthStatusMap;
  private final IPAliasMap aliasMap;
  private final IPAndPort localIPAndPort;
  private final SuspectsZK suspectsZK;

  /*
   * Weak suspects are still members of the system. They are simply de-prioritized for reads.
   * Any truly bad member needs to be added as a suspect proper, which may trigger a topology change.
   */

  private static final boolean verbose = true;
  private static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(PeerHealthMonitor.class);

  public PeerHealthMonitor(MetaClient mc, IPAndPort localIPAndPort, IPAliasMap aliasMap) throws KeeperException {
    healthStatusMap = new ConcurrentHashMap<>();
    if (mc != null) {
      suspectsZK = new SuspectsZK(mc.getZooKeeper(), mc.getMetaPaths().getInstanceSuspectsPath(), localIPAndPort, this);
    } else {
      log.warn("PeerHealthMonitor in unit test mode");
      suspectsZK = null;
    }
    this.localIPAndPort = localIPAndPort;
    this.aliasMap = aliasMap;
  }

  // only used by testing and replicahealthprioritizer presently...
  public boolean isStrongSuspect(IPAndPort peer) {
    PeerHealthStatus peerHealthStatus;

    peerHealthStatus = healthStatusMap.get(peer);
    return peerHealthStatus != null && peerHealthStatus.isStrongSuspect();
  }

  // used by networking code
  @Override
  public void addSuspect(InetSocketAddress peerInterfaceAddr, Object rawCause) {
    PeerHealthIssue issue;
    IPAndPort peerIPAndPort;

    // First, we convert any network module SuspectProblems
    // into dht module PeerHealthIssues
    if (rawCause instanceof PeerHealthIssue) {
      issue = (PeerHealthIssue) rawCause;
    } else if (rawCause instanceof SuspectProblem) {
      switch ((SuspectProblem) rawCause) {
        case ConnectionEstablishmentFailed:
          issue = PeerHealthIssue.CommunicationError;
          break;
        case CommunicationError:
          issue = PeerHealthIssue.CommunicationError;
          break;
        default:
          throw new RuntimeException("Panic");
      }
    } else {
      throw new RuntimeException("Panic");
    }
    // Now ensure that we convert any interface ip:port into a daemon IP where necessary
    peerIPAndPort = aliasMap.interfaceToDaemon(peerInterfaceAddr);
    addSuspect(peerIPAndPort, issue);
  }

  public void addSelfAsSuspect(PeerHealthIssue issue) {
    addSuspect(localIPAndPort, issue);
  }

  private PeerHealthStatus getOrCreatePeerHealthStatus(IPAndPort peer) {
    PeerHealthStatus peerHealthStatus;

    peerHealthStatus = healthStatusMap.get(peer);
    if (peerHealthStatus == null) {
      PeerHealthStatus _peerHealthStatus;

      peerHealthStatus = new PeerHealthStatus();
      _peerHealthStatus = healthStatusMap.putIfAbsent(peer, peerHealthStatus);
      if (_peerHealthStatus != null) {
        peerHealthStatus = _peerHealthStatus;
      }
    }
    return peerHealthStatus;
  }

  public void addSuspect(IPAndPort peer, PeerHealthIssue issue) {
    long curTimeMillis;
    PeerHealthStatus peerHealthStatus;

    log.warn("PeerHealthMonitor.addSuspect: {} {}", peer, issue);
    curTimeMillis = SystemTimeUtil.timerDrivenTimeSource.absTimeMillis();
    peerHealthStatus = getOrCreatePeerHealthStatus(peer);
    if (debug) {
      log.warn("addIssue {} {}", issue, curTimeMillis);
    }
    peerHealthStatus.addIssue(issue, curTimeMillis);
  }

  // called when a new connection is made
  @Override
  public void removeSuspect(InetSocketAddress peerInterfaceAddr) {
    IPAndPort peer;

    peer = aliasMap.interfaceToDaemon(peerInterfaceAddr);
    removeSuspect(peer);
  }

  // (in addition to above) called when a ping ack is received
  public void removeSuspect(IPAndPort peer) {
    long curTimeMillis;
    PeerHealthStatus peerHealthStatus;

    if (verbose) {
      if (isStrongSuspect(peer)) {
        log.warn("PeerHealthMonitor.removeSuspect {}", peer);
      }
    }
    curTimeMillis = SystemTimeUtil.timerDrivenTimeSource.absTimeMillis();
    peerHealthStatus = getOrCreatePeerHealthStatus(peer);
    peerHealthStatus.setHealthy(curTimeMillis);
  }

  public Set<IPAndPort> computeCurrentStrongSuspects() {
    Set<IPAndPort> strongSuspects;

    strongSuspects = new HashSet<>();
    for (Map.Entry<IPAndPort, PeerHealthStatus> entry : healthStatusMap.entrySet()) {
      if (entry.getValue().isStrongSuspect()) {
        strongSuspects.add(entry.getKey());
      }
    }
    return ImmutableSet.copyOf(strongSuspects);
  }
}
