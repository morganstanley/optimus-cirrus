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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.management.OpCompletionTracker;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.meta.DHTMetaUpdate;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoOpResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoProgressMessageGroup;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.toporing.InvalidRingException;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingTree;
import com.ms.silverking.cloud.toporing.RingTreeBuilder;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConvergenceControllerBase implements RequestController {
  protected final UUIDBase uuid;
  protected final DHTConfiguration dhtConfig;
  protected final String targetRingName;
  protected final DHTMetaUpdate targetRing;
  protected final ConvergencePoint targetCP;
  private final DHTMetaReader dhtMetaReader;
  protected final ExclusionSet exclusionSet;
  protected final MessageGroupBase mgBase;
  protected final ResolvedReplicaMap targetReplicaMap;
  private final Map<UUIDBase, NamespaceRequest> nsRequests;
  protected final OpCompletionTracker opCompletionTracker;
  protected final com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;
  protected final RingConfiguration targetRingConfig;
  protected SyncController syncController;
  protected final Set<UUIDBase> opUUIDs;
  protected final RingConfigurationZK ringConfigZK;
  private final Set<Long> ignoredNamespaces;

  private boolean abandoned;
  private boolean failed;
  protected boolean complete;
  private final Lock completionLock;
  private final Condition completionCV;

  protected static final boolean verbose = true;
  protected static final boolean debug = true;

  protected static Logger log = LoggerFactory.getLogger(ConvergenceControllerBase.class);

  public ConvergenceControllerBase(
      UUIDBase uuid,
      DHTMetaReader dhtMetaReader,
      ConvergencePoint targetCP,
      ExclusionSet exclusionSet,
      MessageGroupBase mgBase,
      Set<Long> ignoredNamespaces)
      throws KeeperException, IOException {
    assert dhtMetaReader != null;
    assert targetCP != null;
    this.uuid = uuid;
    dhtConfig = dhtMetaReader.getDHTConfig();
    this.targetCP = targetCP;
    this.dhtMetaReader = dhtMetaReader;
    this.exclusionSet = exclusionSet;
    this.mgBase = mgBase;
    this.ignoredNamespaces = ignoredNamespaces;
    targetRingName = getRingNameFromCP(targetCP);
    targetRing =
        dhtMetaReader.readRing(
            targetRingName, targetCP.getRingIDAndVersionPair().getRingVersionPair());

    syncController =
        new SyncController(mgBase, targetCP, targetCP, SystemTimeUtil.skSystemTimeSource);

    NamedRingConfiguration namedRingConfig;

    namedRingConfig = new NamedRingConfiguration(targetRingName, RingConfiguration.emptyTemplate);
    ringMC =
        new com.ms.silverking.cloud.toporing.meta.MetaClient(
            namedRingConfig, dhtMetaReader.getMetaClient().getZooKeeper().getZKConfig());

    ringConfigZK = new RingConfigurationZK(ringMC);
    log.info("Reading ring configuration: {}", targetRing.getRingIDAndVersionPair());
    targetRingConfig =
        ringConfigZK.readFromZK(
            targetRing.getRingIDAndVersionPair().getRingVersionPair().getV1(), null);
    log.info("Target ring configuration: {}", targetRingConfig);

    log.info("Reading ring: {}", targetRing.getRingIDAndVersionPair());
    targetReplicaMap =
        targetRing
            .getRingTree()
            .getResolvedMap(targetRingConfig.getRingParentName(), new ReplicaNaiveIPPrioritizer());
    log.info("Reading ring complete: {}", targetRing.getRingIDAndVersionPair());
    displayReplicas();

    nsRequests = new HashMap<>();
    opCompletionTracker = new OpCompletionTracker();

    completionLock = new ReentrantLock();
    completionCV = completionLock.newCondition();

    opUUIDs = new ConcurrentSkipListSet<>();
  }

  public UUIDBase getUUID() {
    return uuid;
  }

  protected ResolvedReplicaMap getResolvedReplicaMap(
      DHTMetaUpdate metaUpdate, RingConfiguration ringConfig) {
    RingTree ringTreeMinusExclusions;
    ResolvedReplicaMap resolvedReplicaMapMinusExclusions;

    try {
      ringTreeMinusExclusions =
          RingTreeBuilder.removeExcludedNodes(metaUpdate.getRingTree(), exclusionSet);
    } catch (InvalidRingException ire) {
      throw new RuntimeException("Unexpected InvalidRingException", ire);
    }
    resolvedReplicaMapMinusExclusions =
        ringTreeMinusExclusions.getResolvedMap(ringConfig.getRingParentName(), null);
    return resolvedReplicaMapMinusExclusions;
  }

  private void displayReplicas() {
    log.info("Replicas: ");
    for (IPAndPort replica : targetReplicaMap.allReplicas()) {
      log.info("{}", replica);
    }
  }

  protected Set<IPAndPort> getTargetReplicasWithPorts() {
    return getReplicasWithPorts(targetReplicaMap);
  }

  protected Set<IPAndPort> getReplicasWithPorts(ResolvedReplicaMap resolvedReplicaMap) {
    Set<IPAndPort> replicas;

    replicas = new HashSet<>(resolvedReplicaMap.allReplicas().size());
    for (IPAndPort replica : resolvedReplicaMap.allReplicas()) {
      replicas.add(new IPAndPort(replica.getIP(), dhtConfig.getPort()));
    }
    replicas.removeAll(IPAndPort.set(exclusionSet.getServers(), dhtConfig.getPort()));
    return replicas;
  }

  protected String getRingNameFromCP(ConvergencePoint cp) {
    try {
      return dhtMetaReader.getMetaClient().getDHTConfiguration().getRingName();
    } catch (KeeperException ke) {
      throw new RuntimeException(ke);
    }
  }

  //////////////////////////////////////////////////////////////////

  // FUTURE - Some code is replicated in SKAdmin. Deduplicate this code.

  public void handleOpResponse(MessageGroup message, MessageGroupConnection connection) {
    if (opUUIDs.contains(message.getUUID())) {
      if (debug) {
        log.info(
            "Received op response for: {}  from: {} {}",
            message.getUUID(),
            connection.getRemoteSocketAddress(),
            ProtoOpResponseMessageGroup.result(message));
      }
      opCompletionTracker.setComplete(
          message.getUUID(), ProtoOpResponseMessageGroup.result(message));
    } else {
      if (!syncController.isInactive(message.getUUID())) {
        if (debug) {
          log.info(
              "Received op response for: {}  from: {} {}",
              message.getUUID(),
              connection.getRemoteSocketAddress(),
              ProtoOpResponseMessageGroup.result(message));
        }
        syncController.requestComplete(
            message.getUUID(), ProtoOpResponseMessageGroup.result(message));
      } else {
        if (debug) {
          log.info(
              "Received op response for: {}  from: {} {}",
              message.getUUID(),
              connection.getRemoteSocketAddress(),
              "Ignoring duplicate result");
        }
      }
    }
  }

  public void handleProgress(MessageGroup message, MessageGroupConnection connection) {
    Pair<Long, Long> progress;

    progress = ProtoProgressMessageGroup.progress(message);
    syncController.updateProgress(message.getUUID(), progress);
  }

  //////////////////////////////////////////////////////////////////

  private static final int nsRequestTimeoutMillis = 2 * 60 * 1000;
  private NamespaceRequest nr;

  protected Set<Long> getAllNamespaces() throws ConvergenceException {
    boolean ok;

    if (nr != null) {
      throw new RuntimeException("Unexpected reset of NamespaceRequest");
    }
    log.info("Getting all namespaces {}", targetRing.getRingIDAndVersionPair());
    nr = new NamespaceRequest(mgBase, getTargetReplicasWithPorts(), nsRequests);
    nr.requestNamespacesFromPeers();
    checkForAbandonment();
    ok = nr.waitForCompletion(nsRequestTimeoutMillis);
    if (!ok) {
      throw new ConvergenceException("getAllNamespaces failed");
    } else {
      Set<Long> nrMinusIgnored;

      log.info(
          "Received all namespaces {}\n{}",
          targetRing.getRingIDAndVersionPair(),
          CollectionUtil.numberSetToHexString(nr.getNamespaces()));
      nrMinusIgnored = new HashSet<>(nr.getNamespaces());
      log.info(
          "Removing ignored namespaces {}", CollectionUtil.numberSetToHexString(ignoredNamespaces));
      nrMinusIgnored.removeAll(ignoredNamespaces);
      return ImmutableSet.copyOf(nrMinusIgnored);
    }
  }

  public void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
    Set<Long> namespaces;

    if (debug) {
      log.info("Received namespace response from: {}", connection.getRemoteSocketAddress());
    }
    namespaces = ImmutableSet.copyOf(ProtoNamespaceResponseMessageGroup.getNamespaces(message));
    nr.peerComplete(connection.getRemoteIPAndPort(), namespaces);
  }

  //////////////////////////////////////////////////////////////////

  private long lastDisplayedRequests;
  private long requests;
  private static final long requestDisplayInterval = 1000;

  protected Action syncReplica(
      long ns,
      RingRegion region,
      IPAndPort newOwner,
      IPAndPort oldOwner,
      Action upstreamDependency,
      List<ReplicaSyncRequest> srList)
      throws ConvergenceException {
    if (!newOwner.equals(oldOwner)) {
      ReplicaSyncRequest replicaSyncRequest;

      /*
      Log.infoAsyncf("syncReplica %x %s\t%s <- %s", ns, region, newOwner, oldOwner);
      sendChecksumTreeRequest(new ChecksumTreeRequest(targetCP, curCP, region, oldOwner), ns, newOwner);
      Log.infoAsyncf("Done syncReplica %x %s\t%s <- %s", ns, region, newOwner, oldOwner);
      */
      replicaSyncRequest =
          ReplicaSyncRequest.of(ns, region, newOwner, oldOwner, upstreamDependency);
      srList.add(replicaSyncRequest);
      // Log.infof("Adding %s", replicaSyncRequest);
      syncController.addAction(replicaSyncRequest);
      if (++requests - lastDisplayedRequests > requestDisplayInterval) {
        lastDisplayedRequests = requests;
        log.info("Requests: {}", requests);
      }
      return replicaSyncRequest;
    } else {
      return upstreamDependency;
    }
  }

  public RequestState getRequestState() {
    if (abandoned || failed) {
      return RequestState.FAILED;
    } else {
      return complete ? RequestState.SUCCEEDED : RequestState.INCOMPLETE;
    }
  }

  public void abandon() {
    abandoned = true;
    syncController.abandon();
    waitForCompletion();
  }

  protected void checkForAbandonment() throws ConvergenceException {
    if (abandoned) {
      throw new ConvergenceException("Abandoned");
    }
  }

  protected void setComplete(boolean succeeded) {
    completionLock.lock();
    try {
      failed = !succeeded;
      complete = true;
      completionCV.signalAll();
    } finally {
      completionLock.unlock();
    }
  }

  protected void waitForCompletion() {
    completionLock.lock();
    try {
      while (!complete) {
        try {
          completionCV.await();
        } catch (InterruptedException e) {
        }
      }
    } finally {
      completionLock.unlock();
    }
  }

  ///////////////////////////////////////////////////

  protected void ensureUUIDMatches(UUIDBase uuid) {
    if (!uuid.equals(this.uuid)) {
      throw new RuntimeException("Unexpected UUID: " + uuid + " != " + this.uuid);
    }
  }

  @Override
  public void stop(UUIDBase uuid) {
    ensureUUIDMatches(uuid);
  }

  @Override
  public void waitForCompletion(UUIDBase uuid) {
    ensureUUIDMatches(uuid);
  }
}
