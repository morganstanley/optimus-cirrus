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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Search all replicas for data. Currently, this will only function on relatively small-scale
 * instances.
 */
public class RecoverDataController extends ConvergenceControllerBase implements RequestController {
  private final Set<IPAndPort> targetReplicas;
  private final ResolvedReplicaMap targetMap;

  private static final boolean verbose = true || ConvergenceControllerBase.verbose;
  private static final boolean debug = true || ConvergenceControllerBase.debug;

  private static final boolean serializeNamespaces = false;

  private static Logger log = LoggerFactory.getLogger(RecoverDataController.class);

  public RecoverDataController(
      UUIDBase uuid,
      DHTMetaReader dhtMetaReader,
      ConvergencePoint targetCP,
      ExclusionSet exclusionSet,
      MessageGroupBase mgBase,
      Set<Long> ignoredNamespaces)
      throws KeeperException, IOException {
    super(uuid, dhtMetaReader, targetCP, exclusionSet, mgBase, ignoredNamespaces);
    targetMap = getResolvedReplicaMap(targetRing, targetRingConfig);
    targetReplicas = targetMap.allReplicas();
  }

  //////////////////////////////////////////////////////////////////

  private void recoverRegion(
      long ns, RingEntry targetEntry, Action nsSync, List<ReplicaSyncRequest> srList)
      throws ConvergenceException {
    log.info("recoverRegion {} {}", ns, targetEntry);

    log.info("{} target {}", ns, targetEntry.getRegion());
    for (IPAndPort newOwner : targetEntry.getOwnersIPList(OwnerQueryMode.Primary)) {
      Action prev;

      prev = nsSync;
      for (IPAndPort source : targetReplicas) {
        prev =
            syncReplica(
                ns,
                targetEntry.getRegion(),
                newOwner.port(dhtConfig.getPort()),
                source.port(dhtConfig.getPort()),
                prev,
                srList);
      }
    }
    log.info("Done recoverRegion {} {}", ns, targetEntry);
  }

  private Action recoverNamespace(long ns, Action upstreamDependency) throws ConvergenceException {
    Set<RingEntry> entries;
    SynchronizationPoint syncPoint;
    List<ReplicaSyncRequest> srList;

    log.info("Recovering {}", ns);
    srList = new ArrayList<>();
    entries = targetReplicaMap.getEntries();
    for (RingEntry entry : entries) {
      recoverRegion(ns, entry, upstreamDependency, srList);
    }

    syncPoint = SynchronizationPoint.of(Long.toHexString(ns), srList.toArray(new Action[0]));
    // Note: downstream computed from upstream later
    if (serializeNamespaces) {
      syncController.addAction(syncPoint);
    } else {
      syncController.addCompleteAction(syncPoint);
    }

    log.info("Done synchronizing {}", ns);
    return syncPoint;
  }

  public void recoverAll(Set<Long> namespaces) throws ConvergenceException {
    Action prevDependency;

    log.info("Recovering namespaces");
    prevDependency = null;
    for (long ns : namespaces) {
      prevDependency = recoverNamespace(ns, prevDependency);
    }
    log.info("Done recovering namespaces");

    syncController.freeze();

    log.info(" *** Sending requests");
    syncController.waitForCompletion(
        1, TimeUnit.DAYS); // FUTURE - improve this from a failsafe to a real limit
    log.info(" *** Requests complete");
  }

  public void recover() throws ConvergenceException {
    boolean succeeded;

    succeeded = false;
    try {
      Set<Long> namespaces;

      log.info("Starting recovery {}", targetRing.getRingIDAndVersionPair());
      namespaces = getAllNamespaces();
      recoverAll(namespaces);
      log.info("Recovery complete {}", targetRing.getRingIDAndVersionPair());
      succeeded = true;
    } catch (ConvergenceException ce) {
      log.warn("Recovery failed {}", targetRing.getRingIDAndVersionPair(), ce);
      throw ce;
    } finally {
      setComplete(succeeded);
    }
  }

  ///////////////////////////////////////////////////

  @Override
  public RequestStatus getStatus(UUIDBase uuid) {
    ensureUUIDMatches(uuid);
    if (syncController != null) {
      return new SimpleRequestStatus(
          getRequestState(), "Recovery:" + syncController.getStatus().toString());
    } else {
      return new SimpleRequestStatus(getRequestState(), "<init>");
    }
  }
}
