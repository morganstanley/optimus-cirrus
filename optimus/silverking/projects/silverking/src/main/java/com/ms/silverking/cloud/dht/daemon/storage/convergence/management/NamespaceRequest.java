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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceRequestMessageGroup;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks outstanding requests for namespaces from peers.
 */
class NamespaceRequest {
  private final MessageGroupBase mgBase;
  private final Set<IPAndPort> incompletePeers;
  private final Map<UUIDBase, NamespaceRequest> nsRequests;
  private final Set<Long> namespaces;

  private static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(NamespaceRequest.class);

  NamespaceRequest(MessageGroupBase mgBase, Set<IPAndPort> peers, Map<UUIDBase, NamespaceRequest> nsRequests) {
    this.mgBase = mgBase;
    this.incompletePeers = new HashSet<>(peers);
    this.nsRequests = nsRequests;
    this.namespaces = new ConcurrentSkipListSet<>();
  }

  public boolean waitForCompletion(int waitLimitMillis) {
    synchronized (incompletePeers) {
      Timer timer;

      timer = new SimpleTimer(TimeUnit.MILLISECONDS, waitLimitMillis);
      while (incompletePeers.size() > 0 && !timer.hasExpired()) {
        try {
          incompletePeers.wait(timer.getRemainingMillis());
        } catch (InterruptedException ie) {
        }
      }
      if (incompletePeers.size() > 0) {
        log.info("Unable to receive namespaces from: {}", CollectionUtil.toString(incompletePeers));
      }
      return incompletePeers.size() == 0;
    }
  }

  public Set<Long> getNamespaces() {
    return namespaces;
  }

  public void peerComplete(IPAndPort peer, Set<Long> peerNamespaces) {
    if (debug) {
      log.info("peerComplete: {}", peer);
    }
    removePeer(peer);
    namespaces.addAll(peerNamespaces);
  }

  private void removePeer(IPAndPort peer) {
    synchronized (incompletePeers) {
      incompletePeers.remove(peer);
      if (incompletePeers.size() == 0) {
        incompletePeers.notifyAll();
      }
    }
  }

  public void requestNamespacesFromPeers() {
    Set<IPAndPort> _incompletePeers;

    synchronized (incompletePeers) {
      _incompletePeers = ImmutableSet.copyOf(incompletePeers);
    }
    for (IPAndPort peer : _incompletePeers) {
      sendNamespaceRequest(peer);
    }
  }

  private void sendNamespaceRequest(IPAndPort dest) {
    ProtoNamespaceRequestMessageGroup protoMG;

    if (debug) {
      log.debug("Requesting namespaces from: {}", dest);
    }
    protoMG = new ProtoNamespaceRequestMessageGroup(new UUIDBase(), mgBase.getMyID());
    nsRequests.put(protoMG.getUUID(), this);
    mgBase.send(protoMG.toMessageGroup(), dest);
  }
}