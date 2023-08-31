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
package com.ms.silverking.net.async;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionManager<T extends Connection> implements ConnectionController {

  private static Logger log = LoggerFactory.getLogger(ConnectionManager.class);

  private static final Set<ConnectionManager> activeManagers =
      Collections.newSetFromMap(new WeakHashMap<ConnectionManager, Boolean>());

  static void addManager(ConnectionManager manager) {
    activeManagers.add(manager);
    log.info("Added ConnectionManager, there are {} active managers", activeManagers.size());
  }

  /**
   * this will check if a connection is local to VM or not this can happen if a client is created
   * within server e.g. we do that in NamespaceMetaStore when this internal client connects to
   * server in the same VM, that connection will be recognized by this call. The way it does that is
   * by going through other ConnectionManager objects created in the same VM and check if thay hold
   * the other end of a given conection,
   *
   * @param serverSideEndpoints
   * @param serverSideManager
   * @return
   */
  private static final boolean isLocalToVM(
      Pair<IPAndPort, IPAndPort> serverSideEndpoints, ConnectionManager serverSideManager) {
    Pair<IPAndPort, IPAndPort> clientSideEndPoints =
        Pair.of(serverSideEndpoints.getV2(), serverSideEndpoints.getV1());
    for (ConnectionManager manager : activeManagers) {
      if (manager != serverSideManager) {
        if (manager.connectionMap.containsKey(clientSideEndPoints)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Map of Connections where Key is a pair of local and remote IPAndPort in that order */
  private final ConcurrentMap<Pair<IPAndPort, IPAndPort>, T> connectionMap =
      new ConcurrentHashMap<>();

  @Override
  public int disconnectAll(String reason) {
    int disconnected = 0;
    int localToVM = 0;
    for (Map.Entry<Pair<IPAndPort, IPAndPort>, T> entry : connectionMap.entrySet()) {
      if (isLocalToVM(entry.getKey(), this)) {
        localToVM = localToVM + 1;
      } else {
        disconnectConnection(entry.getValue(), reason);
        disconnected = disconnected + 1;
      }
    }
    log.info(
        "{} ConnectionManager disconnected {} and found {} localToVM connections",
        reason,
        disconnected,
        localToVM);
    return disconnected;
  }

  void addConnection(T c) {
    IPAndPort local = c.getLocalIPAndPort();
    IPAndPort remote = c.getRemoteIPAndPort();
    Pair<IPAndPort, IPAndPort> endPoints = Pair.of(local, remote);
    connectionMap.put(endPoints, c);
    log.info("ConnectionManager added {}", c);
  }

  Collection<T> getConnections() {
    return connectionMap.values();
  }

  void disconnectConnection(Connection c, String reason) {
    IPAndPort local = c.getLocalIPAndPort();
    IPAndPort remote = c.getRemoteIPAndPort();
    Pair<IPAndPort, IPAndPort> endPoints = Pair.of(local, remote);
    connectionMap.remove(endPoints);
    c.disconnect();
    log.info("ConnectionManager disconnect: {} {}", c, reason);
  }
}
