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
package optimus.dht.client.internal.servers;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import optimus.dht.client.api.exceptions.DHTOrphanedConnectionTimeoutException;
import optimus.dht.client.api.servers.ServerRingPlacement;
import optimus.dht.client.internal.callback.InternalCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.registry.RegistryNodeChangesListener;
import optimus.dht.client.api.registry.RegistryObserver;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerConnectionState;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.servers.ServerConnection.ServerIdentity;
import optimus.dht.client.api.qualifiers.Scheduled;
import optimus.dht.client.api.transport.ConnectionsManager;
import optimus.dht.client.api.transport.ServerConnectionStateListener;

@Singleton
public class DefaultServerConnectionsManager
    implements ServerConnectionsManager,
        ServerConnectionStateListener,
        RegistryNodeChangesListener {

  private static final Logger logger =
      LoggerFactory.getLogger(DefaultServerConnectionsManager.class);

  private final ConcurrentMap<ServerIdentity, ServerIdentity> currentServers =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<ServerConnection, ConnectRetry> reconnectingServers =
      new ConcurrentHashMap<>();
  private final ServerConnectionsManagerConfig config;
  private final ConsistentHashRing ring;
  private final ConnectionsManager connectionsManager;
  private final InternalCallbackRegistry callbackRegistry;
  private final ScheduledExecutorService executorService;

  @Inject
  public DefaultServerConnectionsManager(
      RegistryObserver registryObserver,
      ConnectionsManager.Factory connectionsManagerFactory,
      ServerConnectionsManagerConfig config,
      HashCalculator hashCalculator,
      ServerRingPlacement serverRingPlacement,
      InternalCallbackRegistry callbackRegistry,
      @Scheduled ScheduledExecutorService executorService) {
    this.config = config;
    this.ring =
        new ConsistentHashRing(config.slotsPerServer(), hashCalculator, serverRingPlacement);
    this.connectionsManager = connectionsManagerFactory.create(this);
    this.callbackRegistry = callbackRegistry;
    this.executorService = executorService;
    registryObserver.registerListener(this);
  }

  @Override
  public ServersState currentState() {
    if (currentServers.isEmpty()) {
      return ServersState.NO_SERVERS;
    }
    if (hasActiveServers()) {
      return ServersState.MAYBE_REACHABLE;
    }
    return ServersState.ALL_UNREACHABLE;
  }

  private boolean hasActiveServers() {
    return currentServers.values().stream()
        .map(ServerConnection.ServerIdentity::serverConnection)
        .anyMatch(DefaultServerConnectionsManager::isActive);
  }

  private static boolean isActive(ServerConnection serverConnection) {
    return serverConnection.state().active();
  }

  public ConnectionsManager connectionsManager() {
    return connectionsManager;
  }

  @Override
  public List<ServerConnection> getAllServers() {
    return currentServers.values().stream()
        .map(ServerConnection.ServerIdentity::serverConnection)
        .collect(Collectors.toList());
  }

  @Override
  public List<ServerConnection> getActiveServers() {
    return currentServers.values().stream()
        .map(ServerConnection.ServerIdentity::serverConnection)
        .filter(DefaultServerConnectionsManager::isActive)
        .collect(Collectors.toList());
  }

  private Iterator<ServerConnection> getActiveServersForKeyIterator(Key key) {
    return ring.serversForKey(key, DefaultServerConnectionsManager::isActive);
  }

  @Override
  public ServerConnection getPrimaryServerForKey(Key key) {
    Iterator<ServerConnection> iterator = getActiveServersForKeyIterator(key);
    return iterator.hasNext() ? iterator.next() : null;
  }

  @Override
  public Iterator<ServerConnection> getServersForKey(Key key) {
    if (key.hash() == null) {
      throw new NullPointerException("Key's hash must be calculated before calling this method");
    }
    return getActiveServersForKeyIterator(key);
  }

  @Override
  public void serverConnecting(ServerConnection serverConnection) {
    for (; ; ) {
      ServerConnectionState currentState = serverConnection.state();
      switch (currentState) {
        case NEW:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.NEW, ServerConnectionState.CONNECTING)) {
            logger.info("Connecting to server={}", serverConnection);
            return;
          }
          break;
        case DISCONNECTED:
          // We are attempting to reconnect - nothing to do at this point
          return;
        default:
          logger.warn(
              "Received serverConnecting on unexpected state={}, server={}",
              currentState,
              serverConnection);
          return;
      }
    }
  }

  @Override
  public void serverConnected(ServerConnection serverConnection) {
    for (; ; ) {
      ServerConnectionState currentState = serverConnection.state();
      switch (currentState) {
        case CONNECTING:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.CONNECTING, ServerConnectionState.CONNECTED)) {
            logger.info("Connected to server={}", serverConnection);
            return;
          }
          break;
        case DISCONNECTED:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.DISCONNECTED, ServerConnectionState.CONNECTED)) {

            var connectRetry = reconnectingServers.remove(serverConnection);
            if (connectRetry == null) {
              logger.warn(
                  "Unexpected serverConnected call on disconnected server={}, without retry state. Closing connection.",
                  serverConnection);
              connectionsManager.closeConnection(serverConnection);
              return;
            }

            if (connectRetry.lostConnection.tryUpdateState(
                ServerConnectionState.DISCONNECTED, ServerConnectionState.REINCARNATED)) {
              logger.info("Reconnected to server={}", serverConnection);
              ring.addServer(serverConnection);
            } else {
              logger.info("Reconnected to server={}, but it was removed. Closing connection.");
              connectionsManager.closeConnection(serverConnection);
            }

            return;
          }
          break;
        case ORPHANED:
          scheduleOrphanedServerTimeout(serverConnection);
          logger.info("Connected to an orphaned server={}", serverConnection);
          return;
        default:
          logger.warn(
              "Received serverConnected on unexpected state={}, server={}",
              currentState,
              serverConnection);
          return;
      }
    }
  }

  @Override
  public void serverDisconnectedOrFailed(
      ServerConnection serverConnection,
      FailureReason reason,
      boolean wasEstablished,
      @Nullable Throwable cause) {
    for (; ; ) {
      ServerConnectionState currentState = serverConnection.state();
      switch (currentState) {
        case ORPHANED:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.ORPHANED, ServerConnectionState.REMOVED)) {
            logger.info(
                "Disconnected from an orphaned server={}, cause={}",
                serverConnection,
                String.valueOf(cause));
            currentServers.remove(serverConnection.serverIdentity());
            callCallbackRegistryOnDisconnect(serverConnection, wasEstablished, cause);
            return;
          }
        case CONNECTING:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.CONNECTING, ServerConnectionState.DISCONNECTED)) {
            logger.warn(
                "Unable to connect to server={}, cause={}",
                serverConnection,
                String.valueOf(cause));
            ring.removeServer(serverConnection);
            callCallbackRegistryOnDisconnect(serverConnection, wasEstablished, cause);
            initiateReconnect(serverConnection, false);
            return;
          }
          break;
        case CONNECTED:
          if (serverConnection.tryUpdateState(
              ServerConnectionState.CONNECTED, ServerConnectionState.DISCONNECTED)) {
            logger.info(
                "Disconnected from server={}, cause={}", serverConnection, String.valueOf(cause));
            ring.removeServer(serverConnection);
            callCallbackRegistryOnDisconnect(serverConnection, wasEstablished, cause);
            initiateReconnect(serverConnection, true);
            return;
          }
          break;
        case DISCONNECTED:
          ConnectRetry connectRetry = reconnectingServers.get(serverConnection);
          if (connectRetry == null) {
            logger.warn(
                "Unexpected serverDisconnectedOrFailed call on disconnected server={}, without retry state. Ignoring.",
                serverConnection);
            return;
          }
          if (connectRetry.lostConnection.state() == ServerConnectionState.REMOVED) {
            logger.info(
                "Reconnect attempt to server={} failed, cause={}, and no longer retrying as server was removed",
                serverConnection,
                String.valueOf(cause));
            return;
          }
          long nextRetryInMs = connectRetry.scheduleNextRetry();
          logger.warn(
              "Reconnect attempt to server={} failed, cause={}, next attempt in {}ms",
              serverConnection,
              String.valueOf(cause),
              nextRetryInMs);
          return;
        case REMOVED:
          // Only called after disconnect was initiated by us (e.g. ORPHANED timed out), in which
          // case it was already handled
          return;
        default:
          logger.warn(
              "Received serverDisconnected on unexpected state={}, server={}",
              currentState,
              serverConnection,
              cause);
          return;
      }
    }
  }

  private void initiateReconnect(ServerConnection serverConnection, boolean immediate) {
    for (; ; ) {
      ServerIdentity currentIdentity = currentServers.get(serverConnection.serverIdentity());
      ServerConnectionState state =
          currentIdentity != null ? currentIdentity.serverConnection().state() : null;
      if (state == null || state != ServerConnectionState.DISCONNECTED) {
        logger.warn(
            "Unexpected state={}, while initiating reconnection to server={}",
            state,
            serverConnection);
        return;
      }
      ServerConnection incrementedServerConnection =
          currentIdentity.serverConnection().withIncrementedReconnectCount();
      if (currentServers.replace(
          currentIdentity, currentIdentity, incrementedServerConnection.serverIdentity())) {
        ConnectRetry connectRetry = new ConnectRetry(serverConnection, incrementedServerConnection);
        reconnectingServers.put(incrementedServerConnection, connectRetry);
        if (immediate) {
          logger.info("Trying to reestablish connection to server={}", serverConnection);
          connectionsManager.establishConnection(incrementedServerConnection);
        } else {
          long nextRetryInMs = connectRetry.scheduleNextRetry();
          logger.info(
              "Connect attempt to server={} failed, next attempt in {}ms",
              serverConnection,
              nextRetryInMs);
        }
        return;
      }
    }
  }

  private void callCallbackRegistryOnDisconnect(
      ServerConnection serverConnection, boolean wasEstablished, Throwable cause) {
    try {
      callbackRegistry.serverDisconnected(serverConnection, wasEstablished, cause);
    } catch (Exception e) {
      logger.error("Exception caught while calling callback registry", e);
    }
  }

  @Override
  public void nodesRegistryChanged(Set<ServerConnection.ServerIdentity> newNodes) {

    Set<ServerConnection.ServerIdentity> knownNodes = new HashSet<>(currentServers.values());
    Set<ServerConnection.ServerIdentity> removedNodes = Sets.difference(knownNodes, newNodes);

    logger.info(
        "Notified about newServers={}, currentServers={}, addedServers={}, removedServers={}",
        newNodes.size(),
        currentServers.size(),
        Sets.difference(newNodes, knownNodes).size(),
        removedNodes.size());

    for (ServerConnection.ServerIdentity node : newNodes) {
      ServerIdentity knownNode = currentServers.get(node);

      if (knownNode != null) {
        // existing node - need to check state
        for (; ; ) {
          ServerConnectionState state = knownNode.serverConnection().state();
          if (state == ServerConnectionState.ORPHANED) {
            if (knownNode
                .serverConnection()
                .tryUpdateState(ServerConnectionState.ORPHANED, ServerConnectionState.CONNECTED)) {
              logger.info("No longer orphaned, server={}", knownNode.serverConnection());
              break;
            }
          } else {
            break;
          }
        }
      } else {
        ServerConnection serverConnection = node.serverConnection();
        if (config.eagerDnsResolution()) {
          InetSocketAddress maybeResolved =
              new InetSocketAddress(serverConnection.host(), serverConnection.port());
          if (maybeResolved.isUnresolved()) {
            serverConnection = serverConnection.asUnresolved();
            DnsRetry dnsRetry = new DnsRetry(serverConnection.serverIdentity());
            long nextRetryInMs = dnsRetry.scheduleNextRetry();
            logger.warn(
                "Unable to resolve hostname of server={}, will retry in {}ms",
                serverConnection.serverIdentity(),
                nextRetryInMs);
          } else {
            serverConnection = serverConnection.withResolvedServerAddress(maybeResolved);
          }
        }
        ServerIdentity effectiveServer = serverConnection.serverIdentity();
        logger.info("New server={}, state={}", effectiveServer, serverConnection.state());
        if (serverConnection.state().active()) {
          ring.addServer(serverConnection);
        }
        currentServers.put(effectiveServer, effectiveServer);
      }
    }

    removedNodes.stream()
        .forEach(
            n -> {
              ServerConnection serverConnection = n.serverConnection();
              for (; ; ) {
                ServerConnectionState state = serverConnection.state();
                switch (state) {
                  case CONNECTING:
                  case CONNECTED:
                    if (serverConnection.tryUpdateState(state, ServerConnectionState.ORPHANED)) {
                      logger.info("Orphaned server={}", serverConnection);
                      ring.removeServer(serverConnection);
                      scheduleOrphanedServerTimeout(serverConnection);
                      return;
                    }
                    break;
                  case ORPHANED:
                    logger.info("Still orphaned, server={}", serverConnection);
                    return;
                  default:
                    if (serverConnection.tryUpdateState(state, ServerConnectionState.REMOVED)) {
                      if (state.active()) {
                        ring.removeServer(serverConnection);
                      }
                      currentServers.remove(n);
                      logger.info("Removed server={}, previousState={}", serverConnection, state);
                      return;
                    }
                }
              }
            });
  }

  @Override
  public void nodesRegistryUnavailable(Exception e) {}

  @Override
  public void nodesRegistryAvailable() {}

  private void scheduleOrphanedServerTimeout(ServerConnection serverConnection) {
    executorService.schedule(
        new OrphanedServerTimeout(serverConnection),
        config.getOrphanedNodeTimeout().getSeconds(),
        TimeUnit.SECONDS);
    logger.info("Scheduled timeout, orphaned server={}", serverConnection);
  }

  private class OrphanedServerTimeout implements Runnable {

    private final ServerConnection serverConnection;

    OrphanedServerTimeout(ServerConnection serverConnection) {
      this.serverConnection = serverConnection;
    }

    @Override
    public void run() {
      // Only remove it, if server is still in ORPHANED state
      if (serverConnection.tryUpdateState(
          ServerConnectionState.ORPHANED, ServerConnectionState.REMOVED)) {
        logger.info("Orphaned timeout reached, disconnecting server={}", serverConnection);
        currentServers.remove(serverConnection.serverIdentity());
        callCallbackRegistryOnDisconnect(
            serverConnection,
            true,
            new DHTOrphanedConnectionTimeoutException(
                serverConnection, config.getOrphanedNodeTimeout()));
        connectionsManager.closeConnection(serverConnection);
      }
    }
  }

  private abstract class ExponentialRetry implements Runnable {

    private long nextRetryInMs = config.initialRetryInterval().toMillis();

    public long scheduleNextRetry() {
      long currentNextRetryInMs = nextRetryInMs;
      executorService.schedule(this, nextRetryInMs, TimeUnit.MILLISECONDS);
      nextRetryInMs =
          Math.min(
              (long) (nextRetryInMs * config.retryIntervalExpRatio()),
              config.maxRetryInterval().toMillis());
      return currentNextRetryInMs;
    }
  }

  private class DnsRetry extends ExponentialRetry {

    private final ServerConnection.ServerIdentity serverIdentity;

    public DnsRetry(ServerConnection.ServerIdentity serverIdentity) {
      this.serverIdentity = serverIdentity;
    }

    @Override
    public void run() {
      for (; ; ) {
        ServerIdentity currentServerIdentity = currentServers.get(this.serverIdentity);
        if (currentServerIdentity == null) {
          logger.info(
              "No longer trying to resolve hostname of server={}, as it was already removed",
              serverIdentity);
          return;
        }

        InetSocketAddress serverAddress =
            new InetSocketAddress(serverIdentity.host(), serverIdentity.port());
        if (serverAddress.isUnresolved()) {
          long nextRetryInMs = scheduleNextRetry();
          logger.warn(
              "Hostname of server={} is still unresolved, will retry in {}ms",
              serverIdentity,
              nextRetryInMs);
          return;
        } else {
          ServerConnection withResolvedServerAddress =
              currentServerIdentity.serverConnection().withResolvedServerAddress(serverAddress);
          // CAS replace to handle race with concurrent removal
          if (currentServers.replace(
              serverIdentity, currentServerIdentity, withResolvedServerAddress.serverIdentity())) {
            logger.info("Resolved hostname of server={}, adding it to ring", serverIdentity);
            ring.addServer(withResolvedServerAddress);
            return;
          }
        }
      }
    }
  }

  private class ConnectRetry extends ExponentialRetry {

    private final ServerConnection lostConnection;
    private final ServerConnection newConnection;

    public ConnectRetry(ServerConnection lostConnection, ServerConnection newConnection) {
      this.lostConnection = lostConnection;
      this.newConnection = newConnection;
    }

    @Override
    public void run() {
      // Can also be REMOVED at this stage
      if (lostConnection.state() == ServerConnectionState.DISCONNECTED) {
        connectionsManager.establishConnection(newConnection);
      } else {
        logger.info("No longer trying to reconnect to server={}", lostConnection);
      }
    }
  }
}
