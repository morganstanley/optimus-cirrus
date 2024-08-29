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
package optimus.dht.client.internal.registry;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import optimus.dht.common.util.SequentialDelegatingScheduledExecutorService;
import optimus.dht.client.api.exceptions.DHTRegistryException;
import optimus.dht.client.api.registry.RegistryObserver;
import optimus.dht.client.api.registry.RegistryNodeChangesListener;
import optimus.dht.client.api.registry.ZKRegistryConfig;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerConnectionState;

public class ZookeeperRegistryObserver implements RegistryObserver {

  private static final Logger logger = LoggerFactory.getLogger(ZookeeperRegistryObserver.class);

  public static final String SERVERS_PATH = "servers";

  private static final Splitter SPLITTER = Splitter.on(":");
  private static final Joiner JOINER = Joiner.on(":");

  private final CuratorFramework curator;
  private final List<RegistryNodeChangesListener> listeners = new ArrayList<>();
  private final String zkNodesPath;
  private final Duration zkRetryDelay;
  private final ScheduledExecutorService executor;

  protected final CountDownLatch firstTimeConsistentLatch = new CountDownLatch(1);

  // if we are connected to zk
  protected boolean connected = false;
  // set to true after we lost session, and need to create new watchers
  protected boolean expired = false;
  // means we have obtained servers list, and we either have alive zk connection
  // or we are still within grace period
  // volatile, so other threads can read up-to-date value
  protected volatile boolean consistent = false;
  // if we have ever called onZookeeperUnavailable
  protected boolean wasUnavailable = false;

  protected int negotiatedSessionTimeout;
  protected ScheduledFuture<?> sessionTimerFuture;
  protected Set<ServerConnection.ServerIdentity> lastResult;

  public ZookeeperRegistryObserver(CuratorFramework curator, ZKRegistryConfig config) {
    this.curator = curator;
    this.zkNodesPath = ZKPaths.makePath(config.zkClusterPath(), SERVERS_PATH);
    this.zkRetryDelay = config.zkRetryDelay();
    this.executor = new SequentialDelegatingScheduledExecutorService(config.executor());
  }

  @Override
  public void registerListener(RegistryNodeChangesListener listener) {
    Future<?> future =
        executor.submit(
            () -> {
              listeners.add(listener);
              if (isConsistent()) {
                try {
                  listener.nodesRegistryChanged(lastResult);
                } catch (Exception e) {
                  throw new RuntimeException("Exception while calling listener", e);
                }
              }
            });
    try {
      future.get();
    } catch (Exception e) {
      throw new RuntimeException("Exception while registering listener", e);
    }
  }

  public boolean isConsistent() {
    return consistent;
  }

  private void tryToGetZkConnection() {
    try {
      curator.getZookeeperClient().getZooKeeper();
    } catch (Exception e) {
      logger.warn("Exception while trying to get zookeeper instance, will retry in 5 seconds", e);
      executor.schedule(this::tryToGetZkConnection, 5, TimeUnit.SECONDS);
    }
  }

  public void start() {
    curator.getConnectionStateListenable().addListener(this::curatorConnectionStateChanged);
    if (curator.getState() == CuratorFrameworkState.LATENT) {
      // this will block until initial connection establishment succeeds or fails
      curator.start();
      if (!curator.getZookeeperClient().isConnected()) {
        // if it failed, we need to keep asking curator for new connections, otherwise it will stop
        // retrying
        tryToGetZkConnection();
      }
    } else if (curator.getState() == CuratorFrameworkState.STARTED) {
      if (curator.getZookeeperClient().isConnected() && !connected) {
        // handle the case when we are reusing curator instance, and curator is already running
        curatorConnectionStateChanged(curator, ConnectionState.CONNECTED);
      }
    }
  }

  protected List<String> query(
      String clusterPath, CuratorWatcher watcher, BackgroundCallback callback) throws Exception {
    GetChildrenBuilder childrenBuilder = curator.getChildren();
    if (watcher != null) {
      childrenBuilder.usingWatcher(watcher);
    }
    if (callback != null) {
      childrenBuilder.inBackground(callback, executor);
    }
    return childrenBuilder.forPath(clusterPath);
  }

  protected ServerConnection buildNode(String child) {
    try {
      List<String> split = SPLITTER.splitToList(child);

      if (split.size() < 3) {
        logger.warn("Ignoring unrecognized zk node, name={}", child);
        return null;
      }

      String host = split.get(0);
      int port = Integer.valueOf(split.get(1));
      String uniqueId = split.get(2);
      String cloudName = null;
      if (split.size() > 3) {
        cloudName = JOINER.join(split.subList(3, split.size()));
      }

      return new ServerConnection(
          host,
          port,
          InetSocketAddress.createUnresolved(host, port),
          uniqueId,
          cloudName,
          0,
          ServerConnectionState.NEW);
    } catch (Exception e) {
      logger.warn("Exception while reading zknode=" + child, e);
      return null;
    }
  }

  protected void scheduleRetry() {
    lastResult = null;
    executor.schedule(
        () -> {
          queryChildren();
          return null;
        },
        zkRetryDelay.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  protected void callListenersSafely(Consumer<RegistryNodeChangesListener> consumer) {
    for (RegistryNodeChangesListener listener : listeners) {
      try {
        consumer.accept(listener);
      } catch (Exception e) {
        logger.warn("Exception caught while calling listener", e);
      }
    }
  }

  protected void callOnUnavailable(Exception e) {
    wasUnavailable = true;
    callListenersSafely(listener -> listener.nodesRegistryUnavailable(e));
  }

  protected void queryChildren() {
    try {
      logger.info("Asking Curator for a fresh list of servers");
      query(zkNodesPath, this::childrenWatcherFired, this::childrenFetched);
    } catch (Exception e) {

      if (consistent) {
        consistent = false;
        logger.warn("Zookeeper query failed - will retry - future DHT requests will be failed", e);
        callOnUnavailable(e);
      } else {
        logger.info(
            "Zookeeper query failed - will retry - DHT requests are already being failed", e);
      }

      scheduleRetry();
    }
  }

  protected void childrenFetched(CuratorFramework client, CuratorEvent event) {
    executor.submit(() -> processChildrenFetched(client, event));
  }

  protected void childrenWatcherFired(WatchedEvent e) {
    executor.submit(() -> processChildrenWatcherFired(e));
  }

  protected void curatorConnectionStateChanged(CuratorFramework client, ConnectionState newState) {
    executor.submit(() -> processCuratorConnectionStateChanged(client, newState));
  }

  protected void processCuratorConnectionStateChanged(
      CuratorFramework client, ConnectionState newState) {
    logger.info("Received new Curator state notification, newState={}", newState);
    switch (newState) {
      case CONNECTED:
        try {
          if (connected) {
            logger.warn("Curator state changed to CONNECTED, but we are already connected.");
          }
          connected = true;
          negotiatedSessionTimeout =
              curator.getZookeeperClient().getZooKeeper().getSessionTimeout();
          queryChildren();
        } catch (Exception e) {
          logger.error("Exception caught while handling curator state change", e);
        }
        break;
      case RECONNECTED:
        try {
          if (connected) {
            logger.warn("Curator state changed to RECONNECTED, but we are already connected.");
          }
          connected = true;
          negotiatedSessionTimeout =
              curator.getZookeeperClient().getZooKeeper().getSessionTimeout();
          boolean timerCancelled = clearSessionTimer();
          if (expired) {
            logger.warn("Previous Zookeeper session has expired, starting a new query");
            expired = false;
            queryChildren();
          } else {
            if (timerCancelled) {
              logger.info("Zookeeper session was reestablished, session timeout timer cancelled");
            } else {
              if (!consistent) {
                if (lastResult != null) {
                  consistent = true;
                  logger.info(
                      "Session believed to be expired is still valid - no longer failing DHT requests");
                  callListenersSafely(listener -> listener.nodesRegistryAvailable());
                } else {
                  logger.info(
                      "Reconnected to zookeeper, but the list of servers is not yet known, starting a new query");
                  queryChildren();
                }
              } else {
                logger.warn(
                    "Unexpected state, session timeout couldn't be cancelled, but state is still marked as consistent");
              }
            }
          }
        } catch (Exception e) {
          logger.error("Exception caught while handling curator state change", e);
        }
      default:
        // we get other events via childrenWatcherFired
    }
  }

  protected void processChildrenWatcherFired(WatchedEvent e) {
    if (e.getType() == EventType.NodeChildrenChanged) {
      logger.info("Node's children has changed - will query");
      queryChildren();
    } else if (e.getType() == EventType.NodeDeleted) {
      consistent = false;
      logger.warn(
          "Root node was deleted - will keep retrying - future DHT requests will be failed");
      scheduleRetry();
      callOnUnavailable(null);
    } else if (e.getState() == KeeperState.Disconnected) {
      logger.info("Disconnected from zookeeper, starting session timeout timer");
      connected = false;
      sessionTimerFuture =
          executor.schedule(
              this::processSessionTimerExpired, negotiatedSessionTimeout, TimeUnit.MILLISECONDS);
    } else if (e.getState() == KeeperState.Expired) {
      connected = false;
      expired = true;
      // Expired is reported by server - SyncConnected event is very likely to
      // be fired soon,
      // so we don't fail DHT requests yet
      logger.warn("Zookeeper reported session as expired, not failing DHT requests yet");
    } else if (e.getState() == KeeperState.SyncConnected) {
      // Handled in curatorConnectionStateChanged(..)
    } else {
      logger.warn("Unexpected and unhandled zk event: {}", e);
    }
  }

  protected void processSessionTimerExpired() {
    if (!connected) {
      consistent = false;
      logger.warn("Internal timer caused session expiration - future DHT requests will be failed");
      callOnUnavailable(null);
    }
    sessionTimerFuture = null;
  }

  protected boolean clearSessionTimer() {
    if (sessionTimerFuture != null) {
      boolean canceled = sessionTimerFuture.cancel(true);
      sessionTimerFuture = null;
      return canceled;
    }
    return false;
  }

  protected void processChildrenFetched(CuratorFramework client, CuratorEvent event) {
    logger.info("Children fetched callback: event={}", event);

    if (event.getType() == CuratorEventType.CHILDREN) {
      int resultCode = event.getResultCode();
      if (resultCode == Code.OK.intValue()) {
        processChildrenList(event.getChildren());
      } else {
        KeeperException failure = KeeperException.create(Code.get(resultCode), event.getPath());
        logger.warn(
            "Background operation failed, will retry - {} - future DHT requests will be failed",
            failure.getMessage());
        if (consistent) {
          consistent = false;
          callOnUnavailable(null);
        }
        scheduleRetry();
      }
    }
  }

  protected void processChildrenList(List<String> children) {

    Set<ServerConnection.ServerIdentity> nodes =
        children.stream()
            .map(ZookeeperRegistryObserver.this::buildNode)
            .filter(Objects::nonNull)
            .map(ServerConnection::serverIdentity)
            .collect(Collectors.toSet());

    logger.info(
        "Obtained {} servers from zookeeper registry from {} zk nodes",
        nodes.size(),
        children.size());

    callListenersSafely(listener -> listener.nodesRegistryChanged(nodes));

    // do not fire notification on the first call
    if (!consistent && wasUnavailable) {
      logger.info("Zookeeper registry has recovered - no longer failing DHT requests");
      callListenersSafely(listener -> listener.nodesRegistryAvailable());
    }

    lastResult = nodes;
    consistent = true;
    firstTimeConsistentLatch.countDown();
  }

  public void waitUntilConsistentForFirstTime(Duration timeout) throws InterruptedException {
    if (!firstTimeConsistentLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      throw new DHTRegistryException(
          "Couldn't establish consistent zookeeper view within timeout ("
              + timeout.toMillis()
              + "ms)");
    }
  }
}
