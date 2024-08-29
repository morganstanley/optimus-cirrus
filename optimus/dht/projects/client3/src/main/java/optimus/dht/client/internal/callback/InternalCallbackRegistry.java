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
package optimus.dht.client.internal.callback;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Striped;

import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.qualifiers.Scheduled;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;

@Singleton
public class InternalCallbackRegistry implements CallbackRegistry {

  private static final Logger logger = LoggerFactory.getLogger(InternalCallbackRegistry.class);

  private static class MapKey {
    private final ServerConnection server;
    private final Object key;
    private final int hashCode;

    public MapKey(ServerConnection server, Object key) {
      this.server = server;
      this.key = key;
      this.hashCode = (server != null ? 29 * server.hashCode() : 7) + key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof MapKey) {
        MapKey other = (MapKey) obj;
        return Objects.equals(server, other.server) && Objects.equals(key, other.key);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static class CallbackContainer {
    private final Callback callback;
    private ScheduledFuture<?> timeoutFuture;
    private boolean completed = false;

    CallbackContainer(Callback callback, ScheduledFuture<?> timeoutFuture) {
      this.callback = callback;
      this.timeoutFuture = timeoutFuture;
    }

    CallbackContainer(Callback callback) {
      this(callback, null);
    }
  }

  class InternalCallbackContext implements CallbackContext {
    private final MapKey mapKey;
    private final CallbackContainer callbackContainer;
    private final CallbackReason reason;
    @Nullable private final Object param;
    @Nullable private final ReceivedMessageMetrics receivedMessageMetrics;

    InternalCallbackContext(
        MapKey mapKey,
        CallbackContainer callbackContainer,
        CallbackReason reason,
        Object param,
        ReceivedMessageMetrics receivedMessageMetrics) {
      this.mapKey = mapKey;
      this.callbackContainer = callbackContainer;
      this.reason = reason;
      this.param = param;
      this.receivedMessageMetrics = receivedMessageMetrics;
    }

    InternalCallbackContext(
        MapKey mapKey,
        CallbackReason reason,
        Object param,
        ReceivedMessageMetrics receivedMessageMetrics) {
      this(mapKey, null, reason, param, receivedMessageMetrics);
    }

    @Override
    @Nullable
    public ServerConnection server() {
      return mapKey.server;
    }

    @Override
    public CallbackReason reason() {
      return reason;
    }

    @Override
    public Object key() {
      return mapKey.key;
    }

    @Override
    public Object param() {
      return param;
    }

    @Override
    public ReceivedMessageMetrics receivedMessageMetrics() {
      return receivedMessageMetrics;
    }

    @Override
    public void rescheduleTimeout(Duration timeout) {
      if (callbackContainer == null) {
        throw new UnsupportedOperationException(
            "Timeouts may only be rescheduled from inside UPDATE callbacks");
      }
      if (callbackContainer.timeoutFuture != null) {
        callbackContainer.timeoutFuture.cancel(true);
      }
      callbackContainer.timeoutFuture =
          executorService.schedule(
              () -> handleTimeout(mapKey, timeout, callbackContainer.callback),
              timeout.toMillis(),
              TimeUnit.MILLISECONDS);
    }

    @Override
    public void completed() {
      if (callbackContainer == null) {
        throw new UnsupportedOperationException(
            "Completed may only be called from inside UPDATE callbacks");
      }
      if (callbackContainer.completed) {
        throw new IllegalStateException("Cannot complete already completed callback");
      }
      callbackContainer.completed = true;
      if (callbackContainer.timeoutFuture != null) {
        callbackContainer.timeoutFuture.cancel(true);
        callbackContainer.timeoutFuture = null;
      }
      callbacks.remove(mapKey, callbackContainer);
    }
  }

  private final ScheduledExecutorService executorService;
  private final ConcurrentMap<MapKey, CallbackContainer> callbacks = new ConcurrentHashMap<>();
  private final Striped<Lock> stripedLock =
      Striped.lock(2 * Runtime.getRuntime().availableProcessors());

  @Inject
  public InternalCallbackRegistry(@Scheduled ScheduledExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public void registerCallback(@Nullable ServerConnection server, Object key, Callback callback) {
    // node may be null if e.g. implementing operation-level callbacks
    Lock lock = (server != null) ? stripedLock.get(server) : null;
    MapKey mapKey = new MapKey(server, key);
    boolean connectionLost = false;
    try {
      if (lock != null) {
        lock.lock();
      }
      if (server != null && !server.state().connected()) {
        connectionLost = true;
        return;
      }
      CallbackContainer existing = callbacks.putIfAbsent(mapKey, new CallbackContainer(callback));
      if (existing != null) {
        throw new IllegalArgumentException("Callback already registered under the specified key");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
      if (connectionLost) {
        // call callback after releasing the lock
        callback.act(
            new InternalCallbackContext(mapKey, CallbackReason.CONNECTION_LOST, null, null));
      }
    }
  }

  @Override
  public void registerCallbackWithTimeout(
      @Nullable ServerConnection server, Object key, Duration timeout, Callback callback) {
    // node may be null if e.g. implementing operation-level callbacks
    Lock lock = (server != null) ? stripedLock.get(server) : null;
    MapKey mapKey = new MapKey(server, key);
    boolean connectionLost = false;
    try {
      if (lock != null) {
        lock.lock();
      }
      if (server != null && !server.state().connected()) {
        connectionLost = true;
        return;
      }
      ScheduledFuture<?> future =
          executorService.schedule(
              () -> handleTimeout(mapKey, timeout, callback),
              timeout.toMillis(),
              TimeUnit.MILLISECONDS);
      CallbackContainer existing =
          callbacks.putIfAbsent(mapKey, new CallbackContainer(callback, future));
      if (existing != null) {
        future.cancel(true);
        throw new IllegalArgumentException("Callback already registered under the specified key");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
      if (connectionLost) {
        // call callback after releasing the lock
        callback.act(
            new InternalCallbackContext(mapKey, CallbackReason.CONNECTION_LOST, null, null));
      }
    }
  }

  private void handleTimeout(MapKey mapkey, Duration timeout, Callback callback) {
    synchronized (callback) {
      if (Thread.interrupted()) {
        // We can get interrupted if callback got already completed, or more importantly, if timeout
        // was rescheduled
        return;
      }
      CallbackContainer callbackContainer = callbacks.remove(mapkey);
      if (callbackContainer != null && !callbackContainer.completed) {
        callbackContainer.completed = true;
        callbackContainer.timeoutFuture = null;
        try {
          callbackContainer.callback.act(
              new InternalCallbackContext(mapkey, CallbackReason.TIMEOUT, timeout, null));
        } catch (Exception e) {
          logger.warn("Exception caught while calling callback for timeout", e);
        }
      }
    }
  }

  @Override
  public boolean update(
      @Nullable ServerConnection server,
      Object key,
      @Nullable Object param,
      @Nullable ReceivedMessageMetrics receivedMessageMetrics) {
    MapKey mapKey = new MapKey(server, key);
    CallbackContainer callbackContainer = callbacks.get(mapKey);
    if (callbackContainer != null) {
      synchronized (callbackContainer.callback) {
        if (!callbackContainer.completed) {
          try {
            callbackContainer.callback.act(
                new InternalCallbackContext(
                    mapKey,
                    callbackContainer,
                    CallbackReason.UPDATE,
                    param,
                    receivedMessageMetrics));
          } catch (Exception e) {
            logger.warn("Exception caught while calling callback for completed", e);
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean complete(
      @Nullable ServerConnection server,
      Object key,
      @Nullable Object param,
      @Nullable ReceivedMessageMetrics receivedMessageMetrics) {
    MapKey mapKey = new MapKey(server, key);
    CallbackContainer callbackContainer = callbacks.remove(mapKey);
    if (callbackContainer != null) {
      synchronized (callbackContainer.callback) {
        if (callbackContainer.timeoutFuture != null) {
          callbackContainer.timeoutFuture.cancel(true);
          callbackContainer.timeoutFuture = null;
        }
        if (!callbackContainer.completed) {
          callbackContainer.completed = true;
          try {
            callbackContainer.callback.act(
                new InternalCallbackContext(
                    mapKey, CallbackReason.COMPLETED, param, receivedMessageMetrics));
          } catch (Exception e) {
            logger.warn("Exception caught while calling callback for completed", e);
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void serverDisconnected(ServerConnection server, boolean wasEstablished, Throwable cause) {
    Map<MapKey, CallbackContainer> callbacksToRun = new HashMap<>();
    Lock lock = stripedLock.get(server);
    try {
      lock.lock();
      callbacks
          .entrySet()
          .removeIf(
              entry -> {
                if (Objects.equals(entry.getKey().server, server)) {
                  callbacksToRun.put(entry.getKey(), entry.getValue());
                  return true;
                } else {
                  return false;
                }
              });
    } finally {
      lock.unlock();
    }
    // we use stripedLock to prevent races between registerCallback* and serverDisconnected acting
    // for the same server
    // but we can't call callbacks here while holding stripedLock, as callback might want to
    // register new callbacks,
    // leading to deadlocks
    callbacksToRun.forEach(
        (key, callbackContainer) -> {
          synchronized (callbackContainer.callback) {
            if (callbackContainer.timeoutFuture != null) {
              callbackContainer.timeoutFuture.cancel(true);
              callbackContainer.timeoutFuture = null;
            }
            if (!callbackContainer.completed) {
              callbackContainer.completed = true;
              try {
                callbackContainer.callback.act(
                    new InternalCallbackContext(
                        key,
                        wasEstablished
                            ? CallbackReason.CONNECTION_LOST
                            : CallbackReason.CONNECTION_FAILED,
                        cause,
                        null));
              } catch (Exception e) {
                logger.warn("Exception caught while calling callback for completed", e);
              }
            }
          }
        });
  }

  @VisibleForTesting
  public boolean hasCallbacks() {
    return !callbacks.isEmpty();
  }
}
