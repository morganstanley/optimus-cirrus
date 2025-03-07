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
package optimus.dht.client.api.replication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.exceptions.DHTConnectionLostException;
import optimus.dht.client.api.exceptions.DHTOperationTimeoutException;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.transport.MessageSender;
import optimus.dht.client.api.transport.OperationDetails;
import optimus.dht.client.api.transport.RequestIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.client.api.exceptions.DHTConnectionFailedException;
import optimus.dht.client.api.exceptions.DHTException;
import optimus.dht.client.api.exceptions.DHTNoReachableServersException;
import optimus.dht.client.api.exceptions.DHTNoRegisteredServersException;
import optimus.dht.client.api.exceptions.DHTReadTimeoutException;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.SentMessageMetrics;
import optimus.dht.common.util.StringTool;

public abstract class BatchOperationTemplate<K extends Key, E> {

  private static final Logger logger = LoggerFactory.getLogger(BatchOperationTemplate.class);

  private static class BatchItemState {
    private ReplicationStrategy.ReplicatedOperation replicatedOperation;
    private int pendingServers = 0;
    private boolean responseCommitted = false;
    private boolean atLeastOneGoodResponse = false;

    @Override
    public String toString() {
      return "BatchItemState [pendingServers="
          + pendingServers
          + ", responseCommitted="
          + responseCommitted
          + ", atLeastOneGoodResponse="
          + atLeastOneGoodResponse
          + "]";
    }
  }

  protected final Duration readTimeout;
  protected final Duration operationTimeout;
  protected final ReplicationStrategy replicationStrategy;
  protected final ServerConnectionsManager nodesManager;
  protected final RequestIdGenerator idGenerator;
  protected final CallbackRegistry callbackRegistry;
  protected final MessageSender messageSender;
  protected final List<E> allItems;
  protected final Map<K, BatchItemState> batchItemsState;

  private final AtomicInteger remainingItems = new AtomicInteger(0);

  public BatchOperationTemplate(
      Duration readTimeout,
      Duration operationTimeout,
      ReplicationStrategy replicationStrategy,
      ServerConnectionsManager nodesManager,
      RequestIdGenerator idGenerator,
      CallbackRegistry callbackRegistry,
      MessageSender messageSender,
      List<E> allItems) {
    this.readTimeout = readTimeout;
    this.operationTimeout = operationTimeout;
    this.replicationStrategy = replicationStrategy;
    this.nodesManager = nodesManager;
    this.idGenerator = idGenerator;
    this.callbackRegistry = callbackRegistry;
    this.messageSender = messageSender;
    this.allItems = allItems;
    this.batchItemsState =
        new HashMap<>(
            allItems.stream().collect(Collectors.toMap(this::key, e -> new BatchItemState())));
  }

  protected String formatItemsKeys(List<E> items) {
    return items.stream()
        .map(i -> StringTool.formatByteHash(key(i).hash()))
        .collect(Collectors.joining(",", "(", ")"));
  }

  public void run() {
    try {

      switch (nodesManager.currentState()) {
        case NO_SERVERS:
          handleFail(
              new ArrayList<>(allItems),
              new DHTNoRegisteredServersException(keys(allItems)),
              OperationDetails.EMPTY);
          return;
        case ALL_UNREACHABLE:
          handleFail(
              new ArrayList<>(allItems),
              new DHTNoReachableServersException(keys(allItems)),
              OperationDetails.EMPTY);
          return;
        default:
          // probably has reachable servers - can continue
      }

      callbackRegistry.registerCallbackWithTimeout(
          null,
          this,
          operationTimeout,
          ctx -> {
            if (ctx.reason() == CallbackRegistry.CallbackReason.TIMEOUT) {
              handleOperationTimeout(OperationDetails.EMPTY);
            }
          },
          null);

      remainingItems.set(allItems.size());
      Map<ServerConnection, List<E>> initialRequests = new HashMap<>();

      List<E> itemsWithoutServers = new ArrayList<>();

      for (E item : allItems) {

        K key = key(item);

        ReplicationStrategy.ReplicatedOperation replicatedOperation =
            replicationStrategy.replicatedOperation(nodesManager.getServersForKey(key));

        List<ServerConnection> initialServers = replicatedOperation.initialServers();
        BatchItemState batchItemState = batchItemsState.get(key);
        if (initialServers.isEmpty()) {
          synchronized (batchItemState) {
            // we will send it below
            batchItemState.responseCommitted = true;
          }
          itemsWithoutServers.add(item);
        } else {
          synchronized (batchItemState) {
            batchItemState.replicatedOperation = replicatedOperation;
            batchItemState.pendingServers = initialServers.size();
          }
          for (ServerConnection node : initialServers) {
            initialRequests.computeIfAbsent(node, k -> new ArrayList<>()).add(item);
          }
        }
      }

      if (!initialRequests.isEmpty()) {
        long requestId = idGenerator.next();
        initialRequests.forEach((server, list) -> sendMessage(requestId, server, list));
      }

      if (!itemsWithoutServers.isEmpty()) {
        handleFail(
            itemsWithoutServers,
            new DHTNoRegisteredServersException(keys(itemsWithoutServers)),
            OperationDetails.EMPTY);
        int remaining = remainingItems.addAndGet(-itemsWithoutServers.size());
        if (remaining == 0) {
          logger.debug(
              "Immediately completing operation-level callback as all items were missing servers, keys={}",
              formatItemsKeys(allItems));
          callbackRegistry.complete(null, this, null, null);
        }
      }

    } catch (Exception e) {
      panic(e, OperationDetails.EMPTY);
    }
  }

  protected void sendMessage(long requestId, ServerConnection server, List<E> items) {
    class Callback {
      volatile SentMessageMetrics sentMessageMetrics; // can also be read from timeout callback

      void sentMessageMetrics(SentMessageMetrics sentMessageMetrics) {
        this.sentMessageMetrics = sentMessageMetrics;
      }

      void process(CallbackRegistry.CallbackContext ctx) {
        OperationDetails opDetails =
            new OperationDetails(ctx.server(), sentMessageMetrics, ctx.receivedMessageMetrics());
        callback(ctx, items, opDetails);
      }
    }
    Callback callback = new Callback();
    registerMessageCallbackWithTimeout(server, requestId, readTimeout, callback::process);
    messageSender.sendMessage(
        server, generateMessage(requestId, server, items), callback::sentMessageMetrics);
  }

  protected void registerMessageCallbackWithTimeout(
      ServerConnection server,
      long requestId,
      Duration timeout,
      CallbackRegistry.Callback callback) {
    callbackRegistry.registerCallbackWithTimeout(server, requestId, timeout, callback, null);
  }

  protected abstract K key(E e);

  protected List<K> keys(List<E> items) {
    return items.stream().map(this::key).collect(Collectors.toList());
  }

  protected abstract MessageGenerator generateMessage(
      long requestId, ServerConnection server, List<E> items);

  protected abstract List<E> handleResult(
      CallbackRegistry.CallbackContext ctx, List<E> entries, OperationDetails opDetails);

  protected abstract void handleFail(
      List<E> failedEntries, Exception exception, OperationDetails opDetails);

  protected abstract void handleEmptyResult(List<E> entries, OperationDetails opDetails);

  protected void callback(
      CallbackRegistry.CallbackContext ctx, List<E> items, OperationDetails opDetails) {
    try {
      logger.debug(
          "Callback called for keys={}, reason={}, opDetails={}",
          formatItemsKeys(items),
          ctx.reason(),
          opDetails);
      if (ctx.reason().isFailure()) {
        handleInfraFailures(ctx, items, opDetails);
      } else {
        List<E> successfulItems = handleResult(ctx, items, opDetails);
        if (successfulItems != null) {
          successfulItems.stream()
              .map(this::key)
              .forEach(
                  key -> {
                    BatchItemState batchItemState = batchItemsState.get(key);
                    synchronized (batchItemState) {
                      batchItemState.atLeastOneGoodResponse = true;
                    }
                  });
        }
      }
    } catch (Exception e) {
      panic(e, opDetails);
    } finally {
      completeOperationIfNoOutstanding();
    }
  }

  public void completeOperationIfNoOutstanding() {
    if (remainingItems.get() == 0) {
      logger.debug("Completing operation-level callback for keys={}", formatItemsKeys(allItems));
      callbackRegistry.complete(null, this, null, null);
    }
  }

  protected void commitFailure(
      List<E> failedEntries, Exception exception, OperationDetails opDetails) {
    List<E> entriesToFail = new ArrayList<>();
    failedEntries.forEach(
        item -> {
          K key = key(item);
          BatchItemState batchItemState = batchItemsState.get(key);
          if (batchItemState != null) {
            synchronized (batchItemState) {
              if (!batchItemState.responseCommitted) {
                batchItemState.responseCommitted = true;
                remainingItems.decrementAndGet();
                entriesToFail.add(item);
              }
            }
          }
        });
    handleFail(entriesToFail, exception, opDetails);
  }

  protected void handleInfraFailures(
      CallbackRegistry.CallbackContext ctx, List<E> failedEntries, OperationDetails opDetails) {
    Map<ServerConnection, List<E>> additionalRequests = new HashMap<>();
    List<E> outOfServersEntries = new ArrayList<>();
    List<E> emptyEntries = new ArrayList<>();
    for (E failedEntry : failedEntries) {
      K key = key(failedEntry);
      BatchItemState batchItemState = batchItemsState.get(key);

      synchronized (batchItemState) {
        if (batchItemState.responseCommitted) {
          --batchItemState.pendingServers;
          handleSilentFailure(ctx, failedEntry, opDetails);
          continue;
        }

        ServerConnection replacement = batchItemState.replicatedOperation.replacement(ctx.server());
        if (replacement == null) {
          int pendingServers = --batchItemState.pendingServers;
          if (pendingServers == 0 && !batchItemState.responseCommitted) {
            batchItemState.responseCommitted = true;
            remainingItems.decrementAndGet();
            if (batchItemState.atLeastOneGoodResponse) {
              emptyEntries.add(failedEntry);
            } else {
              outOfServersEntries.add(failedEntry);
            }
          }
        } else {
          additionalRequests.computeIfAbsent(replacement, k -> new ArrayList<>()).add(failedEntry);
        }
      }
    }

    if (!additionalRequests.isEmpty()) {
      long requestId = idGenerator.next();
      logger.warn(
          "After requestId={}, keys={} with opDetails={} failed with reason={}, additional requests (count={}) with newRequestId={} will be sent",
          ctx.key(),
          formatItemsKeys(failedEntries),
          opDetails,
          ctx.reason(),
          additionalRequests.size(),
          requestId);
      additionalRequests.forEach((s, list) -> sendMessage(requestId, s, list));
    }

    if (!outOfServersEntries.isEmpty()) {
      DHTException nestedException = null;
      switch (ctx.reason()) {
        case TIMEOUT:
          nestedException =
              new DHTReadTimeoutException(
                  ctx.server(), (Duration) ctx.param(), keys(outOfServersEntries));
          break;
        case CONNECTION_FAILED:
          nestedException =
              new DHTConnectionFailedException(
                  ctx.server(), keys(outOfServersEntries), (Throwable) ctx.param());
          break;
        case CONNECTION_LOST:
          nestedException =
              new DHTConnectionLostException(
                  ctx.server(), keys(outOfServersEntries), (Throwable) ctx.param());
          break;
        default:
          throw new IllegalStateException("Unexpected failure reason: " + ctx.reason());
      }
      handleFail(
          outOfServersEntries,
          new DHTNoReachableServersException(keys(outOfServersEntries), nestedException),
          opDetails);
    }

    if (!emptyEntries.isEmpty()) {
      handleEmptyResult(emptyEntries, opDetails);
    }
  }

  protected void handleFailureInResponse(
      ServerConnection server,
      List<E> failedEntries,
      Exception exception,
      OperationDetails opDetails) {
    List<E> terminalEntries = new ArrayList<>();
    List<E> emptyEntries = new ArrayList<>();
    for (E failedEntry : failedEntries) {
      K key = key(failedEntry);
      BatchItemState batchItemState = batchItemsState.get(key);
      synchronized (batchItemState) {
        int pendingServers = --batchItemState.pendingServers;
        if (pendingServers == 0 && !batchItemState.responseCommitted) {
          batchItemState.responseCommitted = true;
          remainingItems.decrementAndGet();
          if (batchItemState.atLeastOneGoodResponse) {
            emptyEntries.add(failedEntry);
          } else {
            terminalEntries.add(failedEntry);
          }
        }
      }
    }
    if (!emptyEntries.isEmpty()) {
      handleEmptyResult(emptyEntries, opDetails);
    }
    if (!terminalEntries.isEmpty()) {
      handleFail(terminalEntries, exception, opDetails);
    }
  }

  protected void decrementPending(K key, BiFunction<Integer, Boolean, Boolean> function) {
    BatchItemState batchItemState = batchItemsState.get(key);
    synchronized (batchItemState) {
      int pendingServers = --batchItemState.pendingServers;
      Boolean committed = function.apply(pendingServers, batchItemState.responseCommitted);
      batchItemState.responseCommitted |= committed;
      if (committed) {
        remainingItems.decrementAndGet();
      }
    }
  }

  protected void handleOperationTimeout(OperationDetails opDetails) {
    if (remainingItems.get() > 0) {
      List<E> toTimeout = new ArrayList<>();
      allItems.forEach(
          item -> {
            BatchItemState batchItemState = batchItemsState.get(key(item));
            synchronized (batchItemState) {
              if (!batchItemState.responseCommitted) {
                batchItemState.responseCommitted = true;
                remainingItems.decrementAndGet();
                toTimeout.add(item);
              }
            }
          });
      if (!toTimeout.isEmpty()) {
        handleFail(
            toTimeout,
            new DHTOperationTimeoutException(operationTimeout, keys(toTimeout)),
            opDetails);
      } else {
        logger.warn(
            "Timed out operation indicated it has remaining items, but none were found, keys={}",
            formatItemsKeys(allItems));
      }
    } else {
      logger.warn(
          "Operation timed out, but it has no remaining items, keys={}", formatItemsKeys(allItems));
    }
  }

  protected void handleSilentFailure(
      CallbackRegistry.CallbackContext ctx, E failedEntry, OperationDetails opDetails) {}

  protected void panic(Exception e, OperationDetails opDetails) {
    List<E> itemsToFail = new ArrayList<>();
    allItems.forEach(
        item -> {
          K key = key(item);
          BatchItemState batchItemState = batchItemsState.get(key);
          if (batchItemsState != null) {
            synchronized (batchItemState) {
              if (!batchItemState.responseCommitted) {
                batchItemState.responseCommitted = true;
                remainingItems.decrementAndGet();
                itemsToFail.add(item);
              }
            }
          } else {
            itemsToFail.add(item);
          }
        });
    logger.error(
        "Caught unexpected exception - will fail all remaining keys, keys="
            + formatItemsKeys(itemsToFail),
        e);
    handleFail(itemsToFail, e, opDetails);
  }
}
