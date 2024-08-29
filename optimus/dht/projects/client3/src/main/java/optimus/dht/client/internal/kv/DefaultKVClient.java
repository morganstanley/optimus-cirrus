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
package optimus.dht.client.internal.kv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import optimus.dht.client.api.DHTClient;
import optimus.dht.client.api.Key;
import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.callback.TimeoutsConfig;
import optimus.dht.client.api.callback.CallbackRegistry.CallbackContext;
import optimus.dht.client.api.callback.CallbackRegistry.CallbackReason;
import optimus.dht.client.api.exceptions.DHTRemoteException;
import optimus.dht.client.api.hash.HashCalculator;
import optimus.dht.client.api.kv.KVClient;
import optimus.dht.client.api.kv.KVEntry;
import optimus.dht.client.api.kv.KVLargeEntry;
import optimus.dht.client.api.kv.KVLargeValue;
import optimus.dht.client.api.kv.KVValue;
import optimus.dht.client.api.replication.BatchOperationTemplate;
import optimus.dht.client.api.replication.ReplicationStrategy;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerConnectionsManager;
import optimus.dht.client.api.transport.MessageSender;
import optimus.dht.client.api.transport.OperationDetails;
import optimus.dht.client.api.transport.RequestIdGenerator;
import optimus.dht.client.internal.kv.message.KVGenericResponse;
import optimus.dht.client.internal.kv.message.KVKeyOnlyRequestGenerator;
import optimus.dht.client.internal.kv.message.KVLargePutRequestGenerator;
import optimus.dht.client.internal.kv.message.KVPutRequestGenerator;
import optimus.dht.client.internal.kv.message.KVRemoveTransientKeyspaceRequestGenerator;
import optimus.dht.client.internal.kv.message.KVValueResponse;
import optimus.dht.common.api.Keyspace;
import optimus.dht.common.api.kv.KVRequestType;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.SentMessageMetrics;
import optimus.dht.common.util.DHTUtil;

@Singleton
public class DefaultKVClient<K extends Key> implements KVClient<K> {

  private final DHTClient dhtClient;
  private final TimeoutsConfig timeoutsConfig;
  private final ReplicationStrategy replicationStrategy;
  private final HashCalculator hashCalculator;
  private final ServerConnectionsManager nodesManager;
  private final RequestIdGenerator idGenerator;
  private final CallbackRegistry callbackRegistry;
  private final MessageSender messageSender;

  @Inject
  public DefaultKVClient(
      DHTClient dhtClient,
      TimeoutsConfig timeoutsConfig,
      ReplicationStrategy replicationStrategy,
      HashCalculator hashCalculator,
      ServerConnectionsManager nodesManager,
      RequestIdGenerator idGenerator,
      CallbackRegistry callbackRegistry,
      MessageSender messageSender) {
    this.dhtClient = dhtClient;
    this.timeoutsConfig = timeoutsConfig;
    this.replicationStrategy = replicationStrategy;
    this.hashCalculator = hashCalculator;
    this.nodesManager = nodesManager;
    this.idGenerator = idGenerator;
    this.callbackRegistry = callbackRegistry;
    this.messageSender = messageSender;
  }

  @Override
  public DHTClient dhtClient() {
    return dhtClient;
  }

  private static class BatchCallbackToCompletableFuture<T, K extends Key>
      implements BatchCallback<T, K> {
    private final CompletableFuture<T> completableFuture;

    public BatchCallbackToCompletableFuture(CompletableFuture<T> completableFuture) {
      this.completableFuture = completableFuture;
    }

    @Override
    public void results(List<T> results, OperationDetails opDetails) {
      completableFuture.complete(results.iterator().next());
    }

    @Override
    public void errors(List<K> keys, Exception exception, OperationDetails opDetails) {
      completableFuture.completeExceptionally(exception);
    }
  }

  private static class WithKeyBatchCallbackToCompletableFuture<T, K extends Key>
      implements BatchCallback<KeyWithResult<T, K>, K> {
    private final CompletableFuture<T> completableFuture;

    public WithKeyBatchCallbackToCompletableFuture(CompletableFuture<T> completableFuture) {
      this.completableFuture = completableFuture;
    }

    @Override
    public void results(List<KeyWithResult<T, K>> results, OperationDetails opDetails) {
      completableFuture.complete(results.iterator().next().result());
    }

    @Override
    public void errors(List<K> keys, Exception exception, OperationDetails opDetails) {
      completableFuture.completeExceptionally(exception);
    }
  }

  @Override
  public CompletableFuture<KVEntry<K>> get(Keyspace keyspace, K key, String correlationName) {
    CompletableFuture<KVEntry<K>> future = new CompletableFuture<>();
    batchGet(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new BatchCallbackToCompletableFuture<KVEntry<K>, K>(future));
    return future;
  }

  @Override
  public CompletableFuture<Boolean> put(
      Keyspace keyspace, KVEntry<K> entry, String correlationName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    batchPut(
        keyspace,
        Arrays.asList(entry),
        correlationName,
        new WithKeyBatchCallbackToCompletableFuture<Boolean, K>(future));
    return future;
  }

  @Override
  public CompletableFuture<Boolean> contains(Keyspace keyspace, K key, String correlationName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    batchContains(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new WithKeyBatchCallbackToCompletableFuture<Boolean, K>(future));
    return future;
  }

  @Override
  public CompletableFuture<Boolean> remove(Keyspace keyspace, K key, String correlationName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    batchRemove(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new WithKeyBatchCallbackToCompletableFuture<Boolean, K>(future));
    return future;
  }

  private static class SingleToBatchCallback<T, K extends Key> implements BatchCallback<T, K> {
    private final Callback<T> singleCallback;

    public SingleToBatchCallback(Callback<T> singleCallback) {
      this.singleCallback = singleCallback;
    }

    @Override
    public void results(List<T> results, OperationDetails opDetails) {
      singleCallback.result(results.iterator().next(), opDetails);
    }

    @Override
    public void errors(List<K> keys, Exception exception, OperationDetails opDetails) {
      singleCallback.error(exception, opDetails);
    }
  }

  private static class WithKeySingleToBatchCallback<T, K extends Key>
      implements BatchCallback<KeyWithResult<T, K>, K> {
    private final Callback<T> singleCallback;

    public WithKeySingleToBatchCallback(Callback<T> singleCallback) {
      this.singleCallback = singleCallback;
    }

    @Override
    public void results(List<KeyWithResult<T, K>> results, OperationDetails opDetails) {
      singleCallback.result(results.iterator().next().result(), opDetails);
    }

    @Override
    public void errors(List<K> keys, Exception exception, OperationDetails opDetails) {
      singleCallback.error(exception, opDetails);
    }
  }

  @Override
  public void get(
      Keyspace keyspace, K key, String correlationName, Callback<KVEntry<K>> resultCallback) {
    batchGet(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new SingleToBatchCallback<KVEntry<K>, K>(resultCallback));
  }

  @Override
  public void put(
      Keyspace keyspace,
      KVEntry<K> entry,
      String correlationName,
      Callback<Boolean> resultCallback) {
    batchPut(
        keyspace,
        Arrays.asList(entry),
        correlationName,
        new WithKeySingleToBatchCallback<Boolean, K>(resultCallback));
  }

  @Override
  public void contains(
      Keyspace keyspace, K key, String correlationName, Callback<Boolean> resultCallback) {
    batchContains(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new WithKeySingleToBatchCallback<Boolean, K>(resultCallback));
  }

  @Override
  public void remove(
      Keyspace keyspace, K key, String correlationName, Callback<Boolean> resultCallback) {
    batchRemove(
        keyspace,
        Arrays.asList(key),
        correlationName,
        new WithKeySingleToBatchCallback<Boolean, K>(resultCallback));
  }

  private void checkUniqueKeys(List<K> keys) {
    if (keys.size() > 1) {
      K duplicate = DHTUtil.findDuplicate(keys, x -> x);
      if (duplicate != null) {
        throw new IllegalArgumentException("Keys are not unique");
      }
    }
  }

  private void checkUniqueKeysInEntries(List<KVEntry<K>> entries) {
    if (entries.size() > 1) {
      KVEntry<K> duplicate = DHTUtil.findDuplicate(entries, x -> x.key());
      if (duplicate != null) {
        throw new IllegalArgumentException("Keys in the passed entries are not unique");
      }
    }
  }

  @Override
  public void batchGet(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KVEntry<K>, K> resultCallback) {

    keys.forEach(k -> k.ensureHash(hashCalculator));
    checkUniqueKeys(keys);

    new BatchOperationTemplate<K, K>(
        timeoutsConfig.readTimeout(),
        timeoutsConfig.defaultOperationTimeout(),
        replicationStrategy,
        nodesManager,
        idGenerator,
        callbackRegistry,
        messageSender,
        keys) {

      @Override
      public K key(K e) {
        return e;
      }

      @Override
      protected MessageGenerator generateMessage(
          long requestId, ServerConnection server, List<K> items) {
        return new KVKeyOnlyRequestGenerator(
            KVRequestType.GET, requestId, keyspace, items, correlationName);
      }

      @Override
      protected List<K> handleResult(
          CallbackContext ctx, List<K> entries, OperationDetails opDetails) {
        KVValueResponse response = (KVValueResponse) ctx.param();

        if (response.errorCode() != null) {
          DHTRemoteException exception =
              new DHTRemoteException(response.errorCode(), response.errorText(), keys(entries));
          handleFailureInResponse(ctx.server(), entries, exception, opDetails);
          return Collections.emptyList();
        }

        List<KVEntry<K>> results = new ArrayList<>();

        for (int i = 0; i < entries.size(); ++i) {
          K key = entries.get(i);
          KVLargeValue kvLargeValue = response.values().get(i);
          KVValue kvValue;
          if (kvLargeValue == null) {
            kvValue = null;
          } else if (kvLargeValue.buffers().length == 1) {
            kvValue =
                new KVValue(
                    kvLargeValue.buffers()[0].array(),
                    kvLargeValue.entryInfo(),
                    kvLargeValue.expiry());
          } else {
            throw new IllegalArgumentException("This API cannot be used with >=2GB payloads");
          }

          decrementPending(
              key,
              (pendingServers, responseCommitted) -> {
                if (kvValue != null) {
                  if (!responseCommitted) {
                    results.add(new KVEntry<>(key, kvValue));
                    return true;
                  }
                } else if (pendingServers == 0) {
                  if (!responseCommitted) {
                    results.add(new KVEntry<>(key, null));
                    return true;
                  }
                }
                return false;
              });
        }

        if (!results.isEmpty()) {
          resultCallback.results(results, opDetails);
        }
        return entries;
      }

      @Override
      protected void handleFail(List<K> failedEntries, Exception e, OperationDetails opDetails) {
        resultCallback.errors(failedEntries, e, opDetails);
      }

      @Override
      protected void handleEmptyResult(List<K> entries, OperationDetails opDetails) {
        List<KVEntry<K>> results =
            entries.stream().map(k -> new KVEntry<>(k, null)).collect(Collectors.toList());
        resultCallback.results(results, opDetails);
      }
    }.run();
  }

  @Override
  public void batchPut(
      Keyspace keyspace,
      List<KVEntry<K>> entries,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback) {

    entries.forEach(e -> e.key().ensureHash(hashCalculator));
    checkUniqueKeysInEntries(entries);

    new BatchOperationTemplate<K, KVEntry<K>>(
        timeoutsConfig.readTimeout(),
        timeoutsConfig.defaultOperationTimeout(),
        replicationStrategy,
        nodesManager,
        idGenerator,
        callbackRegistry,
        messageSender,
        entries) {

      @Override
      public K key(KVEntry<K> e) {
        return e.key();
      }

      private List<K> toKeys(List<KVEntry<K>> entries) {
        return entries.stream().map(KVEntry::key).collect(Collectors.toList());
      }

      @Override
      protected MessageGenerator generateMessage(
          long requestId, ServerConnection server, List<KVEntry<K>> items) {
        return new KVPutRequestGenerator<>(requestId, keyspace, items, correlationName);
      }

      @Override
      protected List<KVEntry<K>> handleResult(
          CallbackContext ctx, List<KVEntry<K>> entries, OperationDetails opDetails) {
        KVGenericResponse response = (KVGenericResponse) ctx.param();

        if (response.errorCode() != null) {
          DHTRemoteException exception =
              new DHTRemoteException(response.errorCode(), response.errorText(), keys(entries));
          handleFailureInResponse(ctx.server(), entries, exception, opDetails);
          return Collections.emptyList();
        }

        List<KeyWithResult<Boolean, K>> results = new ArrayList<>();

        for (int i = 0; i < entries.size(); ++i) {
          KVEntry<K> entry = entries.get(i);
          K key = entry.key();
          boolean singleResponse = response.responses().get(i);

          decrementPending(
              key,
              (pendingServers, responseCommitted) -> {
                if (pendingServers == 0 && !responseCommitted) {
                  results.add(new KeyWithResult<>(key, singleResponse));
                  return true;
                } else {
                  return false;
                }
              });
        }

        if (!results.isEmpty()) {
          resultCallback.results(results, opDetails);
        }
        return entries;
      }

      @Override
      protected void handleFail(
          List<KVEntry<K>> failedEntries, Exception e, OperationDetails opDetails) {
        resultCallback.errors(toKeys(failedEntries), e, opDetails);
      }

      @Override
      protected void handleEmptyResult(List<KVEntry<K>> entries, OperationDetails opDetails) {
        List<KeyWithResult<Boolean, K>> results =
            entries.stream()
                .map(e -> new KeyWithResult<>(e.key(), false))
                .collect(Collectors.toList());
        resultCallback.results(results, opDetails);
      }
    }.run();
  }

  @Override
  public void batchContains(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback) {

    keys.forEach(k -> k.ensureHash(hashCalculator));
    checkUniqueKeys(keys);

    new KVGenericBatchOperation<>(
            timeoutsConfig,
            replicationStrategy,
            nodesManager,
            idGenerator,
            callbackRegistry,
            messageSender,
            keys,
            KVRequestType.CONTAINS,
            keyspace,
            correlationName,
            resultCallback)
        .run();
  }

  @Override
  public void batchRemove(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback) {

    keys.forEach(k -> k.ensureHash(hashCalculator));
    checkUniqueKeys(keys);

    new KVGenericBatchOperation<>(
            timeoutsConfig,
            replicationStrategy,
            nodesManager,
            idGenerator,
            callbackRegistry,
            messageSender,
            keys,
            KVRequestType.REMOVE,
            keyspace,
            correlationName,
            resultCallback)
        .run();
  }

  class KVGenericBatchOperation<K extends Key> extends BatchOperationTemplate<K, K> {

    private final KVRequestType type;
    private final Keyspace keyspace;
    private final String correlationName;
    private final BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback;

    public KVGenericBatchOperation(
        TimeoutsConfig timeoutsConfig,
        ReplicationStrategy replicationStrategy,
        ServerConnectionsManager nodesManager,
        RequestIdGenerator idGenerator,
        CallbackRegistry callbackRegistry,
        MessageSender messageSender,
        List<K> allItems,
        KVRequestType type,
        Keyspace keyspace,
        String correlationName,
        BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback) {
      super(
          timeoutsConfig.readTimeout(),
          timeoutsConfig.defaultOperationTimeout(),
          replicationStrategy,
          nodesManager,
          idGenerator,
          callbackRegistry,
          messageSender,
          allItems);
      this.type = type;
      this.keyspace = keyspace;
      this.correlationName = correlationName;
      this.resultCallback = resultCallback;
    }

    @Override
    public K key(K e) {
      return e;
    }

    @Override
    protected MessageGenerator generateMessage(
        long requestId, ServerConnection server, List<K> items) {
      return new KVKeyOnlyRequestGenerator(type, requestId, keyspace, items, correlationName);
    }

    @Override
    public List<K> handleResult(CallbackContext ctx, List<K> entries, OperationDetails opDetails) {
      KVGenericResponse response = (KVGenericResponse) ctx.param();

      if (response.errorCode() != null) {
        DHTRemoteException exception =
            new DHTRemoteException(response.errorCode(), response.errorText(), keys(entries));
        handleFailureInResponse(ctx.server(), entries, exception, opDetails);
        return Collections.emptyList();
      }

      List<KeyWithResult<Boolean, K>> results = new ArrayList<>();

      for (int i = 0; i < entries.size(); ++i) {
        K key = entries.get(i);
        boolean singleResponse = response.responses().get(i);

        decrementPending(
            key,
            (pendingServers, responseCommitted) -> {
              if (!responseCommitted) {
                results.add(new KeyWithResult<>(key, singleResponse));
              }
              return true;
            });
      }

      if (!results.isEmpty()) {
        resultCallback.results(results, opDetails);
      }
      return entries;
    }

    @Override
    protected void handleFail(
        List<K> failedEntries, Exception exception, OperationDetails opDetails) {
      resultCallback.errors(failedEntries, exception, opDetails);
    }

    @Override
    protected void handleEmptyResult(List<K> entries, OperationDetails opDetails) {
      List<KeyWithResult<Boolean, K>> results =
          entries.stream().map(k -> new KeyWithResult<>(k, false)).collect(Collectors.toList());
      resultCallback.results(results, opDetails);
    }
  }

  class RemoveTransientKeyspaceOperation {

    private final Keyspace keyspace;
    private final String correlationName;
    private final RemoveTransientKeyspaceCallback resultCallback;

    private final Set<ServerConnection> outstandingServers = new HashSet<>();
    private boolean finished = false;

    private int removedFromServers = 0;
    private int didntHaveServers = 0;
    private int failedServers = 0;
    private int timedOutServers = 0;

    public RemoveTransientKeyspaceOperation(
        Keyspace keyspace, String correlationName, RemoveTransientKeyspaceCallback resultCallback) {
      this.keyspace = keyspace;
      this.correlationName = correlationName;
      this.resultCallback = resultCallback;
    }

    public void run() {

      // first register operation-level timeout
      callbackRegistry.registerCallbackWithTimeout(
          null, this, timeoutsConfig.defaultOperationTimeout(), this::operationCallback);
      long requestId = idGenerator.next();
      KVRemoveTransientKeyspaceRequestGenerator requestGenerator =
          new KVRemoveTransientKeyspaceRequestGenerator(requestId, keyspace, correlationName);

      for (ServerConnection activeServer : nodesManager.getActiveServers()) {
        synchronized (this) {
          outstandingServers.add(activeServer);
        }
        AtomicReference<SentMessageMetrics> sentMetricsRef = new AtomicReference<>();
        callbackRegistry.registerCallbackWithTimeout(
            activeServer,
            requestId,
            timeoutsConfig.readTimeout(),
            ctx -> requestCallback(ctx, sentMetricsRef.get()));
        messageSender.sendMessage(activeServer, requestGenerator, sentMetricsRef::set);
      }
    }

    private void operationCallback(CallbackContext callbackContext) {
      if (callbackContext.reason() == CallbackReason.TIMEOUT) {
        synchronized (this) {
          if (finished) {
            return;
          }
          finished = true;
        }

        resultCallback.finished(
            keyspace,
            removedFromServers,
            didntHaveServers,
            failedServers,
            timedOutServers + outstandingServers.size());
      }
    }

    private void requestCallback(
        CallbackContext callbackContext, SentMessageMetrics sentMessageMetrics) {

      synchronized (this) {
        if (finished) {
          return;
        }

        if (callbackContext.reason() == CallbackReason.COMPLETED) {
          KVGenericResponse response = (KVGenericResponse) callbackContext.param();
          if (response.errorCode() != null) {
            ++failedServers;
          } else if (response.responses().get(0)) {
            ++removedFromServers;
          } else {
            ++didntHaveServers;
          }
        } else if (callbackContext.reason() == CallbackReason.TIMEOUT) {
          ++timedOutServers;
        } else {
          ++failedServers;
        }

        outstandingServers.remove(callbackContext.server());
        if (outstandingServers.isEmpty()) {
          finished = true;
        }
      }

      if (finished) {
        callbackRegistry.complete(null, this, null, null); // clears operation-level timeout
        resultCallback.finished(
            keyspace, removedFromServers, didntHaveServers, failedServers, timedOutServers);
      }
    }
  }

  @Override
  public void removeTransientKeyspace(
      Keyspace keyspace, String correlationName, RemoveTransientKeyspaceCallback resultCallback) {
    if (!keyspace.isTransient()) {
      throw new IllegalArgumentException(
          "Only transient keyspaces can be removed, keyspace=" + keyspace);
    }
    new RemoveTransientKeyspaceOperation(keyspace, correlationName, resultCallback).run();
  }

  @Override
  public void getLargeValue(
      Keyspace keyspace, K key, String correlationName, Callback<KVLargeEntry<K>> resultCallback) {

    key.ensureHash(hashCalculator);

    new BatchOperationTemplate<K, K>(
        timeoutsConfig.readTimeout(),
        timeoutsConfig.defaultOperationTimeout(),
        replicationStrategy,
        nodesManager,
        idGenerator,
        callbackRegistry,
        messageSender,
        Arrays.asList(key)) {

      @Override
      public K key(K e) {
        return e;
      }

      @Override
      protected MessageGenerator generateMessage(
          long requestId, ServerConnection server, List<K> items) {
        return new KVKeyOnlyRequestGenerator(
            KVRequestType.GET, requestId, keyspace, items, correlationName);
      }

      @Override
      protected List<K> handleResult(
          CallbackContext ctx, List<K> entries, OperationDetails opDetails) {
        KVValueResponse response = (KVValueResponse) ctx.param();

        if (response.errorCode() != null) {
          DHTRemoteException exception =
              new DHTRemoteException(response.errorCode(), response.errorText(), keys(entries));
          handleFailureInResponse(ctx.server(), entries, exception, opDetails);
          return Collections.emptyList();
        }

        for (int i = 0; i < entries.size(); ++i) {
          K key = entries.get(i);
          KVLargeValue kvValue = response.values().get(i);

          decrementPending(
              key,
              (pendingServers, responseCommitted) -> {
                if (kvValue != null) {
                  if (!responseCommitted) {
                    resultCallback.result(new KVLargeEntry<>(key, kvValue), opDetails);
                    return true;
                  }
                } else if (pendingServers == 0) {
                  if (!responseCommitted) {
                    resultCallback.result(new KVLargeEntry<>(key, null), opDetails);
                    return true;
                  }
                }
                return false;
              });
        }

        return entries;
      }

      @Override
      protected void handleFail(List<K> failedEntries, Exception e, OperationDetails opDetails) {
        resultCallback.error(e, opDetails);
      }

      @Override
      protected void handleEmptyResult(List<K> entries, OperationDetails opDetails) {
        resultCallback.result(new KVLargeEntry<>(entries.get(0), null), opDetails);
      }
    }.run();
  }

  @Override
  public void putLargeValue(
      Keyspace keyspace,
      KVLargeEntry<K> entry,
      String correlationName,
      Callback<Boolean> resultCallback) {

    entry.key().ensureHash(hashCalculator);

    new BatchOperationTemplate<K, KVLargeEntry<K>>(
        timeoutsConfig.readTimeout(),
        timeoutsConfig.defaultOperationTimeout(),
        replicationStrategy,
        nodesManager,
        idGenerator,
        callbackRegistry,
        messageSender,
        Arrays.asList(entry)) {

      @Override
      public K key(KVLargeEntry<K> e) {
        return e.key();
      }

      private List<K> toKeys(List<KVEntry<K>> entries) {
        return entries.stream().map(KVEntry::key).collect(Collectors.toList());
      }

      @Override
      protected MessageGenerator generateMessage(
          long requestId, ServerConnection server, List<KVLargeEntry<K>> items) {
        return new KVLargePutRequestGenerator<>(requestId, keyspace, items.get(0), correlationName);
      }

      @Override
      protected List<KVLargeEntry<K>> handleResult(
          CallbackContext ctx, List<KVLargeEntry<K>> entries, OperationDetails opDetails) {
        KVGenericResponse response = (KVGenericResponse) ctx.param();

        if (response.errorCode() != null) {
          DHTRemoteException exception =
              new DHTRemoteException(response.errorCode(), response.errorText(), keys(entries));
          handleFailureInResponse(ctx.server(), entries, exception, opDetails);
          return Collections.emptyList();
        }

        for (int i = 0; i < entries.size(); ++i) {
          KVLargeEntry<K> entry = entries.get(i);
          K key = entry.key();
          boolean singleResponse = response.responses().get(i);

          decrementPending(
              key,
              (pendingServers, responseCommitted) -> {
                if (pendingServers == 0 && !responseCommitted) {
                  resultCallback.result(singleResponse, opDetails);
                  return true;
                } else {
                  return false;
                }
              });
        }

        return entries;
      }

      @Override
      protected void handleFail(
          List<KVLargeEntry<K>> failedEntries, Exception e, OperationDetails opDetails) {
        resultCallback.error(e, opDetails);
      }

      @Override
      protected void handleEmptyResult(List<KVLargeEntry<K>> entries, OperationDetails opDetails) {
        resultCallback.result(false, opDetails);
      }
    }.run();
  }
}
