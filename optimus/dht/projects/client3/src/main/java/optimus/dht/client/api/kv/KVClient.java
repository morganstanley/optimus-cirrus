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
package optimus.dht.client.api.kv;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import optimus.dht.client.api.Key;
import optimus.dht.client.api.module.ModuleClient;
import optimus.dht.client.api.transport.OperationDetails;
import optimus.dht.common.api.Keyspace;

public interface KVClient<K extends Key> extends ModuleClient {

  interface Callback<T> {
    void result(T result, OperationDetails opDetails);

    void error(Throwable exception, OperationDetails opDetails);
  }

  CompletableFuture<KVEntry<K>> get(Keyspace keyspace, K key, String correlationName);

  CompletableFuture<Boolean> put(Keyspace keyspace, KVEntry<K> entry, String correlationName);

  CompletableFuture<Boolean> contains(Keyspace keyspace, K key, String correlationName);

  CompletableFuture<Boolean> remove(Keyspace keyspace, K key, String correlationName);

  void get(Keyspace keyspace, K key, String correlationName, Callback<KVEntry<K>> resultCallback);

  void put(
      Keyspace keyspace,
      KVEntry<K> entry,
      String correlationName,
      Callback<Boolean> resultCallback);

  void contains(Keyspace keyspace, K key, String correlationName, Callback<Boolean> resultCallback);

  void remove(Keyspace keyspace, K key, String correlationName, Callback<Boolean> resultCallback);

  interface BatchCallback<T, K extends Key> {
    void results(List<T> results, OperationDetails opDetails);

    void errors(List<K> keys, Exception exception, OperationDetails opDetails);
  }

  void batchGet(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KVEntry<K>, K> resultCallback);

  void batchPut(
      Keyspace keyspace,
      List<KVEntry<K>> entries,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback);

  void batchContains(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback);

  void batchRemove(
      Keyspace keyspace,
      List<K> keys,
      String correlationName,
      BatchCallback<KeyWithResult<Boolean, K>, K> resultCallback);

  class KeyWithResult<T, K extends Key> {
    private final K key;
    private final T result;

    public KeyWithResult(K key, T result) {
      this.key = key;
      this.result = result;
    }

    public K key() {
      return key;
    }

    public T result() {
      return result;
    }
  }

  interface RemoveTransientKeyspaceCallback {
    void finished(
        Keyspace keyspace,
        int removedFromServers,
        int didntHaveServers,
        int failedServers,
        int timedOutServers);
  }

  void removeTransientKeyspace(
      Keyspace keyspace, String correlationName, RemoveTransientKeyspaceCallback resultCallback);

  void getLargeValue(
      Keyspace keyspace, K key, String correlationName, Callback<KVLargeEntry<K>> resultCallback);

  void putLargeValue(
      Keyspace keyspace,
      KVLargeEntry<K> entry,
      String correlationName,
      Callback<Boolean> resultCallback);
}
