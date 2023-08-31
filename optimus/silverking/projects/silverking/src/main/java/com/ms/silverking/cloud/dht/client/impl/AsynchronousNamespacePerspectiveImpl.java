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
package com.ms.silverking.cloud.dht.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSingleRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSyncRequest;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.SessionClosedException;
import com.ms.silverking.cloud.dht.client.WaitForCompletionException;
import com.ms.silverking.cloud.dht.client.impl.ClientNamespace.OpLWTMode;

class AsynchronousNamespacePerspectiveImpl<K, V> extends BaseNamespacePerspectiveImpl<K, V>
    implements AsynchronousNamespacePerspective<K, V> {

  private static final OpLWTMode opLWTMode;

  static {
    opLWTMode = OpLWTMode.DisallowUserThreadUsage;
  }

  AsynchronousNamespacePerspectiveImpl(
      ClientNamespace clientNamespace,
      String name,
      NamespacePerspectiveOptionsImpl<K, V> nspoImpl) {
    super(clientNamespace, name, nspoImpl);
  }

  // reads

  @Override
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions) {
    return baseRetrieve(keys, retrievalOptions, opLWTMode);
  }

  @Override
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys) {
    return retrieve(keys, nspoImpl.getDefaultGetOptions());
  }

  @Override
  public AsyncSingleRetrieval<K, V> retrieve(K key, RetrievalOptions retrievalOptions) {
    return (AsyncSingleRetrieval<K, V>)
        baseRetrieve(ImmutableSet.of(key), retrievalOptions, opLWTMode);
  }

  @Override
  public AsyncSingleRetrieval<K, V> retrieve(K key) {
    return retrieve(key, nspoImpl.getDefaultGetOptions());
  }

  // gets

  @Override
  public AsyncValueRetrieval<K, V> get(Set<? extends K> keys, GetOptions getOptions) {
    return (AsyncValueRetrieval<K, V>) retrieve(keys, getOptions);
  }

  @Override
  public AsyncValueRetrieval<K, V> get(Set<? extends K> keys) {
    return get(keys, nspoImpl.getDefaultGetOptions());
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> get(K key, GetOptions getOptions) {
    return (AsyncSingleValueRetrieval<K, V>) retrieve(ImmutableSet.of(key), getOptions);
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> get(K key) {
    return get(key, nspoImpl.getDefaultGetOptions());
  }

  // waitfors

  @Override
  public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys, WaitOptions waitOptions) {
    return (AsyncValueRetrieval<K, V>) retrieve(keys, waitOptions);
  }

  @Override
  public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys) {
    return waitFor(keys, nspoImpl.getDefaultWaitOptions());
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> waitFor(K key, WaitOptions waitOptions) {
    return (AsyncSingleValueRetrieval<K, V>) retrieve(ImmutableSet.of(key), waitOptions);
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> waitFor(K key) {
    return waitFor(key, nspoImpl.getDefaultWaitOptions());
  }

  // puts

  @Override
  public AsyncPut<K> put(Map<? extends K, ? extends V> values, PutOptions putOptions) {
    return basePut(values, putOptions, this, opLWTMode);
  }

  @Override
  public AsyncPut<K> put(Map<? extends K, ? extends V> values) {
    return put(values, nspoImpl.getDefaultPutOptions());
  }

  @Override
  public AsyncPut<K> put(K key, V value, PutOptions putOptions) {
    return put(ImmutableMap.of(key, value), putOptions);
  }

  @Override
  public AsyncPut<K> put(K key, V value) {
    return put(key, value, nspoImpl.getDefaultPutOptions());
  }

  // invalidations

  public AsyncInvalidation<K> invalidate(
      Set<? extends K> keys, InvalidationOptions invalidationOptions) {
    return baseInvalidation(keys, invalidationOptions, nspoImpl.getValueSerializer(), opLWTMode);
  }

  public AsyncInvalidation<K> invalidate(Set<? extends K> keys) {
    return invalidate(keys, nspoImpl.getDefaultInvalidationOptions());
  }

  public AsyncInvalidation<K> invalidate(K key, InvalidationOptions invalidationOptions) {
    return invalidate(ImmutableSet.of(key), invalidationOptions);
  }

  public AsyncInvalidation<K> invalidate(K key) {
    return invalidate(key, nspoImpl.getDefaultInvalidationOptions());
  }

  //

  @Override
  public void waitForActiveOps() throws WaitForCompletionException {
    List<AsyncOperationImpl> activeAsyncOps;
    List<AsyncOperation> failedOps;

    failedOps = new ArrayList<>();
    activeAsyncOps = clientNamespace.getActiveAsyncOperations();
    for (AsyncOperationImpl activeAsyncOp : activeAsyncOps) {
      try {
        activeAsyncOp.waitForCompletion();
      } catch (OperationException oe) {
        failedOps.add(activeAsyncOp);
      }
    }
    if (failedOps.size() > 0) {
      throw new WaitForCompletionException();
    }
  }

  public AsyncSyncRequest syncRequest(long version) throws SessionClosedException {
    return super.baseSyncRequest(version, this);
  }

  public AsyncSyncRequest syncRequest() throws SessionClosedException {
    return syncRequest(getAbsMillisTimeSource().absTimeMillis());
  }
}
