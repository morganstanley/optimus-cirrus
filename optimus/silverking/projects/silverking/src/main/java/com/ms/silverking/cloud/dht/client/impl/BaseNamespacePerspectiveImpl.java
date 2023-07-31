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

import static com.ms.silverking.cloud.dht.common.OpResult.SESSION_CLOSED;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSnapshot;
import com.ms.silverking.cloud.dht.client.AsyncSyncRequest;
import com.ms.silverking.cloud.dht.client.BaseNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SessionClosedException;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.client.impl.ClientNamespace.OpLWTMode;
import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.time.AbsMillisTimeSource;

class BaseNamespacePerspectiveImpl<K, V> implements BaseNamespacePerspective<K, V> {
  protected final ClientNamespace clientNamespace;
  protected volatile NamespacePerspectiveOptionsImpl<K, V> nspoImpl;

  BaseNamespacePerspectiveImpl(ClientNamespace clientNamespace, String name,
      NamespacePerspectiveOptionsImpl<K, V> nspoImpl) {
    this.clientNamespace = clientNamespace;
    if (nspoImpl.getDefaultPutOptions() == null) {
      nspoImpl = nspoImpl.defaultPutOptions(clientNamespace.getOptions().getDefaultPutOptions());
    }
    if (nspoImpl.getDefaultGetOptions() == null) {
      nspoImpl = nspoImpl.defaultGetOptions(clientNamespace.getOptions().getDefaultGetOptions());
    }
    if (nspoImpl.getDefaultWaitOptions() == null) {
      nspoImpl = nspoImpl.defaultWaitOptions(clientNamespace.getOptions().getDefaultWaitOptions());
    }
    this.nspoImpl = nspoImpl;
  }

  @Override
  public String getName() {
    return clientNamespace.getName();
  }

  @Override
  public Namespace getNamespace() {
    return clientNamespace;
  }

  @Override
  public NamespacePerspectiveOptions<K, V> getOptions() {
    return nspoImpl.getNSPOptions();
  }

  @Override
  public void setOptions(NamespacePerspectiveOptions<K, V> nspOptions) {
    nspoImpl = nspoImpl.namespacePerspectiveOptions(nspOptions);
  }

  @Override
  public void setDefaultRetrievalVersionConstraint(VersionConstraint vc) {
    NamespacePerspectiveOptions<K, V> nspOptions;

    nspOptions = nspoImpl.getNSPOptions();
    nspOptions = nspOptions.defaultGetOptions(nspOptions.getDefaultGetOptions().versionConstraint(vc));
    nspOptions = nspOptions.defaultWaitOptions(nspOptions.getDefaultWaitOptions().versionConstraint(vc));
    setOptions(nspOptions);
  }

  public void setDefaultVersionProvider(VersionProvider versionProvider) {
    setOptions(nspoImpl.getNSPOptions().defaultVersionProvider(versionProvider));
  }

  public void setDefaultVersion(long version) {
    setOptions(nspoImpl.getNSPOptions().defaultVersionProvider(new ConstantVersionProvider(version)));
  }

  public AbsMillisTimeSource getAbsMillisTimeSource() {
    return clientNamespace.getAbsMillisTimeSource();
  }

  @Override
  public void close() {
  }

  // reads

  public AsyncRetrieval<K, V> baseRetrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions,
      OpLWTMode opLWTMode) {
    AsyncRetrievalOperationImpl<K, V> opImpl;

    clientNamespace.validateOpOptions(retrievalOptions);
    opImpl = new AsyncRetrievalOperationImpl(new RetrievalOperation<>(clientNamespace, keys, retrievalOptions),
        clientNamespace, nspoImpl, clientNamespace.getAbsMillisTimeSource().absTimeMillis(),
        clientNamespace.getOriginator());
    try {
      clientNamespace.startOperation(opImpl, opLWTMode);
    } catch (SessionClosedException ex) {
      opImpl.setResult(SESSION_CLOSED);
    }
    return opImpl;
  }

  // writes

  public AsyncPut<K> basePut(Map<? extends K, ? extends V> values, PutOptions putOptions,
      AsynchronousNamespacePerspectiveImpl<K, V> asynchronousNSPerspectiveImpl, OpLWTMode opLWTMode) {
    AsyncPutOperationImpl<K, V> opImpl;

        /*
        if (clientNamespace.getOptions() != null) {
            if (clientNamespace.getOptions().getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
                putOptions = putOptions.version(clientNamespace.getAbsMillisTimeSource().absTimeMillis());
            } else if (clientNamespace.getOptions().getVersionMode().isSystemSpecified()) {
                if (putOptions.getVersion() == PutOptions.defaultVersion) {
                    putOptions = putOptions.version(DHTConstants.unspecifiedVersion);
                }
            } else {
                if (putOptions.getVersion() == PutOptions.defaultVersion) {
                    putOptions = putOptions.version(nspoImpl.getNSPOptions().getDefaultVersionProvider().getVersion());
                }
            }
        }
        */

    clientNamespace.validateOpOptions(putOptions);
    clientNamespace.validatePutOptions(putOptions);
    opImpl = new AsyncPutOperationImpl<>(new PutOperation<>(clientNamespace, values, putOptions), clientNamespace,
        nspoImpl, clientNamespace.getAbsMillisTimeSource().absTimeMillis(), clientNamespace.getOriginator(),
        nspoImpl.getNSPOptions().getDefaultVersionProvider());
    try {
      clientNamespace.startOperation(opImpl, opLWTMode);
    } catch (SessionClosedException ex) {
      opImpl.setResult(SESSION_CLOSED);
    }
    return opImpl;
  }

  public AsyncPut<K> basePut(Map<? extends K, ? extends V> values, PutOptions putOptions, OpLWTMode opLWTMode)
      throws PutException {
    return basePut(values, putOptions, null, opLWTMode);
  }

  public AsyncInvalidation<K> baseInvalidation(Set<? extends K> keys, InvalidationOptions invalidationOptions,
      BufferDestSerializer<V> valueSerializer, OpLWTMode oplwtmode) {
    ImmutableMap.Builder<K, V> values;

    clientNamespace.validateOpOptions(invalidationOptions);
    values = ImmutableMap.builder();
    for (K k : keys) {
      values.put(k, valueSerializer.emptyObject());
    }
    return (AsyncInvalidation<K>) basePut(values.build(), invalidationOptions, null, oplwtmode);
  }

  // snapshots

  protected AsyncSnapshot baseSnapshot(long version,
      AsynchronousNamespacePerspectiveImpl<K, V> asynchronousNSPerspectiveImpl) throws SessionClosedException {
    SnapshotOperation snapshotOperation;
    AsyncSnapshotOperationImpl asyncSnapshotImpl;

    snapshotOperation = new SnapshotOperation(clientNamespace, version);
    // TODO (OPTIMUS-0000): Snapshot is not supported in a server who enables traceID feature
    clientNamespace.validateOpOptions(snapshotOperation.options);
    asyncSnapshotImpl = new AsyncSnapshotOperationImpl(snapshotOperation, clientNamespace.getContext(),
        clientNamespace.getAbsMillisTimeSource().absTimeMillis(), clientNamespace.getOriginator());
    clientNamespace.getActiveVersionedBasicOperations().addOp(asyncSnapshotImpl);
    clientNamespace.startOperation(asyncSnapshotImpl, OpLWTMode.DisallowUserThreadUsage);
    return asyncSnapshotImpl;
  }

  // syncRequest

  protected AsyncSyncRequest baseSyncRequest(long version,
      AsynchronousNamespacePerspectiveImpl<K, V> asynchronousNSPerspectiveImpl) throws SessionClosedException {
    SyncRequestOperation syncRequestOperation;
    AsyncSyncRequestOperationImpl asyncSyncRequestOperationImpl;

    syncRequestOperation = new SyncRequestOperation(clientNamespace, version);
    // TODO (OPTIMUS-0000): SyncRequest is not supported in a server who enables traceID feature
    clientNamespace.validateOpOptions(syncRequestOperation.options);
    asyncSyncRequestOperationImpl = new AsyncSyncRequestOperationImpl(syncRequestOperation,
        clientNamespace.getContext(), clientNamespace.getAbsMillisTimeSource().absTimeMillis(),
        clientNamespace.getOriginator());
    clientNamespace.getActiveVersionedBasicOperations().addOp(asyncSyncRequestOperationImpl);
    clientNamespace.startOperation(asyncSyncRequestOperationImpl, OpLWTMode.DisallowUserThreadUsage);
    return asyncSyncRequestOperationImpl;
  }

  // misc.

  @Override
  public String toString() {
    return clientNamespace.toString() + ":" + nspoImpl;
  }
}
