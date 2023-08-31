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
package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.time.AbsMillisTimeSource;

/** Provides functionality common to all operations, */
public abstract class BaseOperation<S> {
  // entryStateMap is only written to during creation
  // Hence, no concurrency control around reads
  private final Map<DHTKey, S> entryStateMap;
  private final long deadline;
  private final long minInternalAbsTimeoutMillis;
  protected final int numEntries;
  protected AtomicInteger completeEntries;
  // Concurrency controlled through synchronization on this
  private OpResult wholeOpResult;

  protected final OperationContainer operationContainer;
  protected final ForwardingMode forwardingMode;

  protected static final boolean debug = false;

  private static final int capacityFactor = 2;

  protected static AbsMillisTimeSource absMillisTimeSource;

  public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
    absMillisTimeSource = _absMillisTimeSource;
  }

  public BaseOperation(
      long deadline,
      OperationContainer operationContainer,
      ForwardingMode forwardingMode,
      int minInternalRelTimeoutMillis,
      int numEntries) {
    this.deadline = deadline;
    this.operationContainer = operationContainer;
    this.forwardingMode = forwardingMode;
    this.minInternalAbsTimeoutMillis =
        absMillisTimeSource.absTimeMillis() + minInternalRelTimeoutMillis;
    entryStateMap = new HashMap<>(numEntries * capacityFactor);
    this.numEntries = numEntries;
    completeEntries = new AtomicInteger();
    wholeOpResult = OpResult.INCOMPLETE;
  }

  public long getDeadline() {
    return deadline;
  }

  public long getMinInternalTimeoutMillis() {
    return minInternalAbsTimeoutMillis;
  }

  public S getEntryState(DHTKey key) {
    if (debug) {
      System.out.printf("getEntryState %s\t%s\n", key, entryStateMap.get(key));
    }
    return entryStateMap.get(key);
  }

  public void setEntryState(DHTKey key, S state) {
    if (debug) {
      System.out.printf("setEntryState %s\t%s\n", key, state);
    }
    entryStateMap.put(key, state);
  }

  protected Collection<DHTKey> opKeys() {
    return entryStateMap.keySet();
  }

  public OpResult getOpResult() {
    return wholeOpResult;
  }

  protected void setOpResult(OpResult wholeOpResult) {
    synchronized (this) {
      if (!this.wholeOpResult.isComplete()) {
        this.wholeOpResult = wholeOpResult;
      }
    }
  }
}
