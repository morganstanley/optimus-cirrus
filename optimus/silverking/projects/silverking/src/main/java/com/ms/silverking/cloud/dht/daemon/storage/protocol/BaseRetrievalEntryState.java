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

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.PropertiesHelper;

public abstract class BaseRetrievalEntryState {
  protected static AbsMillisTimeSource absMillisTimeSource;

  private long nextTimeoutAbsMillis;
  private static final int relTimeoutMillis =
      PropertiesHelper.systemHelper.getInt(DHTConstants.internalRelTimeoutMillisProp, 100);
  static final int minRelTimeoutMillis = relTimeoutMillis;

  public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
    absMillisTimeSource = _absMillisTimeSource;
  }

  public BaseRetrievalEntryState() {
    computeNextReplicaTimeout();
  }

  public abstract IPAndPort getInitialReplica();

  public abstract IPAndPort currentReplica();

  public abstract IPAndPort nextReplica();

  public abstract boolean isComplete();

  public abstract boolean prevReplicaSameAsCurrent();

  protected void computeNextReplicaTimeout() {
    nextTimeoutAbsMillis =
        relTimeoutMillis >= 0
            ? absMillisTimeSource.absTimeMillis() + relTimeoutMillis
            : Long.MAX_VALUE;
  }

  // timeout
  public boolean hasTimedOut(long curTimeMillis) {
    return !isComplete() && curTimeMillis > nextTimeoutAbsMillis;
  }
}
