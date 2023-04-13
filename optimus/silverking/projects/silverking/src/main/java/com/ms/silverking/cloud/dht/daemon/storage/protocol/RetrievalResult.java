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

import java.nio.ByteBuffer;
import java.util.Collection;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.serverside.IntermediateResult;
import com.ms.silverking.cloud.dht.throttle.SkThrottlingDebt;

public class RetrievalResult extends KeyedResult {
  private final ByteBuffer value;
  private final SkThrottlingDebt debt;

  public RetrievalResult(DHTKey key, OpResult result, ByteBuffer value, SkThrottlingDebt debt) {
    super(key, result);
    this.value = value;
    this.debt = debt;
  }

  public RetrievalResult(DHTKey key, OpResult opResult, IntermediateResult interResult) {
    super(key, opResult);
    if (interResult != null) {
      this.value = interResult.getResultBuffer();
      this.debt = interResult.getThrottleDebt();
    } else {
      this.value = null;
      this.debt = null;
    }
  }

  public ByteBuffer getValue() {
    return value;
  }

  public SkThrottlingDebt getDebt() {
    return debt;
  }

  public int getResultLength() {
    return value != null ? value.limit() : 0;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + value;
  }

  public static int totalResultLength(Collection<RetrievalResult> results) {
    int totalResultLength;

    totalResultLength = 0;
    for (RetrievalResult retrievalResult : results) {
      //System.out.printf("%s\t%d\n", retrievalResult, retrievalResult.getResultLength());
      totalResultLength += retrievalResult.getResultLength();
    }
    return totalResultLength;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RetrievalResult) {
      RetrievalResult other = (RetrievalResult) obj;

      return (this.getKey().equals(other.getKey()) && this.getResult().equals(
          other.getResult()) && ((this.value == null && other.value == null) || this.value.equals(other.value)));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.getKey().hashCode() ^ this.getResult().hashCode() ^ this.value.hashCode();
  }
}
