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
package com.ms.silverking.cloud.dht.throttle;

import java.nio.ByteBuffer;
import com.ms.silverking.numeric.NumConversion;

public class SkThrottlingDebt {
  private int keysRead;
  private int cacheHits;
  private int computeHits;

  public SkThrottlingDebt(int keysRead, int cacheHits, int computeHits) {
    this.keysRead = keysRead;
    this.cacheHits = cacheHits;
    this.computeHits = computeHits;
  }

  public static SkThrottlingDebt noDebt() {
    return new SkThrottlingDebt(0, 0, 0);
  }

  public SkThrottlingDebt combine(SkThrottlingDebt newDebt) {
    return new SkThrottlingDebt(
        keysRead + newDebt.getKeysRead(),
        cacheHits + newDebt.getCacheHits(),
        computeHits + newDebt.getComputeHits());
  }

  public int getKeysRead() {
    return keysRead;
  }

  public int getCacheHits() {
    return cacheHits;
  }

  public int getComputeHits() {
    return computeHits;
  }

  public static ByteBuffer serialize(SkThrottlingDebt skThrottlingDebt) {
    int totalDebtBytes =
        NumConversion.BYTES_PER_INT * 3; // results read, cache hits & read throughs.
    ByteBuffer debtBuffer = ByteBuffer.allocate(totalDebtBytes);
    debtBuffer.putInt(skThrottlingDebt.getKeysRead());
    debtBuffer.putInt(skThrottlingDebt.getCacheHits());
    debtBuffer.putInt(skThrottlingDebt.getComputeHits());
    return debtBuffer;
  }

  public static SkThrottlingDebt deserialize(ByteBuffer debtBuffer) {
    int resultsReadIndex = NumConversion.BYTES_PER_INT;
    int cacheHitsIndex = resultsReadIndex + NumConversion.BYTES_PER_INT;
    int readThroughsIndex = cacheHitsIndex + NumConversion.BYTES_PER_INT;

    return new SkThrottlingDebt(
        debtBuffer.getInt(resultsReadIndex),
        debtBuffer.getInt(cacheHitsIndex),
        debtBuffer.getInt(readThroughsIndex));
  }
}
