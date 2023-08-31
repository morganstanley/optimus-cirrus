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
package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import com.ms.silverking.cloud.dht.throttle.SkThrottlingDebt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntermediateResult {
  private static final Logger log = LoggerFactory.getLogger(IntermediateResult.class);
  private ByteBuffer resultBuffer;
  private SkThrottlingDebt throttleDebt;

  private IntermediateResult(ByteBuffer resultBuffer, SkThrottlingDebt throttleDebt) {
    this.resultBuffer = resultBuffer;
    this.throttleDebt = throttleDebt;
  }

  public static IntermediateResult of(ByteBuffer resultBuffer, SkThrottlingDebt throttleDebt) {
    return of(resultBuffer, throttleDebt, "Untraced");
  }

  public static IntermediateResult of(
      ByteBuffer resultBuffer, SkThrottlingDebt throttleDebt, String traceId) {
    if (resultBuffer == null) {
      log.warn(
          "Tried to construct IntermediateResult for null result buffer for trace id: " + traceId,
          new NullPointerException("Suspicious code path"));
      return null;
    }
    return new IntermediateResult(resultBuffer, throttleDebt);
  }

  public ByteBuffer getResultBuffer() {
    return resultBuffer;
  }

  public SkThrottlingDebt getThrottleDebt() {
    return throttleDebt;
  }
}
