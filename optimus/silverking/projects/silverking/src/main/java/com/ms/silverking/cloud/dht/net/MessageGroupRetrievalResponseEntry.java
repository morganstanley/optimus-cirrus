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
package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;
import com.ms.silverking.cloud.dht.throttle.SkThrottlingDebt;

public class MessageGroupRetrievalResponseEntry extends MessageGroupKVEntry {

  private SkThrottlingDebt totalDebt;
  private int debtIndex;
  private String debtProperty = "silverking.mg.debtEnabled";

  public MessageGroupRetrievalResponseEntry(
      ByteBuffer keyBuffer, int offset, ByteBuffer[] buffers) {
    super(keyBuffer, offset);
    storedLength = keyBuffer.getInt(offset + RetrievalResponseMessageFormat.resultLengthOffset);
    initValBuffer(buffers);
    initialiseTotalDebt(buffers);
  }

  public void initialiseTotalDebt(ByteBuffer[] buffers) {
    // TODO (OPTIMUS-42080): debtIndex cannot be assigned a fixed position since buffers are added
    // in inheritance order. ProtoValueMG is the very last level so this index works for now.
    boolean debtEnabled = Boolean.getBoolean(debtProperty);
    if (debtEnabled) {
      debtIndex = buffers.length - 1;
      ByteBuffer debtBuffer = buffers[debtIndex];
      totalDebt = SkThrottlingDebt.deserialize(debtBuffer);
    } else {
      totalDebt = SkThrottlingDebt.noDebt();
    }
  }

  public SkThrottlingDebt getTotalDebt() {
    return totalDebt;
  }

  public int entryLength() {
    return RetrievalResponseMessageFormat.size;
  }

  public OpResult getOpResult() {
    return hasValue() ? OpResult.SUCCEEDED : EnumValues.opResult[getErrorCode()];
  }
}
