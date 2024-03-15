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

public class MessageGroupRetrievalResponseEntry extends MessageGroupKVEntry {

  public MessageGroupRetrievalResponseEntry(
      ByteBuffer keyBuffer, int offset, ByteBuffer[] buffers) {
    super(keyBuffer, offset);
    storedLength = keyBuffer.getInt(offset + RetrievalResponseMessageFormat.resultLengthOffset);
    initValBuffer(buffers);
  }

  public int entryLength() {
    return RetrievalResponseMessageFormat.size;
  }

  public OpResult getOpResult() {
    return hasValue() ? OpResult.SUCCEEDED : EnumValues.opResult[getErrorCode()];
  }
}
