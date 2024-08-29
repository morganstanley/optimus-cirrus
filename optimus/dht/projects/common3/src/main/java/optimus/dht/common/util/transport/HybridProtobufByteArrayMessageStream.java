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
package optimus.dht.common.util.transport;

import java.util.Deque;
import java.util.Queue;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.MessageLite;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;

public class HybridProtobufByteArrayMessageStream extends BaseHybridProtobufMessageStream {

  private final Deque<byte[]> mutablePayloads;

  private MessageStream wrappedStream;

  public HybridProtobufByteArrayMessageStream(
      short typeId, MessageLite protoMessage, Deque<byte[]> mutablePayloads) {
    this(typeId, protoMessage, mutablePayloads, null);
  }

  public HybridProtobufByteArrayMessageStream(
      short typeId,
      MessageLite protoMessage,
      Deque<byte[]> mutablePayloads,
      Consumer<SentMessageMetrics> metricsCallback) {
    super(
        typeId,
        protoMessage,
        headerSize(protoMessage) + calculateArraysSize(mutablePayloads),
        metricsCallback);
    this.mutablePayloads = mutablePayloads;
  }

  private static long calculateArraysSize(Queue<byte[]> byteArrays) {
    long size = 0;
    for (byte[] byteArray : byteArrays) {
      size += byteArray.length;
    }
    return size;
  }

  @VisibleForTesting
  public Deque<byte[]> payloads() {
    return mutablePayloads;
  }

  @Override
  protected MessageStream createWrappedStream(byte[] header) {
    mutablePayloads.addFirst(header);
    return new ByteArraysMessageStream(mutablePayloads);
  }
}
