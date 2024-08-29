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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseHybridProtobufMessageStream implements MessageStream {

  private static final Logger logger =
      LoggerFactory.getLogger(HybridProtobufByteArrayMessageStream.class);

  private final short typeId;
  private final MessageLite protoMessage;
  private final long size;

  private final Consumer<SentMessageMetrics> metricsCallback;

  private MessageStream wrappedStream;

  public BaseHybridProtobufMessageStream(
      short typeId,
      MessageLite protoMessage,
      long size,
      Consumer<SentMessageMetrics> metricsCallback) {
    this.typeId = typeId;
    this.protoMessage = protoMessage;
    this.size = size;
    this.metricsCallback = metricsCallback;
  }

  protected static int headerSize(MessageLite protoMessage) {
    return BaseHybridEstablishedStreamHandler.PREHEADER_SIZE + protoMessage.getSerializedSize();
  }

  private static long calculateBuffersSize(Queue<ByteBuffer> byteBuffers) {
    long size = 0;
    for (ByteBuffer byteBuffer : byteBuffers) {
      size += byteBuffer.remaining();
    }
    return size;
  }

  @VisibleForTesting
  public MessageLite protoMessage() {
    return protoMessage;
  }

  @Override
  public long size() {
    return size;
  }

  protected abstract MessageStream createWrappedStream(byte[] header);

  @Override
  public void messageStart() {
    int serializedSize = protoMessage.getSerializedSize();
    byte[] msg = new byte[BaseHybridEstablishedStreamHandler.PREHEADER_SIZE + serializedSize];
    ByteBuffer byteBuffer =
        ByteBuffer.wrap(msg).order(ByteOrder.LITTLE_ENDIAN).putShort(typeId).putInt(serializedSize);

    CodedOutputStream codedOutputStream =
        CodedOutputStream.newInstance(
            msg, BaseHybridEstablishedStreamHandler.PREHEADER_SIZE, serializedSize);
    try {
      protoMessage.writeTo(codedOutputStream);
    } catch (IOException e) {
      throw new RuntimeException("IOException while writing to memory!", e);
    }
    wrappedStream = createWrappedStream(msg);
    wrappedStream.messageStart();
  }

  @Override
  public void generate(ByteBuf buf, int writeable) {
    wrappedStream.generate(buf, writeable);
  }

  @Override
  public void messageSent(SentMessageMetrics metrics) {
    wrappedStream.messageSent(metrics);
    if (metricsCallback != null) {
      try {
        metricsCallback.accept(metrics);
      } catch (Exception e) {
        logger.warn("Exception caught in metrics callback", e);
      }
    }
  }

  @Override
  public void messageFailed(Throwable throwable) {
    if (wrappedStream != null) {
      wrappedStream.messageFailed(throwable);
    }
  }
}
