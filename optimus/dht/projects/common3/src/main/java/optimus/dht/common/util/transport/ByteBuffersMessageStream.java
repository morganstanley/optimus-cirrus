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

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBuffersMessageStream implements MessageStream {

  private static final Logger logger = LoggerFactory.getLogger(ByteArraysMessageStream.class);

  private final Queue<ByteBuffer> byteBuffers;
  private final Consumer<SentMessageMetrics> metricsCallback;

  private long totalSize;

  private ByteBuffer currentByteBuffer;

  public ByteBuffersMessageStream(Queue<ByteBuffer> byteBuffers) {
    this(byteBuffers, null);
  }

  public ByteBuffersMessageStream(
      Queue<ByteBuffer> byteBuffers, Consumer<SentMessageMetrics> metricsCallback) {
    this.byteBuffers = byteBuffers;
    this.metricsCallback = metricsCallback;
    totalSize = calculateTotalSize(byteBuffers);
    currentByteBuffer = byteBuffers.poll();
  }

  private static long calculateTotalSize(Queue<ByteBuffer> byteBuffers) {
    long size = 0;
    for (ByteBuffer buffer : byteBuffers) {
      size += buffer.remaining();
    }
    return size;
  }

  @Override
  public long size() {
    return totalSize;
  }

  @Override
  public void generate(ByteBuf buf, int writeable) {
    int writeableInBuffer = writeable;
    while (writeableInBuffer > 0 && (currentByteBuffer.hasRemaining() || !byteBuffers.isEmpty())) {
      int toWriteFromCurrentBuffer = Math.min(currentByteBuffer.remaining(), writeableInBuffer);
      buf.writeBytes(Unpooled.wrappedBuffer(currentByteBuffer), toWriteFromCurrentBuffer);
      currentByteBuffer.position(currentByteBuffer.position() + toWriteFromCurrentBuffer);
      writeableInBuffer -= toWriteFromCurrentBuffer;
      if (!currentByteBuffer.hasRemaining() && !byteBuffers.isEmpty()) {
        currentByteBuffer = byteBuffers.poll();
      }
    }
  }

  @Override
  public void messageSent(SentMessageMetrics metrics) {
    if (metricsCallback != null) {
      try {
        metricsCallback.accept(metrics);
      } catch (Exception e) {
        logger.warn("Exception caught in metrics callback", e);
      }
    }
  }
}
