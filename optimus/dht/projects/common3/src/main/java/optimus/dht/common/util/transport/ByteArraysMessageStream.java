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

import java.util.Queue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;
import io.netty.buffer.ByteBuf;

public class ByteArraysMessageStream implements MessageStream {

  private static final Logger logger = LoggerFactory.getLogger(ByteArraysMessageStream.class);

  private final Queue<byte[]> byteArrays;
  private final Consumer<SentMessageMetrics> metricsCallback;

  private long totalSize;

  private byte[] currentByteArray;
  private int positionInArray = 0;

  public ByteArraysMessageStream(Queue<byte[]> byteArrays) {
    this(byteArrays, null);
  }

  public ByteArraysMessageStream(
      Queue<byte[]> byteArrays, Consumer<SentMessageMetrics> metricsCallback) {
    this.byteArrays = byteArrays;
    this.metricsCallback = metricsCallback;
    totalSize = calculateTotalSize(byteArrays);
    currentByteArray = byteArrays.poll();
  }

  private static long calculateTotalSize(Queue<byte[]> byteArrays) {
    long size = 0;
    for (byte[] array : byteArrays) {
      size += array.length;
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
    while (writeableInBuffer > 0
        && ((positionInArray < currentByteArray.length) || (!byteArrays.isEmpty()))) {
      int toWriteFromCurrentArray =
          Math.min(currentByteArray.length - positionInArray, writeableInBuffer);
      buf.writeBytes(currentByteArray, positionInArray, toWriteFromCurrentArray);
      positionInArray += toWriteFromCurrentArray;
      writeableInBuffer -= toWriteFromCurrentArray;
      if (currentByteArray.length == positionInArray && !byteArrays.isEmpty()) {
        currentByteArray = byteArrays.poll();
        positionInArray = 0;
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
