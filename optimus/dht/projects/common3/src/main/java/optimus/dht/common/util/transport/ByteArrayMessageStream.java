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

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;
import io.netty.buffer.ByteBuf;

public class ByteArrayMessageStream implements MessageStream {

  private static final Logger logger = LoggerFactory.getLogger(ByteArrayMessageStream.class);

  private final byte[] byteArray;
  private final Consumer<SentMessageMetrics> metricsCallback;

  private int arrayPosition = 0;

  public ByteArrayMessageStream(byte[] byteArray) {
    this(byteArray, null);
  }

  public ByteArrayMessageStream(byte[] byteArray, Consumer<SentMessageMetrics> metricsCallback) {
    this.byteArray = byteArray;
    this.metricsCallback = metricsCallback;
  }

  @Override
  public long size() {
    return byteArray.length;
  }

  private long remaining() {
    return size() - arrayPosition;
  }

  @Override
  public void generate(ByteBuf buf, int writeable) {
    // arrays cannot have more then 1<<31 elements, so this cast is safe
    int toWrite = (int) Math.min(remaining(), writeable);
    buf.writeBytes(byteArray, arrayPosition, toWrite);
    arrayPosition += toWrite;
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
