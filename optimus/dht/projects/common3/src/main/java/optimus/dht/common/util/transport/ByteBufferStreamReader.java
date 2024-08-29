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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.LoggerFactory;

public class ByteBufferStreamReader implements RawStreamReader {

  private final ByteBuffer buffer;
  private final ByteBuf writeBuffer;

  public ByteBufferStreamReader(ByteBuffer buffer) {
    this.buffer = buffer;
    this.writeBuffer = Unpooled.wrappedBuffer(buffer);
    writeBuffer.writerIndex(0);
  }

  @Override
  public long size() {
    return buffer.capacity();
  }

  @Override
  public long remaining() {
    return writeBuffer.writableBytes();
  }

  @Override
  public void read(ByteBuf buf, int length) {
    if (!writeBuffer.isWritable()) {
      throw new IllegalStateException("Buffer was already fully read");
    }
    int toRead = Math.min(writeBuffer.writableBytes(), length);
    buf.readBytes(writeBuffer, toRead);
  }
}
