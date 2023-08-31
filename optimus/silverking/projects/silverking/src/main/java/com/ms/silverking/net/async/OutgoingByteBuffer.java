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
package com.ms.silverking.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OutgoingData wrapper for ByteBuffers. */
public class OutgoingByteBuffer extends OutgoingData {
  private final ByteBuffer buffer;
  private AtomicLong totalWritten;

  private static final int maxLoggingLen = 15 * 50;

  private static Logger log = LoggerFactory.getLogger(OutgoingByteBuffer.class);

  public OutgoingByteBuffer(
      ByteBuffer buffer, UUIDBase sendUUID, AsyncSendListener asyncSendListener, long deadline) {
    super(sendUUID, asyncSendListener, deadline);
    this.buffer = buffer;
  }

  public static OutgoingByteBuffer wrap(byte[] buf) {
    return new OutgoingByteBuffer(ByteBuffer.wrap(buf), null, null, Long.MAX_VALUE);
  }

  public static OutgoingByteBuffer wrap(
      byte[] buf, UUIDBase sendUUID, AsyncSendListener asyncSendListener) {
    return new OutgoingByteBuffer(
        ByteBuffer.wrap(buf), sendUUID, asyncSendListener, Long.MAX_VALUE);
  }

  public static OutgoingByteBuffer allocate(
      int capacity, UUIDBase sendUUID, AsyncSendListener asyncSendListener) {
    return new OutgoingByteBuffer(
        ByteBuffer.allocate(capacity), sendUUID, asyncSendListener, Long.MAX_VALUE);
  }

  public long getTotalBytes() {
    return buffer.array().length;
  }

  @Override
  public boolean writeToChannel(SocketChannel channel) throws IOException {
    int written;

    written = channel.write(buffer);
    totalWritten.addAndGet(written);
    if (AsyncGlobals.debug && log.isDebugEnabled()) {
      log.debug("written to channel: {} {}", written, totalWritten.get());
    }
    return buffer.remaining() == 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    String content = StringUtil.byteArrayToHexString(buffer.array());

    sb.append("OutgoingByteBuffer(sendUuid");
    sb.append(getSendUUID());
    sb.append("):");
    sb.append(content, 0, Math.min(content.length(), maxLoggingLen));

    return sb.toString();
  }

  public void displayForDebug() {
    log.debug("buffer.remaining() {}", buffer.remaining());
    log.debug("{}", this.toString());
  }
}
