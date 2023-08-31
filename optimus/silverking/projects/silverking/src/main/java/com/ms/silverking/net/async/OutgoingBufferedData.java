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

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutgoingData used by QueueingConnection. Stores a list of ByteBuffers to be written to the
 * Connection.
 */
public class OutgoingBufferedData extends OutgoingData {
  private static final int maxLoggingBuffers = 15;
  private static final int maxLoggingLen = maxLoggingBuffers * 50;

  private final ByteBuffer[] buffers;
  private long totalBytes;
  private long bytesWritten;

  private static Logger log = LoggerFactory.getLogger(OutgoingBufferedData.class);

  private static final boolean displayContentsForDebug = false;

  public OutgoingBufferedData(
      ByteBuffer[] buffers,
      UUIDBase sendUUID,
      AsyncSendListener asyncSendListener,
      long deadline,
      Priority priority) {
    super(sendUUID, asyncSendListener, deadline, priority);
    this.buffers = buffers;
    for (ByteBuffer buffer : buffers) {
      totalBytes += buffer.remaining();
    }
    assert totalBytes != 0;
    if (AsyncGlobals.debug && log.isDebugEnabled()) {
      displayForDebug();
    }
  }

  public OutgoingBufferedData(
      ByteBuffer[] buffers, UUIDBase sendUUID, AsyncSendListener asyncSendListener, long deadline) {
    this(buffers, sendUUID, asyncSendListener, deadline, Priority.NORMAL);
  }

  @Override
  public long getTotalBytes() {
    return totalBytes;
  }

  public void displayForDebug() {
    log.info("OutgoingBufferedData buffers.size(): {}", buffers.length);
    log.info("OutgoingBufferedData content: {}", this.toString());
    for (int i = 0; i < Math.min(buffers.length, maxLoggingBuffers); i++) {
      ByteBuffer buffer = buffers[i];
      if (displayContentsForDebug) {
        log.info(StringUtil.byteArrayToHexString(buffer.array()));
      }
    }
    if (buffers.length > maxLoggingBuffers) {
      log.info("...... this OutgoingBufferedData has {} buffers", buffers.length);
    }
  }

  @Override
  public boolean writeToChannel(SocketChannel channel) throws IOException {
    if (AsyncGlobals.debug && log.isDebugEnabled()) {
      log.debug("writeToChannel {}", channel);
      displayForDebug();
    }
    bytesWritten += channel.write(buffers);
    if (AsyncGlobals.debug && log.isInfoEnabled()) {
      log.info("writeToChannel bytesWritten / totalbytes {} / {}", bytesWritten, totalBytes);
    }
    assert bytesWritten <= totalBytes;
    return bytesWritten == totalBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append("OutgoingBufferedData(sendUuid=").append(getSendUUID()).append("): ");
    sb.append(buffers.length);
    for (int i = 0; i < Math.min(buffers.length, maxLoggingBuffers); i++) {
      sb.append(' ');
      sb.append(buffers[i]);
      if (sb.length() > maxLoggingLen) {
        break;
      }
    }
    if (sb.length() > maxLoggingLen) {
      sb.delete(maxLoggingLen, sb.length());
      sb.append("......(").append(buffers.length).append(" buffers)");
    }
    return sb.toString();
  }
}
