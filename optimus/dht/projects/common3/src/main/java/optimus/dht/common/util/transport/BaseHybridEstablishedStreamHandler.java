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

import optimus.dht.common.api.transport.CorruptedStreamException;
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Base class for established stream handlers that use hybrid protocol.
 *
 * <p>Hybrid protocol messages consist of two parts:
 *
 * <ul>
 *   <li>variable size header - header needs to be fully read before it can be processed
 *   <li>variable size payloads - payloads are read directly to their target destination
 * </ul>
 *
 * @param <T> class representing incoming message type
 * @param <S> class representing incoming message
 */
public abstract class BaseHybridEstablishedStreamHandler<T, S> implements EstablishedStreamHandler {

  public static final int PREHEADER_SIZE = 6;

  public enum State {
    PREHEADER,
    HEADER,
    PAYLOAD
  }

  private State state = State.PREHEADER;

  private ByteBuf preheaderBuf = Unpooled.buffer(PREHEADER_SIZE, PREHEADER_SIZE);
  protected T messageType;
  protected byte[] headerMsg;
  private int positionInHeader;

  protected Deque<RawStreamReader> payloadsToRead;
  private RawStreamReader currentPayload;

  protected S currentMessage;

  @Override
  public void dataReceived(ByteBuf buf) {
    while (buf.isReadable()) {
      switch (state) {
        case PREHEADER:
          processPreheader(buf);
          break;
        case HEADER:
          processHeader(buf);
          break;
        case PAYLOAD:
          processPayload(buf);
          break;
      }
    }
  }

  protected void processPreheader(ByteBuf buf) {
    ByteBuf activeReadBuf;
    if (preheaderBuf.isReadable()) {
      preheaderBuf.writeBytes(buf, Math.min(buf.readableBytes(), preheaderBuf.writableBytes()));
      activeReadBuf = preheaderBuf;
    } else {
      activeReadBuf = buf;
    }

    if (activeReadBuf.isReadable(PREHEADER_SIZE)) {
      int type = activeReadBuf.readShortLE();
      messageType = lookupMessageType(type);
      if (messageType == null) {
        throw new CorruptedStreamException("Unknown message type, type=" + type);
      }
      headerMsg = new byte[activeReadBuf.readIntLE()];
      positionInHeader = 0;
      preheaderBuf.clear();

      state = State.HEADER;

    } else {
      preheaderBuf.writeBytes(buf);
    }
  }

  protected abstract T lookupMessageType(int type);

  protected void processHeader(ByteBuf buf) {

    int toRead = Math.min(buf.readableBytes(), headerMsg.length - positionInHeader);
    buf.readBytes(headerMsg, positionInHeader, toRead);
    positionInHeader += toRead;

    if (positionInHeader == headerMsg.length) {
      processMessageHeader();
      headerMsg = null;
      positionInHeader = 0;
      if (payloadsToRead != null && !payloadsToRead.isEmpty()) {
        currentPayload = payloadsToRead.poll();
        state = State.PAYLOAD;
      } else {
        state = State.PREHEADER;
      }
    }
  }

  protected abstract void processMessageHeader();

  protected void processPayload(ByteBuf buf) {
    while (buf.isReadable() && currentPayload != null) {
      int toRead =
          Math.min(
              buf.readableBytes(), (int) Math.min(Integer.MAX_VALUE, currentPayload.remaining()));
      currentPayload.read(buf, toRead);

      if (currentPayload.remaining() == 0) {
        currentPayload = payloadsToRead.poll();
      }
    }
  }

  @Override
  public final void messageCompleted(ReceivedMessageMetrics metrics) {
    state = State.PREHEADER;
    resetPayloads();
    processMessageCompleted(metrics);
  }

  protected void resetPayloads() {
    payloadsToRead = null;
    currentPayload = null;
  }

  protected abstract void processMessageCompleted(ReceivedMessageMetrics metrics);
}
