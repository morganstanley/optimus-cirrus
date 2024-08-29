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
package optimus.dht.client.internal.coreprotocol;

import optimus.dht.common.api.transport.CorruptedStreamException;
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import optimus.dht.common.internal.coreprotocol.CoreProtocolV1MessageType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class CoreProtocolEstablishedStreamHandlerV1 implements EstablishedStreamHandler {

  private final ByteBuf readBuf = Unpooled.buffer(4, 4);

  public CoreProtocolEstablishedStreamHandlerV1() {}

  @Override
  public void dataReceived(ByteBuf buf) {
    while (buf.isReadable()) {
      if (readBuf.isReadable()) {
        int toRead = Math.min(buf.readableBytes(), readBuf.writableBytes());
        readBuf.writeBytes(buf, toRead);
        if (readBuf.isReadable(4)) {
          handlePingReply(readBuf.readIntLE());
          readBuf.clear();
        }
      } else {
        byte messageType = buf.readByte();
        switch (CoreProtocolV1MessageType.fromId(messageType)) {
          case HEARTBEAT:
            // NOOP - heartbeat
            break;
          case PING_REPLY:
            if (buf.isReadable(4)) {
              handlePingReply(buf.readIntLE());
            } else if (buf.isReadable()) {
              readBuf.writeBytes(buf);
            }
            break;
          default:
            throw new CorruptedStreamException(
                "Received unknown core message type: " + messageType);
        }
      }
    }
  }

  private void handlePingReply(int sequenceNr) {
    // currently NOOP
  }
}
