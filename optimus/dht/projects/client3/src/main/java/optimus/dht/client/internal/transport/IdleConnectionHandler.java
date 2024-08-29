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
package optimus.dht.client.internal.transport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.IdleConnectionStrategy;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.internal.coreprotocol.CoreProtocolV1MessageType;
import optimus.dht.common.util.transport.ByteArrayMessageStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class receives idle connection notifications, and acts upon them.
 *
 * <p>Specifically, we had issues on long running idle connection in the cloud environments.
 * Periodically sending some data ensures that connection won't be forgotten by a stateful firewall.
 */
public class IdleConnectionHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(IdleConnectionHandler.class);

  private final ServerConnection serverConnection;
  private final DefaultConnectionsManager.ConnectedHandle handle;
  private final IdleConnectionStrategy idleConnectionStrategy;

  private int counter = 0;

  public IdleConnectionHandler(
      ServerConnection serverConnection,
      DefaultConnectionsManager.ConnectedHandle handle,
      IdleConnectionStrategy idleConnectionStrategy) {
    this.serverConnection = serverConnection;
    this.handle = handle;
    this.idleConnectionStrategy = idleConnectionStrategy;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      logger.trace(
          "Connection to server="
              + serverConnection
              + " was idle, applying actionType="
              + idleConnectionStrategy.actionType());
      switch (idleConnectionStrategy.actionType()) {
        case UNIDIRECTIONAL_HEARTBEAT:
          handle.addMessage((short) 0, new HeartbeatMessageGenerator(), null);
          handle.flush();
          break;
        case BIDIRECTIONAL_PING:
          handle.addMessage((short) 0, new PingRequestMessageGenerator(counter++), null);
          handle.flush();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported actionType " + idleConnectionStrategy.actionType());
      }
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }

  static class HeartbeatMessageGenerator implements MessageGenerator {

    @Override
    public MessageStream build(int protocolVersion) {
      switch (protocolVersion) {
        case 1:
          return new ByteArrayMessageStream(new byte[] {CoreProtocolV1MessageType.HEARTBEAT.id()});
        default:
          throw new IllegalArgumentException("Unsupported protocol version " + protocolVersion);
      }
    }

    @Override
    public long estimatedSize() {
      return 1;
    }
  }

  static class PingRequestMessageGenerator implements MessageGenerator {

    private final int sequenceNr;

    public PingRequestMessageGenerator(int sequenceNr) {
      this.sequenceNr = sequenceNr;
    }

    @Override
    public MessageStream build(int protocolVersion) {
      switch (protocolVersion) {
        case 1:
          ByteBuffer byteBuffer = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
          byteBuffer.put(CoreProtocolV1MessageType.PING_REQUEST.id());
          byteBuffer.putInt(sequenceNr);
          return new ByteArrayMessageStream(byteBuffer.array());
        default:
          throw new IllegalArgumentException("Unsupported protocol version " + protocolVersion);
      }
    }

    @Override
    public long estimatedSize() {
      return 5;
    }
  }
}
