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
package optimus.dht.common.internal.transport;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ConnectionCountingChannelHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger =
      LoggerFactory.getLogger(ConnectionCountingChannelHandler.class);

  private final ChannelSharedState sharedState;
  private final AtomicInteger connectionsCount;

  private boolean wasActive = false;

  public ConnectionCountingChannelHandler(
      ChannelSharedState sharedState, AtomicInteger connectionsCount) {
    this.sharedState = sharedState;
    this.connectionsCount = connectionsCount;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.info(
        "Connection established, openConnections={}, addr={}, remote={}, codeVersions={}, protocolVersions={}, auth={}",
        connectionsCount.incrementAndGet(),
        ctx.channel().remoteAddress(),
        sharedState.remoteInstanceIdentity(),
        sharedState.remoteCodeVersions(),
        sharedState.negotiatedProtocolVersions(),
        sharedState.authenticatedConnection());
    wasActive = true;
    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (wasActive) {
      logger.info(
          "Connection closed, openConnections={}, addr={}, remote={}",
          connectionsCount.decrementAndGet(),
          ctx.channel().remoteAddress(),
          sharedState.remoteInstanceIdentity());
    } else {
      logger.warn(
          "Connection closed, but it was never active, addr={}, remote={}",
          ctx.channel().remoteAddress(),
          sharedState.remoteInstanceIdentity());
    }
    ctx.fireChannelInactive();
  }
}
