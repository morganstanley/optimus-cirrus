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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import optimus.dht.client.api.ClientIdentity;
import optimus.dht.client.api.transport.ClientInitialStreamHandler;
import optimus.dht.client.api.transport.ClientProtocolVersionHandler;
import optimus.dht.client.internal.transport.DefaultConnectionsManager.ClientChannelSharedState;
import optimus.dht.common.api.RemoteInstanceIdentity;
import optimus.dht.common.api.transport.CloseableStreamHandler;
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import optimus.dht.common.api.transport.InitialExchangeException;
import optimus.dht.common.api.transport.RemoteConnectionDetails;
import optimus.dht.common.internal.ProvidedRemoteInstanceIdentity;
import optimus.dht.common.internal.transport.generated.InitialProto2.InitialClientMessage;
import optimus.dht.common.internal.transport.generated.InitialProto2.InitialServerMessage;
import optimus.dht.common.internal.transport.generated.InitialProto2.ModuleExchangeMessage;
import optimus.dht.common.internal.transport.generated.InitialProto2.ModuleProtocolVersion;
import optimus.dht.common.internal.transport.generated.InitialProto2.SingleModuleExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyInitialClientHandler extends ChannelDuplexHandler {

  private static final Logger logger = LoggerFactory.getLogger(NettyInitialClientHandler.class);

  private final ClientIdentity clientIdentity;
  private final Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers;
  private final ClientChannelSharedState sharedState;

  private final Map<Integer, ClientInitialStreamHandler> establishingHandlers;
  private final Map<Integer, EstablishedStreamHandler> establishedHandlers;
  private final Map<Integer, String> remoteCodeVersions;
  private final List<CloseableStreamHandler> handlersList;
  private final Map<Integer, Integer> negotiatedProtocolVersions;

  private boolean firstMessage = true;
  private boolean fault = false;

  public NettyInitialClientHandler(
      ClientIdentity clientIdentity,
      Map<Integer, ClientProtocolVersionHandler> protocolVersionHandlers,
      ClientChannelSharedState sharedState) {
    this.clientIdentity = clientIdentity;
    this.protocolVersionHandlers = protocolVersionHandlers;
    this.sharedState = sharedState;
    this.establishingHandlers = new HashMap<>();
    this.establishedHandlers = new HashMap<>();
    this.remoteCodeVersions = new TreeMap<>();
    this.handlersList = new ArrayList<>();
    this.negotiatedProtocolVersions = new HashMap<>();
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    sharedState.handlers(handlersList);
    sharedState.remoteCodeVersions(remoteCodeVersions);
    sharedState.negotiatedProtocolVersions(negotiatedProtocolVersions);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    InitialClientMessage.Builder builder = InitialClientMessage.newBuilder();
    builder.setClientUniqueId(clientIdentity.uniqueId());
    builder.setClientCodeVersion(clientIdentity.codeVersion());
    builder.setClientFQDN(clientIdentity.hostFQDN());
    builder.setClientPID(clientIdentity.pid());
    if (clientIdentity.cloudName() != null) {
      builder.setClientCloudName(clientIdentity.cloudName());
    }
    protocolVersionHandlers.forEach(
        (moduleId, handler) -> {
          ModuleProtocolVersion.Builder moduleVersionBuilder = ModuleProtocolVersion.newBuilder();
          moduleVersionBuilder.setModuleId(moduleId);
          moduleVersionBuilder.setProtocolVersion(handler.clientPreferredProtocolVersion());
          moduleVersionBuilder.setModuleCodeVersion(handler.codeVersion());
          builder.addModuleVersions(moduleVersionBuilder);
        });

    byte[] serializedMessage = builder.build().toByteArray();
    ByteBuf byteBuf = ctx.alloc().buffer(serializedMessage.length);
    byteBuf.writeBytes(serializedMessage);
    ctx.writeAndFlush(byteBuf);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf incomingBuf = (ByteBuf) msg;
    byte[] message = new byte[incomingBuf.readableBytes()];
    incomingBuf.readBytes(message);
    incomingBuf.release();
    if (firstMessage) {
      firstMessage = false;
      processInitialServerMessage(ctx, InitialServerMessage.parseFrom(message));
    } else {
      processModuleExchangeMessage(ctx, ModuleExchangeMessage.parseFrom(message));
    }

    if (fault) {
      ctx.close();
    } else if (establishingHandlers.isEmpty()) {
      ctx.fireUserEventTriggered(new ConnectionEstablishedEvent(establishedHandlers));
      ctx.fireChannelActive();
    }
  }

  protected void processInitialServerMessage(
      ChannelHandlerContext ctx, InitialServerMessage initialServerMessage) {
    String serverUniqueId = initialServerMessage.getServerUniqueId();
    String serverCodeVersion =
        initialServerMessage.hasServerCodeVersion()
            ? initialServerMessage.getServerCodeVersion()
            : null;
    String serverFQDN =
        initialServerMessage.hasServerFQDN() ? initialServerMessage.getServerFQDN() : null;
    long serverPid = initialServerMessage.hasServerPID() ? initialServerMessage.getServerPID() : -1;
    String serverCloudName =
        initialServerMessage.hasServerCloudName()
            ? initialServerMessage.getServerCloudName()
            : null;

    RemoteInstanceIdentity remoteInstanceIdentity =
        new ProvidedRemoteInstanceIdentity(
            serverUniqueId,
            serverCodeVersion,
            serverFQDN,
            serverPid,
            serverCloudName,
            (InetSocketAddress) ctx.channel().remoteAddress());

    sharedState.remoteInstanceIdentity(remoteInstanceIdentity);

    RemoteConnectionDetails remoteConnectionDetails =
        new RemoteConnectionDetails(remoteInstanceIdentity, sharedState.authenticatedConnection());

    boolean completed =
        initialServerMessage.hasCompleted() ? initialServerMessage.getCompleted() : false;

    ModuleExchangeMessage.Builder builder;
    try {
      builder = ModuleExchangeMessage.newBuilder();

      for (ModuleProtocolVersion moduleProtocolVersion :
          initialServerMessage.getModuleVersionsList()) {
        int moduleId = moduleProtocolVersion.getModuleId();
        SingleModuleExchange.Builder singleBuilder = SingleModuleExchange.newBuilder();
        singleBuilder.setModuleId(moduleId);

        ClientProtocolVersionHandler protocolHandler = protocolVersionHandlers.get(moduleId);
        if (protocolHandler == null) {
          singleBuilder.setUnsupported(true);
        } else {
          int protocolVersion = moduleProtocolVersion.getProtocolVersion();
          ClientInitialStreamHandler initialHandler =
              protocolHandler.buildInitialHandler(
                  protocolVersion, sharedState.server(), remoteConnectionDetails);
          negotiatedProtocolVersions.put(moduleId, protocolVersion);
          remoteCodeVersions.put(moduleId, moduleProtocolVersion.getModuleCodeVersion());

          byte[] initialEstablished = initialHandler.initialEstablished();
          if (initialEstablished != null) {
            singleBuilder.setPayload(UnsafeByteOperations.unsafeWrap(initialEstablished));
          }

          if (initialHandler.isInitialFinished()) {
            singleBuilder.setFinished(true);
            EstablishedStreamHandler establishedHandler =
                initialHandler.createEstablishedHandler(sharedState.server());
            establishedHandlers.put(moduleId, establishedHandler);
            handlersList.add(establishedHandler);
          } else {
            establishingHandlers.put(moduleId, initialHandler);
            handlersList.add(initialHandler);
          }
        }
        builder.addMessages(singleBuilder);
      }

    } catch (InitialExchangeException e) {
      logger.warn("Initial exchange failed, will close connection", e);
      builder = ModuleExchangeMessage.newBuilder();
      builder.setCompleted(true);
      builder.setFault(e.getMessage());
      fault = true;
    }

    if (!completed) {
      byte[] message = builder.build().toByteArray();
      ByteBuf byteBuf = ctx.alloc().buffer(message.length);
      byteBuf.writeBytes(message);
      ctx.writeAndFlush(byteBuf);
    }
  }

  protected void processModuleExchangeMessage(
      ChannelHandlerContext ctx, ModuleExchangeMessage moduleExchangeMessage) {
    ModuleExchangeMessage.Builder builder = ModuleExchangeMessage.newBuilder();

    moduleExchangeMessage
        .getMessagesList()
        .forEach(
            singleModuleMessage -> {
              int moduleId = singleModuleMessage.getModuleId();
              SingleModuleExchange.Builder singleBuilder = SingleModuleExchange.newBuilder();
              singleBuilder.setModuleId(moduleId);
              ClientInitialStreamHandler handler = establishingHandlers.get(moduleId);
              if (handler == null) {
                singleBuilder.setUnsupported(true);
              } else {
                //        byte[] onInitialEstablished = handler.onReceived(reveived)
                //        if (onInitialEstablished != null) {
                //          singleBuilder.setPayload(ByteString.copyFrom(onInitialEstablished));
                //        }
                //        if (handler.isInitialFinished()) {
                //          singleBuilder.setFinished(true);
                //          establishingHandlers.remove(moduleId);
                //          fullyEstablishedHandlers.put(moduleId,
                // handler.createFullyEstablishedHandler(null));
                //        }
              }
              builder.addMessages(singleBuilder);
            });
  }

  public static class ConnectionEstablishedEvent {
    private final Map<Integer, EstablishedStreamHandler> establishedHandlers;

    public ConnectionEstablishedEvent(Map<Integer, EstablishedStreamHandler> establishedHandlers) {
      this.establishedHandlers = establishedHandlers;
    }

    public Map<Integer, EstablishedStreamHandler> establishedHandlers() {
      return establishedHandlers;
    }
  }
}
