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

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import io.netty.channel.ChannelHandler;
import jakarta.inject.Inject;
import javax.net.ssl.SSLException;

import optimus.dht.client.api.transport.IdleConnectionStrategy;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.ssl.SslCloseCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import optimus.dht.client.internal.exceptions.UnexpectedServerIdException;
import optimus.dht.common.internal.transport.ChannelHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.assistedinject.Assisted;

import optimus.dht.client.api.ClientIdentity;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.transport.ConnectionsManager;
import optimus.dht.client.api.transport.ServerConnectionStateListener;
import optimus.dht.client.api.transport.ServerConnectionStateListener.FailureReason;
import optimus.dht.client.internal.module.ClientModulesAware;
import optimus.dht.client.internal.module.ClientModulesRegistry;
import optimus.dht.common.api.RemoteInstanceIdentity;
import optimus.dht.common.api.transport.CorruptedStreamException;
import optimus.dht.common.api.transport.InitialExchangeException;
import optimus.dht.common.api.transport.MessageGenerator;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.SentMessageMetrics;
import optimus.dht.common.api.transport.CloseableStreamHandler;
import optimus.dht.common.internal.transport.ChannelSharedState;
import optimus.dht.common.internal.transport.ConnectionCountingChannelHandler;
import optimus.dht.common.internal.transport.EstablishedTransportMessagesQueue;
import optimus.dht.common.internal.transport.FailableTransportMessagesQueue;
import optimus.dht.common.internal.transport.InitialTransportMessagesQueue;
import optimus.dht.common.internal.transport.InitialTransportMessagesQueue.QueueElement;
import optimus.dht.common.internal.transport.MutableSentMessageMetrics;
import optimus.dht.common.internal.transport.NettyEstablishedChannelHandler;
import optimus.dht.common.internal.transport.SslHandshakeCompletedInboundChannelHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.AttributeKey;

public class DefaultConnectionsManager implements ConnectionsManager, ClientModulesAware {

  private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionsManager.class);

  private static final AttributeKey<ClientChannelSharedState> SHARED_STATE_ATTR =
      AttributeKey.valueOf("SHARED_STATE");

  protected final ClientIdentity clientIdentity;
  protected final ClientTransportConfig config;
  protected final ChannelHandlerFactory channelHandlerFactory;
  protected final ServerConnectionStateListener stateListener;
  protected Bootstrap bootstrapTemplate;

  protected final ConcurrentMap<ServerConnection, Handle> connections = new ConcurrentHashMap<>();

  @Inject
  public DefaultConnectionsManager(
      ClientIdentity clientIdentity,
      ClientTransportConfig config,
      ChannelHandlerFactory channelHandlerFactory,
      @Assisted ServerConnectionStateListener stateListener) {
    this.clientIdentity = clientIdentity;
    this.config = config;
    this.channelHandlerFactory = channelHandlerFactory;
    this.stateListener = stateListener;
    this.bootstrapTemplate =
        new Bootstrap()
            .group(
                new NioEventLoopGroup(
                    config.threads(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("dht-netty-" + clientIdentity.uniqueId() + "-%d")
                        .build()))
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) config.connectTimeout().toMillis())
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(
                ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(config.writeLowWaterMark(), config.writeHighWaterMark()))
            .option(
                ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(
                    config.readMinBufferSize(),
                    config.readMinBufferSize(),
                    config.readMaxBufferSize()));

    if (config.asyncDns()) {
      this.bootstrapTemplate.resolver(
          new DnsAddressResolverGroup(
              NioDatagramChannel.class, DefaultDnsServerAddressStreamProvider.INSTANCE));
    }
  }

  @Override
  public void initWithModulesRegistry(ClientModulesRegistry registry) {
    bootstrapTemplate.handler(
        new ChannelInitializer<Channel>() {

          private final AtomicInteger connectionsCount = new AtomicInteger();
          private final SslContext sslContext;

          {
            if (config.sslTransportConfig() != null) {
              try {
                sslContext =
                    SslContextBuilder.forClient()
                        .keyManager(config.sslTransportConfig().keyManagerFactory())
                        .trustManager(config.sslTransportConfig().trustManagerFactory())
                        .sslProvider(
                            config.sslTransportConfig().runtimeImplementation().nettyProvider())
                        .build();
              } catch (SSLException e) {
                throw new RuntimeException(e);
              }
            } else {
              sslContext = null;
            }
          }

          @Override
          protected void initChannel(Channel ch) {
            ClientChannelSharedState sharedState = ch.attr(SHARED_STATE_ATTR).get();
            ch.attr(SHARED_STATE_ATTR).set(null);

            if (config.kerberosExecutor() != null) {
              ChannelHandler kerberosHandler =
                  channelHandlerFactory.createKerberosHandler(
                      false, config.kerberosExecutor(), sharedState);
              ch.pipeline().addLast("kerberos", kerberosHandler);
            }
            if (config.sslTransportConfig() != null) {
              SslHandler sslHandler = sslContext.newHandler(ch.alloc());
              sslHandler.setHandshakeTimeoutMillis(
                  config.sslTransportConfig().handshakeTimeout().toMillis());
              ch.pipeline().addLast("ssl", sslHandler);
              ch.pipeline()
                  .addLast(
                      "ssl-handshake",
                      new SslHandshakeCompletedInboundChannelHandler(sslHandler, sharedState));
            }
            ch.pipeline()
                .addLast("protocol-sensing", channelHandlerFactory.createAdaptiveHandler(false));
            ch.pipeline()
                .addLast(
                    "initial-length-prepender",
                    new LengthFieldPrepender(ByteOrder.LITTLE_ENDIAN, 4, 0, false));
            ch.pipeline()
                .addLast(
                    "initial-frame-decoder",
                    new LengthFieldBasedFrameDecoder(
                        ByteOrder.LITTLE_ENDIAN, Integer.MAX_VALUE, 0, 4, 0, 4, false));

            ch.pipeline()
                .addLast(
                    "initial-establisher",
                    new NettyInitialClientHandler(
                        clientIdentity, registry.protocolVersionHandlers(), sharedState));
            ch.pipeline()
                .addLast("connection-management", new ConnectionManagementHandler(sharedState));
            ch.pipeline()
                .addLast(
                    "connection-counting",
                    new ConnectionCountingChannelHandler(sharedState, connectionsCount));
          }
        });
  }

  @Override
  public void establishConnection(ServerConnection node) {
    handleForConnection(node);
  }

  @Override
  public void closeConnection(ServerConnection node) {
    Handle handle = handleForConnection(node);
    handle.close();
  }

  @Override
  public void sendMessage(
      ServerConnection node,
      short moduleId,
      MessageGenerator messageGenerator,
      @Nullable Consumer<SentMessageMetrics> metricsCallback) {
    Handle handle = handleForConnection(node);
    handle.addMessage(moduleId, messageGenerator, metricsCallback);
    handle.flush();
  }

  @Override
  public boolean offerMessage(
      ServerConnection node,
      short moduleId,
      MessageGenerator messageGenerator,
      @Nullable Consumer<SentMessageMetrics> metricsCallback) {
    Handle handle = handleForConnection(node);
    if (handle.offerMessage(moduleId, messageGenerator, metricsCallback)) {
      handle.flush();
      return true;
    } else {
      return false;
    }
  }

  protected Handle handleForConnection(ServerConnection server) {
    Handle handle = connections.get(server);
    if (handle == null) {
      InitialTransportMessagesQueue messagesQueue = new InitialTransportMessagesQueue();
      ConnectingHandle newHandle = new ConnectingHandle(messagesQueue, server);
      Handle existingHandle = connections.putIfAbsent(server, newHandle);
      if (existingHandle == null) {
        handle = newHandle;
        ClientChannelSharedState sharedState = new ClientChannelSharedState(newHandle);
        stateListener.serverConnecting(server);
        ChannelFuture channelFuture =
            bootstrapTemplate
                .clone()
                .attr(SHARED_STATE_ATTR, sharedState)
                .connect(server.serverAddress());
        newHandle.channelFuture = channelFuture;
        channelFuture.addListener(
            future -> {
              if (!future.isSuccess()) {
                connections.remove(server, newHandle);
                stateListener.serverDisconnectedOrFailed(
                    server, FailureReason.IO, false, future.cause());
              }
            });
      } else {
        handle = existingHandle;
      }
    }
    return handle;
  }

  @Override
  public void shutdown(boolean gracefully) {
    EventLoopGroup eventLoopGroup = bootstrapTemplate.config().group();
    if (eventLoopGroup != null) {
      if (gracefully) {
        eventLoopGroup.shutdownGracefully();
      } else {
        eventLoopGroup.shutdownNow();
      }
    }
  }

  public interface Handle {

    void addMessage(
        short moduleId,
        MessageGenerator message,
        @Nullable Consumer<SentMessageMetrics> metricsCallback);

    boolean offerMessage(
        short moduleId,
        MessageGenerator message,
        @Nullable Consumer<SentMessageMetrics> metricsCallback);

    void flush();

    void close();
  }

  public class ConnectingHandle implements Handle {

    protected final InitialTransportMessagesQueue messagesQueue;

    protected final ServerConnection serverNode;

    protected ConnectedHandle connectedHandle;

    protected ChannelFuture channelFuture;

    public ConnectingHandle(
        InitialTransportMessagesQueue messagesQueue, ServerConnection serverNode) {
      this.messagesQueue = messagesQueue;
      this.serverNode = serverNode;
    }

    @Override
    public synchronized void addMessage(
        short moduleId,
        MessageGenerator message,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      if (connectedHandle != null) {
        connectedHandle.addMessage(moduleId, message, metricsCallback);
      } else {
        messagesQueue.addMessage(moduleId, message, metricsCallback);
      }
    }

    @Override
    public synchronized boolean offerMessage(
        short moduleId,
        MessageGenerator message,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      if (connectedHandle != null) {
        return connectedHandle.offerMessage(moduleId, message, metricsCallback);
      } else {
        return messagesQueue.offerMessage(moduleId, message, metricsCallback);
      }
    }

    @Override
    public synchronized void flush() {
      if (connectedHandle != null) {
        connectedHandle.flush();
      }
    }

    @Override
    public void close() {
      channelFuture.addListener(
          ChannelFutureListener.CLOSE); // this handles condition when channel is already active
      channelFuture.cancel(true); // try to cancel it
    }

    Stream<QueueElement> elements() {
      return messagesQueue.elements();
    }
  }

  public class ConnectedHandle implements Handle {

    protected final Channel channel;

    protected final EstablishedTransportMessagesQueue messagesQueue;

    protected final Map<Integer, Integer> negotiatedProtocolVersions;

    public ConnectedHandle(
        EstablishedTransportMessagesQueue messagesQueue,
        Channel channel,
        Map<Integer, Integer> negotiatedProtocolVersions) {
      this.messagesQueue = messagesQueue;
      this.channel = channel;
      this.negotiatedProtocolVersions = negotiatedProtocolVersions;
    }

    @Override
    public void addMessage(
        short moduleId,
        MessageGenerator messageGenerator,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      Integer protocolVersion = negotiatedProtocolVersions.get(Integer.valueOf(moduleId));

      MessageStream message = messageGenerator.build(protocolVersion);

      if (message != null) {
        messagesQueue.addMessage(
            moduleId, message, MutableSentMessageMetrics.connected(), metricsCallback);
      } else if (metricsCallback != null) {
        metricsCallback.accept(SentMessageMetrics.NOT_SENT);
      }
    }

    @Override
    public boolean offerMessage(
        short moduleId,
        MessageGenerator messageGenerator,
        @Nullable Consumer<SentMessageMetrics> metricsCallback) {
      Integer protocolVersion = negotiatedProtocolVersions.get(Integer.valueOf(moduleId));

      MessageStream message = messageGenerator.build(protocolVersion);

      if (message != null) {
        return messagesQueue.offerMessage(
            moduleId, message, MutableSentMessageMetrics.connected(), metricsCallback);
      } else if (metricsCallback != null) {
        metricsCallback.accept(SentMessageMetrics.NOT_SENT);
      }

      return false;
    }

    @Override
    public void flush() {
      channel.flush();
    }

    @Override
    public void close() {
      channel.close();
    }
  }

  public static class ClientChannelSharedState extends ChannelSharedState {

    private final ServerConnection server;

    private ConnectingHandle connectingHandle;
    private Handle currentHandle;

    public ClientChannelSharedState(ConnectingHandle connectingHandle) {
      this.server = connectingHandle.serverNode;
      connectingHandle(connectingHandle);
      currentHandle(connectingHandle);
      messagesQueue(connectingHandle.messagesQueue);
    }

    public ServerConnection server() {
      return server;
    }

    public ConnectingHandle connectingHandle() {
      return connectingHandle;
    }

    public void connectingHandle(ConnectingHandle connectingHandle) {
      this.connectingHandle = connectingHandle;
    }

    public Handle currentHandle() {
      return currentHandle;
    }

    public void currentHandle(Handle currentHandle) {
      this.currentHandle = currentHandle;
    }
  }

  private class ConnectionManagementHandler extends ChannelDuplexHandler {

    private final ClientChannelSharedState sharedState;
    private boolean listenersNotifiedOnDisconnect;
    private boolean established;

    public ConnectionManagementHandler(ClientChannelSharedState sharedState) {
      this.sharedState = sharedState;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

      ServerConnection server = sharedState.server();

      if (listenersNotifiedOnDisconnect) {
        logger.warn("Listeners already notified after exceptionCaught, server=" + server, cause);
        return;
      }

      try {

        RemoteInstanceIdentity remote = sharedState.remoteInstanceIdentity();

        if (cause instanceof UnexpectedServerIdException) {
          logger.warn(
              "Connected to an unexpected server, server=" + server + ", remote=" + remote, cause);
          stateListener.serverDisconnectedOrFailed(
              server, FailureReason.WRONG_ID, established, cause);
        } else if (cause instanceof CorruptedStreamException) {
          logger.warn("Received wrong input, server=" + server + ", remote=" + remote, cause);
          stateListener.serverDisconnectedOrFailed(
              server, FailureReason.PROTOCOL, established, cause);
        } else if (cause instanceof InitialExchangeException) {
          logger.warn("Initial exchange failed, server=" + server + ", remote=" + remote, cause);
          stateListener.serverDisconnectedOrFailed(
              server, FailureReason.PROTOCOL, established, cause);
        } else if (cause instanceof IOException) {
          logger.info(
              "Received IO exception, server={}, remote={}, exception={}",
              server,
              remote,
              String.valueOf(cause));
          stateListener.serverDisconnectedOrFailed(server, FailureReason.IO, established, cause);
        } else {
          logger.warn(
              "Unexpected failure on active channel, server=" + server + ", remote=" + remote,
              cause);
          stateListener.serverDisconnectedOrFailed(
              server, FailureReason.UNKNOWN, established, cause);
        }
        callHandlersAndMessages(ctx, cause);
      } finally {
        listenersNotifiedOnDisconnect = true;
        Channel channel = ctx.channel();
        if (channel.isOpen()) {
          channel.close();
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      ServerConnection server = sharedState.server();
      if (!listenersNotifiedOnDisconnect) {
        listenersNotifiedOnDisconnect = true;
        stateListener.serverDisconnectedOrFailed(server, FailureReason.UNKNOWN, established, null);
        callHandlersAndMessages(ctx, null);
      }
      connections.remove(server, sharedState.currentHandle());
      ctx.fireChannelInactive();
    }

    private void callHandlersAndMessages(ChannelHandlerContext ctx, Throwable e) {
      List<CloseableStreamHandler> list = sharedState.handlers();
      if (list != null) {
        for (CloseableStreamHandler handler : list) {
          try {
            handler.connectionClosed(e);
          } catch (Exception e2) {
            logger.warn("Exception caught while calling connectionClosed", e2);
          }
        }
      }
      FailableTransportMessagesQueue failableTransportMessagesQueue = sharedState.messagesQueue();
      if (failableTransportMessagesQueue != null) {
        failableTransportMessagesQueue.failAll(e);
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

      if (evt instanceof NettyInitialClientHandler.ConnectionEstablishedEvent) {
        handleConnectionEstablished(
            ctx, (NettyInitialClientHandler.ConnectionEstablishedEvent) evt);
      } else if (evt instanceof SslCloseCompletionEvent) {
        // NOOP - noise avoidance
      } else {
        logger.warn("Ignoring unknown userEvent={}", evt);
      }
    }

    private void handleConnectionEstablished(
        ChannelHandlerContext ctx, NettyInitialClientHandler.ConnectionEstablishedEvent event)
        throws Exception {
      EstablishedTransportMessagesQueue establishedMessagesQueue =
          new EstablishedTransportMessagesQueue();

      ConnectingHandle connectingHandle = sharedState.connectingHandle();
      ServerConnection server = sharedState.server();
      ConnectedHandle connectedHandle;

      String remoteUniqueId = sharedState.remoteInstanceIdentity().uniqueId();
      if (!server.uniqueId().equals(remoteUniqueId)) {
        throw new UnexpectedServerIdException(
            "Server responded with unexpected uniqueId=" + remoteUniqueId + ", server=" + server);
      }

      Map<Integer, Integer> negotiatedProtocolVersions = sharedState.negotiatedProtocolVersions();

      synchronized (connectingHandle) {
        connectingHandle
            .elements()
            .forEach(
                element -> {
                  short moduleId = element.moduleId();
                  int protocolVersion = negotiatedProtocolVersions.get(Integer.valueOf(moduleId));
                  MessageStream messageStream =
                      element.messageGenerator().build(Integer.valueOf(protocolVersion));
                  MutableSentMessageMetrics metrics = element.metrics();
                  Consumer<SentMessageMetrics> metricsCallback = element.metricsCallback();
                  metrics.touchConnected();
                  establishedMessagesQueue.addMessage(
                      moduleId, messageStream, metrics, metricsCallback);
                });

        connectedHandle =
            new ConnectedHandle(
                establishedMessagesQueue, ctx.channel(), negotiatedProtocolVersions);
        connectingHandle.connectedHandle = connectedHandle;

        sharedState.messagesQueue(establishedMessagesQueue);
      }

      established = true;

      ctx.pipeline().remove("initial-establisher");
      ctx.pipeline().remove("initial-length-prepender");

      ServerConnection serverConnection = connectingHandle.serverNode;
      if (config.idleConnectionStrategy().isActive()) {
        addIdleConnectionStrategy(ctx, serverConnection, connectedHandle);
      }

      ctx.pipeline()
          .addBefore(
              "connection-management",
              "established-handler",
              new NettyEstablishedChannelHandler(
                  config.sendBufferSize(),
                  sharedState,
                  event.establishedHandlers(),
                  establishedMessagesQueue));

      ctx.pipeline().remove("initial-frame-decoder");

      if (!connections.replace(serverConnection, connectingHandle, connectedHandle)) {
        throw new IllegalStateException(
            "Failed to replace handle for server="
                + serverConnection
                + ", oldHandler="
                + connectingHandle
                + ", newHandler="
                + connectedHandle);
      }
      sharedState.currentHandle(connectedHandle);
      sharedState.connectingHandle(null);
      stateListener.serverConnected(serverConnection);
    }

    private void addIdleConnectionStrategy(
        ChannelHandlerContext ctx, ServerConnection serverConnection, ConnectedHandle handle) {
      IdleConnectionStrategy idleConnectionStrategy = config.idleConnectionStrategy();
      ctx.pipeline()
          .addBefore(
              "connection-management",
              "idle-state-handler-netty",
              new IdleStateHandler(0, 0, (int) idleConnectionStrategy.idleDuration().getSeconds()));
      ctx.pipeline()
          .addBefore(
              "connection-management",
              "idle-state-handler-dht",
              new IdleConnectionHandler(serverConnection, handle, idleConnectionStrategy));
    }
  }
}
