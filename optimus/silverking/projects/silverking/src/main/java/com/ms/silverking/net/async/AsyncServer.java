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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for implementing an asynchronous TCP/IP server. Maintains persistent connections with
 * peers.
 */
public class AsyncServer<T extends Connection> extends AsyncBase<T> {
  private final InetSocketAddress localSocketAddr;
  private ServerSocketChannel serverChannel;
  private final IncomingConnectionListener<T> incomingConnectionListener;
  private final boolean debug;
  private boolean enabled;

  private static Logger log = LoggerFactory.getLogger(AsyncServer.class);

  public static boolean verbose = AsyncGlobals.verbose;

  private AsyncServer(
      int port,
      int backlog,
      int numSelectorControllers,
      String controllerClass,
      Acceptor<T> acceptor,
      ConnectionCreator<T> connectionCreator,
      IncomingConnectionListener<T> incomingConnectionListener,
      LWTPool readerLWTPool,
      LWTPool writerLWTPool,
      int selectionThreadWorkLimit,
      boolean enabled,
      boolean debug)
      throws IOException {
    super(
        port,
        numSelectorControllers,
        controllerClass,
        acceptor,
        connectionCreator,
        readerLWTPool,
        writerLWTPool,
        selectionThreadWorkLimit,
        debug);

    this.incomingConnectionListener = incomingConnectionListener;
    this.enabled = enabled;
    this.debug = debug;

    acceptor.setAsyncServer(this);

    // Create a new non-blocking server socket channel
    serverChannel = ServerSocketChannel.open();

    // Bind the server socket to the specified address and port
    localSocketAddr = new InetSocketAddress(port);
    serverChannel.socket().bind(localSocketAddr, backlog);

    // Now add the server channel to the AsyncBase
    addServerChannel(serverChannel);

    if (verbose) {
      log.info("AsyncServer.port: {}", getPort());
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public AsyncServer(
      int port,
      int backlog,
      int numSelectorControllers,
      String controllerClass,
      ConnectionCreator<T> connectionCreator,
      IncomingConnectionListener<T> newConnectionListener,
      LWTPool readerLWTPool,
      LWTPool writerLWTPool,
      LWTPool acceptorPool,
      int selectionThreadWorkLimit,
      boolean enabled,
      boolean debug)
      throws IOException {
    this(
        port,
        backlog,
        numSelectorControllers,
        controllerClass,
        new Acceptor<T>(acceptorPool),
        connectionCreator,
        newConnectionListener,
        readerLWTPool,
        writerLWTPool,
        selectionThreadWorkLimit,
        enabled,
        debug);
  }

  //////////////////////////////////////////////////////////////////////

  public final int getPort() {
    return serverChannel.socket().getLocalPort();
  }

  //////////////////////////////////////////////////////////////////////

  public void enable() {
    enabled = true;
  }

  public void shutdown() {
    try {
      if (serverChannel != null) {
        serverChannel.close();
        serverChannel = null;
      }
    } catch (IOException ioe) {
      log.error("", ioe);
    }
    super.shutdown();
  }

  // Need to expose this for tests as socket chanell close is final and cannot be mocked
  void closeChannel(SocketChannel socketChannel) throws IOException {
    socketChannel.close();
  }

  void accept(ServerSocketChannel channel) {
    SocketChannel socketChannel = null;
    boolean connectionSuccess = false;

    assert channel != null;
    log.debug("accept {}", channel);

    try {
      socketChannel = channel.accept();
      if (isEnabled()) {
        if (socketChannel != null) {
          T connection;

          connection = addConnection(socketChannel, true);
          incomingConnectionListener.incomingConnection(connection);
        } else {
          if (AsyncGlobals.debug && debug) {
            log.debug("null socketChannel");
          }
        }
      } else {
        if (socketChannel != null) {
          // if we're not enabled, we are not yet
          // able to process incoming connections
          closeChannel(socketChannel);
        }
      }
      connectionSuccess = true;
    } catch (AuthFailedException | IOException e) {
      log.error("", e);
    } finally {
      if (!connectionSuccess && socketChannel != null) {
        try {
          closeChannel(socketChannel);
        } catch (IOException e) {
          log.error("Could not close socketChannel {}", socketChannel, e);
        }
      }
    }
  }

  static class Acceptor<T extends Connection>
      extends BaseWorker<Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>> {
    private AsyncServer<T> asyncServer;

    public Acceptor(LWTPool lwtPool) {
      super(lwtPool, true, 0);
    }

    public void setAsyncServer(AsyncServer<T> asyncServer) {
      this.asyncServer = asyncServer;
    }

    @Override
    public void doWork(Triple<ServerSocketChannel, SelectorController<T>, SelectionKey> work) {
      ServerSocketChannel channel;
      SelectorController<T> selectorController;
      SelectionKey key;

      channel = work.getV1();
      selectorController = work.getV2();
      key = work.getV3();
      try {
        asyncServer.accept(channel);
      } finally {
        selectorController.addSelectionKeyOps(key, SelectionKey.OP_ACCEPT);
      }
    }

    @Override
    public Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>[] newWorkArray(
        int size) {
      return new Triple[size];
    }
  }
}
