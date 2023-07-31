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

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.net.security.AuthenticationResult;
import com.ms.silverking.net.security.Authenticator;
import com.ms.silverking.net.security.NoopAuthenticatorImpl;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTConstants;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for implementing an asynchronous TCP/IP server.
 * Maintains persistent connections with peers.
 */
public abstract class AsyncBase<T extends Connection> {
  private final List<SelectorController<T>> selectorControllers;

  private static Logger log = LoggerFactory.getLogger(AsyncBase.class);

  private SuspectAddressListener suspectAddressListener;

  private static final int _defReceiveBufferSize = 1024 * 1024;
  private static final int _defSendBufferSize = 1024 * 1024;

  // FUTURE - link below to default op timeouts so that we at least try one additional connection in each op
  private static final int _defSocketReadTimeout = 5 * 60 * 1000 - 10 * 1000;
  private static final int _defSocketConnectTimeout = 25 * 1000;
  private static final int _defaultAuthenticationTimeoutInMillisecond = 30 * 1000;

  private static final int defReceiveBufferSize;
  private static final int defSendBufferSize;
  private static final int defSocketReadTimeout;
  private static final int defSocketConnectTimeout;
  private static final int defAuthenticationTimeoutInMillisecond;

  private static final String propertyBase = AsyncBase.class.getPackage().getName() + ".";
  private static final String defReceiveBufferSizeProperty = propertyBase + "ReceiveBufferSize";
  private static final String defSendBufferSizeProperty = propertyBase + "SendBufferSize";
  private static final String defSocketReadTimeoutProperty = propertyBase + "SocketReadTimeout";
  private static final String defSocketConnectTimeoutProperty = propertyBase + "SocketConnectTimeoutProperty";
  private static final String defAuthenticationTimeoutProperty = propertyBase + "TimeoutMs";

  private final ChannelSelectorControllerAssigner<T> cscAssigner;
  private final ConnectionCreator<T> connectionCreator;
  private final ConnectionManager<T> connectionManager;
  private final boolean debug;
  private final ConnectionStatsWriter connectionStatsWriter;
  private final Authenticator authenticator;

  // favors short operations rather than attempting to parallelize
  private static final int asyncBaseIdleThreadThreshold = Integer.MAX_VALUE;
  private static final int asyncBaseMaxDirectCallDepth = LWTConstants.defaultMaxDirectCallDepth;

  private static final boolean tcpNoDelay = true;
  private static final boolean logConnections = AsyncGlobals.verbose;

  private static final Authenticator _defAuthenticator;
  private static final ThreadLocal<Authenticator> defAuthenticatorThreadLocal;

  protected boolean running;

  static {
    defReceiveBufferSize = setProperty(defReceiveBufferSizeProperty, _defReceiveBufferSize);
    defSendBufferSize = setProperty(defSendBufferSizeProperty, _defSendBufferSize);
    defSocketReadTimeout = setProperty(defSocketReadTimeoutProperty, _defSocketReadTimeout);
    defSocketConnectTimeout = setProperty(defSocketConnectTimeoutProperty, _defSocketConnectTimeout);
    defAuthenticationTimeoutInMillisecond = setProperty(defAuthenticationTimeoutProperty,
        _defaultAuthenticationTimeoutInMillisecond);

    _defAuthenticator = Authenticator.getAuthenticator();
    if (_defAuthenticator instanceof NoopAuthenticatorImpl) {
      log.debug(
          "Authenticator: No authentication operation will be performed; Back-ended by [ {} ]" , _defAuthenticator.getName());
    } else {
      log.debug("Authenticator: A customized authenticator is in use [{}}" , _defAuthenticator.getName() );
    }
    defAuthenticatorThreadLocal = new ThreadLocal<Authenticator>() {
      @Override
      protected Authenticator initialValue() {
        return _defAuthenticator.createLocalCopy();
      }
    };

    if (AsyncGlobals.verbose) {
      log.info("defReceiveBufferSize: {}" , defReceiveBufferSize);
      log.info("defSendBufferSize: {}" , defSendBufferSize);
      log.info("defSocketReadTimeout: {}" , defSocketReadTimeout);
      log.info("defSocketConnectTimeout: {}" , defSocketConnectTimeout);
    }
  }

  private static int setProperty(String property, int defaultValue) {
    String val;
    int rVal;

    val = System.getProperty(property);
    if (val == null) {
      rVal = defaultValue;
    } else {
      rVal = Integer.parseInt(val);
    }
    if (AsyncGlobals.verbose) {
      log.info("{}:  {}",property , rVal);
    }
    return rVal;
  }

  //TODO (OPTIMUS-0000): we may need to remove this hardcode "/tmp/silverking/"
  private static File statsBaseDir(int port) {
    return new File("/tmp/silverking/stats/" + port);
  }

  private AsyncBase(int port, int numSelectorControllers, String controllerClass,
      BaseWorker<Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>> acceptWorker,
      ConnectionCreator<T> connectionCreator, ChannelSelectorControllerAssigner<T> cscAssigner, LWTPool readerWorkPool,
      LWTPool writerWorkPool, int selectionThreadWorkLimit, boolean debug) throws IOException {
    this.cscAssigner = cscAssigner;
    this.connectionCreator = connectionCreator;
    this.connectionManager = new ConnectionManager<T>();
    this.debug = debug;

    selectorControllers = new ArrayList<SelectorController<T>>();
    running = true;
    for (int i = 0; i < numSelectorControllers; i++) {
      selectorControllers.add(new SelectorController(acceptWorker, null/*ConnecctWorker*/, new Reader(readerWorkPool),
          new Writer(writerWorkPool), controllerClass, selectionThreadWorkLimit, debug));
    }

    if (Connection.statsEnabled) {
      File statsBaseDir;

      statsBaseDir = statsBaseDir(port);
      if (statsBaseDir.exists()) {
        FileUtil.cleanDirectory(statsBaseDir);
      }
      connectionStatsWriter = new ConnectionStatsWriter(statsBaseDir);
    } else {
      connectionStatsWriter = null;
    }

    this.authenticator = defAuthenticatorThreadLocal.get();
  }

  public AsyncBase(int port, int numSelectorControllers, String controllerClass,
      BaseWorker<Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>> acceptWorker,
      ConnectionCreator<T> connectionCreator, LWTPool readerLWTPool, LWTPool writerLWTPool,
      int selectionThreadWorkLimit, boolean debug) throws IOException {
    this(port, numSelectorControllers, controllerClass, acceptWorker, connectionCreator,
        new LocalGroupingCSCA<T>(numSelectorControllers / 4), readerLWTPool, writerLWTPool, selectionThreadWorkLimit,
        debug);
    // FUTURE allow more than just the fixed fraction of local selector controllers
    //new RandomChannelSelectorControllerAssigner<T>(), lwtPool, debug);
  }

  //////////////////////////////////////////////////////////////////////

  public void start() throws IOException {
  }

  /**
   * Terminates all SelectorControllers, reads, and writers.
   */
  public void shutdown() {
    running = false;
    for (SelectorController<T> selectorController : selectorControllers) {
      selectorController.shutdown();
    }
  }

  //////////////////////////////////////////////////////////////////////

  private void disconnect(Connection connection, String reason) {
    connectionManager.disconnectConnection(connection, reason);
  }

  public void setSuspectAddressListener(SuspectAddressListener suspectAddressListener) {
    if (this.suspectAddressListener != null) {
      throw new RuntimeException("Unexpected mutation of suspectAddressListener");
    }
    this.suspectAddressListener = suspectAddressListener;
  }

  public void informSuspectAddressListener(InetSocketAddress addr) {
    if (suspectAddressListener != null) {
      suspectAddressListener.addSuspect(addr, SuspectProblem.CommunicationError);
    } else {
      log.info("suspectAddressListener is null. Can't notify of: {}", addr);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////

  public final class Reader extends BaseWorker<T> {
    public Reader(LWTPool workPool) {
      super(workPool, true, asyncBaseMaxDirectCallDepth, asyncBaseIdleThreadThreshold);
    }

    @Override
    public void doWork(Connection connection) {
      int numRead;
      boolean cleanRead;

      cleanRead = false;
      try {
        numRead = connection.read();
        cleanRead = true;
        if (numRead < 0) {
          // Remote entity shut the socket down cleanly. Do the
          // same from our end and cancel the channel.
          // Also called if there is a corrupt message.
          disconnect(connection, "numRead < 0");
        }
      } catch (IOException ioe) {
        if (connection.isConnected()) {
          log.error("",ioe);
          informSuspectAddressListener(connection.getRemoteSocketAddress());
          // FUTURE - think about marking for all exceptions
                    /*
                    if (ioe.getMessage() != null) {
                        if (ioe.getMessage().startsWith("Connection reset")) {
                            if (suspectAddressListener != null) {
                                suspectAddressListener.addSuspect(connection.getRemoteSocketAddress());
                            } else {
                                Log.info("suspectAddressListener is null");
                            }
                        }
                    }
                    */
        }
      } catch (Exception e) {
        if (connection.isConnected()) {
          log.error("Unhandled exception",e);
        }
      } finally {
        if (!cleanRead) {
          disconnect(connection, "!cleanRead");
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////

  public class Writer extends BaseWorker<T> {
    public Writer(LWTPool workPool) {
      super(workPool, true, asyncBaseMaxDirectCallDepth, asyncBaseIdleThreadThreshold);
    }

    @Override
    public void doWork(Connection connection) {
      boolean cleanWrite;

      if (AsyncGlobals.debug) {
        log.debug("AsyncBase.Writer.doWork()");
      }
      cleanWrite = false;
      try {
        connection.writeAllPending();
        cleanWrite = true;
      } catch (IOException ioe) {
        if (connection.isConnected()) {
          log.error("",ioe);
          // Commented out for new approach.
          // New approach is to simply do the disconnect as before, but not mark as suspect.
          // If the reconnect fails, then it will get marked.
          //if (suspectAddressListener != null) {
          //  suspectAddressListener.addSuspect(connection.getRemoteSocketAddress());
          //} else {
          //  Log.info("suspectAddressListener is null");
          //}
        }
      } catch (Exception e) {
        if (connection.isConnected()) {
          log.error("Unhandled exception()", e);
        }
      } finally {
        if (!cleanWrite) {
          disconnect(connection, "!cleanWrite");
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////
  public T newOutgoingConnection(InetSocketAddress dest, ConnectionListener listener) throws IOException, AuthFailedException {
    SocketChannel channel = null;
    boolean connectionSuccess = false;

    try {
      channel = SocketChannel.open();
      LWTThreadUtil.setBlocked();
      channel.socket().connect(dest, defSocketConnectTimeout);
      T conn = addConnection(channel, listener, false);
      connectionSuccess = true;
      return conn;
    } catch (UnresolvedAddressException uae) {
      log.error("",uae);
      log.info("{}",dest);
      throw new ConnectException(dest.toString());
    } finally {
      LWTThreadUtil.setNonBlocked();
      if (!connectionSuccess && channel != null) {
        try {
          channel.close();
        } catch (IOException e) {
          log.error("Could not close socketChannel {}" , channel, e);
        }
      }
    }
  }

  public T addConnection(SocketChannel channel, boolean serverside) throws SocketException, AuthFailedException {
    return addConnection(channel, null, serverside);
  }

  public T addConnection(SocketChannel channel, ConnectionListener listener, boolean serverside) throws SocketException, AuthFailedException {
    T connection;
    SelectorController<T> selectorController;

    channel.socket().setTcpNoDelay(tcpNoDelay);
    if (defReceiveBufferSize > 0) {
      channel.socket().setReceiveBufferSize(defReceiveBufferSize);
    }
    if (defSendBufferSize > 0) {
      channel.socket().setSendBufferSize(defSendBufferSize);
    }
    channel.socket().setSoTimeout(defSocketReadTimeout); // (for auth as the rest is non-blocking)

    String connInfo = channel.socket() != null ? channel.socket().toString() : "nullSock";
    AuthenticationResult authResult = authenticator.syncAuthenticate(channel.socket(), serverside,
        defAuthenticationTimeoutInMillisecond);
    Authenticator.checkForAuthFailure(authResult, connInfo, serverside, authenticator);

    if (logConnections) {
      log.info("AsyncBase addConnection: {} sBuf {} rBuf {}", channel, channel.socket().getSendBufferSize(),
          channel.socket().getReceiveBufferSize());
    }

    try {
      channel.configureBlocking(false);
    } catch (IOException ioe) {
      log.error("",ioe);
      throw new RuntimeException("Unable to configure non-blocking socket");
    }
    selectorController = cscAssigner.assignChannelToSelectorController(channel, selectorControllers);
    connection = connectionCreator.createConnection(channel, selectorController, listener, debug);

    if (authResult.isSuccessful()) {
      log.debug("Authenticator: authId[ {} ] is obtained in [ {} ] by [ {} ] for connection {}" ,authResult.getAuthenticatedId().get(),
                (serverside ? "ServerSide" : "ClientSide"), authenticator.getName()  , connInfo);
      connection.setAuthenticationResult(authResult);
    }
    connection.start();
    connectionManager.addConnection(connection);
    return connection;
  }

  /**
   * Assigned a new ServerSocketChannel to a selector and adds it to that
   * selector. Also, configures the channel as non-blocking.
   *
   * @param serverChannel
   * @throws IOException
   */
  protected void addServerChannel(ServerSocketChannel serverChannel) throws IOException {
    SelectorController<T> selectorController;

    serverChannel.configureBlocking(false);
    selectorController = cscAssigner.assignChannelToSelectorController(serverChannel, selectorControllers);
    selectorController.addServerChannel(serverChannel);
  }

  public void writeStats() {
    for (Connection c : connectionManager.getConnections()) {
      try {
        connectionStatsWriter.writeStats(c);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public ConnectionController getConnectionController(){
    return connectionManager;
  }

  void registerConnectionManager(){
    ConnectionManager.addManager(connectionManager);
  }
}
