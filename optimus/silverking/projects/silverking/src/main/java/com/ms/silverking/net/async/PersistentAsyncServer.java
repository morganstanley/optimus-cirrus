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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.time.RandomBackoff;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/** Maintains persistent TCP connections to other peers */
public class PersistentAsyncServer<T extends Connection>
    implements IncomingConnectionListener<T>, ConnectionListener {
  private final AsyncServer<T> asyncServer;
  private final ConcurrentMap<InetSocketAddress, T> connections =
      new ConcurrentHashMap<InetSocketAddress, T>();
  private final ConcurrentMap<InetSocketAddress, ReentrantLock> newConnectionLocks;
  private final boolean debug;
  private final BaseWorker<OutgoingAsyncMessage> asyncConnector;
  private final NewConnectionTimeoutController newConnectionTimeoutController;
  // Client servers will refuse incoming connections
  // All communication between a client and server is initiated by the client
  // the server may only ever reply on the channel opened by the client
  // it will not open a new connection in reply - the client rejects such incoming connections
  private final boolean isClient;
  private final SessionPolicyOnDisconnect sessionPolicyOnDisconnect;

  private static Logger log = LoggerFactory.getLogger(PersistentAsyncServer.class);

  private AddressStatusProvider addressStatusProvider;
  private SuspectAddressListener suspectAddressListener;
  private volatile boolean isRunning;
  private static final int connectionCreationAttemptTimeoutMS = 1000;
  private static final int connectionCreationMaximumTimeoutMS = 40 * 1000;
  private static final int maxConnectBackoffNum = 16;
  public static final NewConnectionTimeoutController defaultNewConnectionTimeoutController =
      new SimpleNewConnectionTimeoutController(
          maxConnectBackoffNum,
          connectionCreationAttemptTimeoutMS,
          connectionCreationMaximumTimeoutMS);

  private static final int initialConnectBackoffValue = 100;
  private static final int maxSendBackoffNum = 7;
  private static final int initialSendBackoffValue = 250;

  private static final int defaultSelectorControllers = 1;
  private static final String defaultSelectorControllerClass = "PAServer";

  public static final int useDefaultBacklog = 0;

  // Mainly used for test or inject cutomized AsyncServer
  PersistentAsyncServer(
      AsyncServer<T> asyncServer,
      NewConnectionTimeoutController newConnectionTimeoutController,
      BaseWorker<OutgoingAsyncMessage> asyncConnector,
      boolean debug,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID) {
    isRunning = true;
    this.debug = debug;
    this.newConnectionTimeoutController = newConnectionTimeoutController;
    this.asyncServer = asyncServer;
    newConnectionLocks = new ConcurrentHashMap<InetSocketAddress, ReentrantLock>();
    this.asyncConnector = asyncConnector;
    if (mqListener != null) {
      new ConnectionQueueWatcher(mqListener, mqUUID);
    }
    this.isClient = false;
    this.sessionPolicyOnDisconnect = SessionPolicyOnDisconnect.DoNothing;
    asyncServer.registerConnectionManager();
    // new ConnectionDebugger();
  }

  // FUTURE - Make target size and max size configurable via property
  private static final LWTPool defaultConnectorPool =
      LWTPoolProvider.createPool(
          LWTPoolParameters.create("ConnectorPool").targetSize(4).maxSize(16).workUnit(1));

  public static final void shutdownConnectorPool() {
    defaultConnectorPool.stop();
  }

  public PersistentAsyncServer(
      int port,
      int backlog,
      int numSelectorControllers,
      String controllerClass,
      ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController,
      LWTPool readerLWTPool,
      LWTPool writerLWTPool,
      LWTPool acceptorPool,
      LWTPool connectorPool,
      int selectionThreadWorkLimit,
      boolean enabled,
      boolean debug,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      boolean isClient)
      throws IOException {
    this(
        port,
        backlog,
        numSelectorControllers,
        controllerClass,
        connectionCreator,
        newConnectionTimeoutController,
        readerLWTPool,
        writerLWTPool,
        acceptorPool,
        connectorPool,
        selectionThreadWorkLimit,
        enabled,
        debug,
        mqListener,
        mqUUID,
        isClient,
        SessionPolicyOnDisconnect.DoNothing);
  }

  public PersistentAsyncServer(
      int port,
      int backlog,
      int numSelectorControllers,
      String controllerClass,
      ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController,
      LWTPool readerLWTPool,
      LWTPool writerLWTPool,
      LWTPool acceptorPool,
      LWTPool connectorPool,
      int selectionThreadWorkLimit,
      boolean enabled,
      boolean debug,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      boolean isClient,
      SessionPolicyOnDisconnect onDisconnect)
      throws IOException {
    isRunning = true;
    this.debug = debug;
    this.newConnectionTimeoutController = newConnectionTimeoutController;
    asyncServer =
        new AsyncServer<T>(
            port,
            backlog,
            numSelectorControllers,
            controllerClass,
            connectionCreator,
            this,
            readerLWTPool,
            writerLWTPool,
            acceptorPool,
            selectionThreadWorkLimit,
            enabled,
            debug);
    newConnectionLocks = new ConcurrentHashMap<InetSocketAddress, ReentrantLock>();
    asyncConnector = new AsyncConnector(connectorPool);
    if (mqListener != null) {
      new ConnectionQueueWatcher(mqListener, mqUUID);
    }
    this.isClient = isClient;
    this.sessionPolicyOnDisconnect = onDisconnect;
    log.info("On disconnect PersistentAsyncServer will {}", onDisconnect);

    // For server side optional logic of disconnecting all connections on node exclusion.
    // We want to keep a track of all server side ConnectionManager objects so that local
    // connections
    // originated from same VM can be skipped while doing disconnect.
    //
    // This check will also evaluate to be true for client applications which choose to
    // DoNothing if there is a connection disconnect. But adding the ConnectionManager
    // in the client side should be harmless operation for now.
    //
    // This check depending on multiple flags is not very clean so ideally we would need
    // some additional info e.g. context in which PersistentAsyncServer is created to
    // figure out whether it is created in a server or client context.
    if (isClient == false || sessionPolicyOnDisconnect == SessionPolicyOnDisconnect.DoNothing) {
      asyncServer.registerConnectionManager();
    }
    // new ConnectionDebugger();
  }

  // test hook
  PersistentAsyncServer(
      int port,
      int backlog,
      int numSelectorControllers,
      String controllerClass,
      ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController)
      throws IOException {
    this(
        port,
        backlog,
        numSelectorControllers,
        controllerClass,
        connectionCreator,
        newConnectionTimeoutController,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        defaultConnectorPool,
        SelectorController.defaultSelectionThreadWorkLimit,
        true,
        false,
        null,
        null,
        false);
  }

  // Test hook
  public PersistentAsyncServer(int port, ConnectionCreator<T> connectionCreator)
      throws IOException {
    this(
        port,
        useDefaultBacklog,
        defaultSelectorControllers,
        defaultSelectorControllerClass,
        connectionCreator,
        defaultNewConnectionTimeoutController);
  }

  // Test hook
  public PersistentAsyncServer(
      int port,
      ConnectionCreator<T> connectionCreator,
      int numSelectorControllers,
      String controllerClass)
      throws IOException {
    this(
        port,
        useDefaultBacklog,
        numSelectorControllers,
        controllerClass,
        connectionCreator,
        defaultNewConnectionTimeoutController);
  }

  public PersistentAsyncServer(
      int port,
      ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController,
      int numSelectorControllers,
      String controllerClass,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      int selectionThreadWorkLimit,
      boolean isClient)
      throws IOException {

    this(
        port,
        useDefaultBacklog,
        numSelectorControllers,
        controllerClass,
        connectionCreator,
        newConnectionTimeoutController,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        defaultConnectorPool,
        selectionThreadWorkLimit,
        false,
        false,
        mqListener,
        mqUUID,
        isClient);
  }

  public PersistentAsyncServer(
      int port,
      ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController,
      int numSelectorControllers,
      String controllerClass,
      MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID,
      int selectionThreadWorkLimit,
      boolean isClient,
      SessionPolicyOnDisconnect onDisconnectPolicy)
      throws IOException {

    this(
        port,
        useDefaultBacklog,
        numSelectorControllers,
        controllerClass,
        connectionCreator,
        newConnectionTimeoutController,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool,
        defaultConnectorPool,
        selectionThreadWorkLimit,
        false,
        false,
        mqListener,
        mqUUID,
        isClient,
        onDisconnectPolicy);
  }

  //////////////////////////////////////////////////////////////////////

  public void enable() {
    asyncServer.enable();
  }

  public void shutdown() {
    for (Connection connection : connections.values()) {
      connection.close();
    }
    if (asyncServer != null) {
      asyncServer.shutdown();
    }
    isRunning = false;
  }

  public boolean isRunning() {
    return isRunning;
  }

  //////////////////////////////////////////////////////////////////////

  public int getPort() {
    return asyncServer.getPort();
  }

  public void setSuspectAddressListener(SuspectAddressListener suspectAddressListener) {
    this.suspectAddressListener = suspectAddressListener;
    asyncServer.setSuspectAddressListener(suspectAddressListener);
  }

  public void setAddressStatusProvider(AddressStatusProvider addressStatusProvider) {
    if (this.addressStatusProvider != null) {
      throw new RuntimeException("Unexpected mutation of addressStatusProvider");
    }
    this.addressStatusProvider = addressStatusProvider;
    /*
    // FUTURE - make this cleaner or consider removing
    suspectAddressListener = (SuspectAddressListener)addressStatusProvider;
    asyncServer.setSuspectAddressListener(suspectAddressListener);
    */
  }

  //////////////////////////////////////////////////////////////////////

  public void sendAsynchronous(
      AddrAndPort dest, Object data, UUIDBase uuid, AsyncSendListener listener, long deadline)
      throws UnknownHostException {
    sendAsynchronous(dest.toInetSocketAddress(), data, uuid, listener, deadline);
  }

  public void sendAsynchronous(
      InetSocketAddress dest,
      Object data,
      UUIDBase uuid,
      AsyncSendListener listener,
      long deadline) {
    try {
      Connection connection;

      connection = getEstablishedConnection(dest);
      // if (DebugUtil.delayedDebug()) {
      //    System.out.println("getEstablishedConnection: "+ connection);
      // }
      if (connection != null) {
        connection.sendAsynchronous(data, uuid, listener, deadline);
      } else {
        newConnectionSendAsynchronous(dest, data, uuid, listener, deadline);
      }
    } catch (IOException ioe) {
      log.warn("Send failed: {}, {}", uuid, dest, ioe);
      if (listener != null && uuid != null) {
        listener.failed(uuid);
      }
      informSuspectAddressListener(dest);
    }
  }

  private void informSuspectAddressListener(InetSocketAddress addr) {
    asyncServer.informSuspectAddressListener(addr);
  }

  public void sendSynchronous(
      AddrAndPort dest, Object data, UUIDBase uuid, AsyncSendListener listener, long deadline)
      throws IOException {
    sendSynchronous(dest.toInetSocketAddress(), data, uuid, listener, deadline);
  }

  public void sendSynchronous(
      InetSocketAddress dest, Object data, UUIDBase uuid, AsyncSendListener listener, long deadline)
      throws IOException {
    Connection connection;
    RandomBackoff backoff;

    if (uuid == null) {
      uuid = new UUIDBase();
      log.debug("null send uuid, picking new uuid {}", uuid);
      listener = null;
    }
    backoff = null;
    while (true) {
      try {
        connection = getConnectionFast(dest, deadline, null);
      } catch (AuthFailedException e) {
        throw new IOException(e);
      }
      try {
        connection.sendSynchronous(data, uuid, listener, deadline);
        return;
      } catch (IOException ioe) {
        log.warn("IO Exception during sendSynchronous to {}", dest, ioe);
        connections.remove(dest);
        informSuspectAddressListener(dest);
        if (backoff == null) {
          backoff = new RandomBackoff(maxSendBackoffNum, initialSendBackoffValue, deadline);
        }
        if (!backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          listener.failed(uuid);
          throw ioe;
        }
      }
    }
  }

  public void send(
      AddrAndPort dest,
      Object data,
      boolean synchronous,
      UUIDBase uuid,
      AsyncSendListener listener,
      long deadline)
      throws IOException {
    send(dest.toInetSocketAddress(), data, synchronous, uuid, listener, deadline);
  }

  public void send(
      InetSocketAddress dest,
      Object data,
      boolean synchronous,
      UUIDBase uuid,
      AsyncSendListener listener,
      long deadline)
      throws IOException {
    if (synchronous) {
      sendSynchronous(dest, data, uuid, listener, deadline);
    } else {
      sendAsynchronous(dest, data, uuid, listener, deadline);
    }
  }

  public void send(AddrAndPort dest, Object data, boolean synchronous, long deadline)
      throws IOException {
    send(dest.toInetSocketAddress(), data, synchronous, deadline);
  }

  public void send(InetSocketAddress dest, Object data, boolean synchronous, long deadline)
      throws IOException {
    UUIDBase uuid;

    uuid = new UUIDBase();
    if (synchronous) {
      sendSynchronous(dest, data, uuid, null, deadline);
    } else {
      sendAsynchronous(dest, data, null, null, deadline);
    }
  }

  //////////////////////////////////////////////////////////////////////

  private Connection getEstablishedConnection(InetSocketAddress dest) throws ConnectException {
    return connections.get(dest);
  }

  public void ensureConnected(AddrAndPort dest) throws ConnectException, AuthFailedException {
    getConnection(
        dest,
        SystemTimeUtil.skSystemTimeSource.absTimeMillis()
            + newConnectionTimeoutController.getMaxRelativeTimeoutMillis(dest));
  }

  public Connection getConnection(AddrAndPort dest, long deadline)
      throws ConnectException, AuthFailedException {
    try {
      return getConnectionFast(dest.toInetSocketAddress(), deadline, null);
    } catch (UnknownHostException uhe) {
      throw new RuntimeException(uhe);
    }
  }

  private Connection getConnectionFast(InetSocketAddress dest, long deadline, String context)
      throws ConnectException, AuthFailedException {
    Connection connection;

    connection = connections.get(dest);
    if (connection == null) {
      connection = getConnectionSlow(dest, deadline, context);
    }
    return connection;
  }

  private Connection getConnectionSlow(InetSocketAddress dest, long deadline, String context)
      throws ConnectException, AuthFailedException {
    Connection connection;
    ReentrantLock destNewConnectionLock;

    destNewConnectionLock = newConnectionLocks.get(dest);
    if (destNewConnectionLock == null) {
      newConnectionLocks.putIfAbsent(dest, new ReentrantLock());
      destNewConnectionLock = newConnectionLocks.get(dest);
    }
    destNewConnectionLock.lock();
    try {
      connection = connections.get(dest);
      if (connection == null) {
        connection = createConnection(dest, deadline, context);
      }
      return connection;
    } finally {
      destNewConnectionLock.unlock();
    }
  }

  /**
   * only called when the given connection does not exist
   *
   * @return
   */
  private Connection createConnection(InetSocketAddress dest, long deadline, String context)
      throws ConnectException, AuthFailedException {
    RandomBackoff backoff;
    IPAndPort _dest;

    if (log.isInfoEnabled()) {
      String tag = ((isClient) ? "[Client]" : "") + "createConnection: ";
      log.debug("{} {}", tag, dest);
    }

    _dest = new IPAndPort(dest);

    deadline =
        Math.min(
            deadline,
            SystemTimeUtil.skSystemTimeSource.absTimeMillis()
                + newConnectionTimeoutController.getMaxRelativeTimeoutMillis(_dest));

    backoff = null;
    while (isRunning) {
      if (addressStatusProvider != null
          && !addressStatusProvider.isAddressStatusProviderThread(context)
          && !addressStatusProvider.isHealthy(dest)) {
        throw new UnhealthyConnectionAttemptException(
            "Connection attempted to unhealthy address: " + dest);
      }

      try {
        T connection;

        connection = asyncServer.newOutgoingConnection(dest, this);
        connections.putIfAbsent(dest, connection);
        if (suspectAddressListener != null) {
          suspectAddressListener.removeSuspect(dest);
        }
        return connection;
      } catch (AuthFailedException afe) {
        log.error("", afe);
        if (afe.isRetryable()) {
          if (backoff == null) {
            backoff =
                new RandomBackoff(
                    newConnectionTimeoutController.getMaxAttempts(_dest),
                    initialConnectBackoffValue,
                    // TODO (OPTIMUS-0000): ultimately this shouldn't hard-code the 1st attempt as
                    // the value can change per attempt, but
                    // it's good enough for now.
                    newConnectionTimeoutController.getRelativeTimeoutMillisForAttempt(_dest, 1),
                    deadline);
          }
          if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline
              && !backoff.maxBackoffExceeded()) {
            backoff.backoff();
          } else {
            if (suspectAddressListener != null) {
              suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
            }
            log.error(
                "Authentication fails after run out of retries [currTime= {} ,deadline= {} ] and {}",
                SystemTimeUtil.skSystemTimeSource.absTimeMillis(),
                deadline,
                backoff,
                afe);
            throw afe;
          }
        } else {
          throw afe;
        }
      } catch (ConnectException ce) {
        if (addressStatusProvider != null
            && addressStatusProvider.isAddressStatusProviderThread()) {
          throw new ConnectException("addressStatusProvider failed to connect: " + dest);
        }
        if (suspectAddressListener != null) {
          suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
        }
        log.info("{}  {}", SystemTimeUtil.skSystemTimeSource.absTimeMillis(), deadline);
        log.error("{} {}", SystemTimeUtil.skSystemTimeSource.absTimeMillis(), deadline);
        log.info("{} {}", ce, dest);
        log.error("", ce);
        if (backoff == null) {
          backoff =
              new RandomBackoff(
                  newConnectionTimeoutController.getMaxAttempts(_dest),
                  initialConnectBackoffValue,
                  // TODO (OPTIMUS-0000): ultimately this shouldn't hard-code the 1st attempt as the
                  // value can change per attempt, but
                  // it's good enough for now.
                  newConnectionTimeoutController.getRelativeTimeoutMillisForAttempt(_dest, 1),
                  deadline);
        }
        if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline
            && !backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          informSuspectAddressListener(dest);
          throw ce;
        }
      } catch (SocketTimeoutException ste) {
        if (addressStatusProvider != null
            && addressStatusProvider.isAddressStatusProviderThread()) {
          throw new ConnectException("addressStatusProvider failed to connect: " + dest);
        }
        log.info("{} {}", System.currentTimeMillis(), deadline);
        log.error("{} {}", System.currentTimeMillis(), deadline);
        log.info("{} {}", ste, dest);
        log.error("", ste);
        if (backoff == null) {
          backoff =
              new RandomBackoff(
                  newConnectionTimeoutController.getMaxAttempts(_dest),
                  initialConnectBackoffValue,
                  // TODO (OPTIMUS-0000): ultimately this shouldn't hard-code the 1st attempt as the
                  // value can change per attempt, but
                  // it's good enough for now.
                  newConnectionTimeoutController.getRelativeTimeoutMillisForAttempt(_dest, 1),
                  deadline);
        }
        if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline
            && !backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          if (suspectAddressListener != null) {
            suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
          }
          throw new ConnectException(ste.toString());
        }
      } catch (IOException ioe) {
        log.info("{} {}", ioe, dest);
        log.error("", ioe);
        informSuspectAddressListener(dest);
        throw new RuntimeException("Unexpected IOException", ioe);
      }
    }
    return null;
  }

  @Override
  public void incomingConnection(T connection) {
    // Clients refuse incoming connections - all communication must occur on channels opened by the
    // client
    // the server may not open a separate connection
    if (this.isClient) {
      throw new RuntimeException("Client refused an unexpected incoming connection");
    }
    connections.putIfAbsent(connection.getRemoteSocketAddress(), connection);
    connection.setConnectionListener(this);
    if (suspectAddressListener != null) {
      suspectAddressListener.removeSuspect(connection.getRemoteSocketAddress());
    }
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public void disconnected(
      Connection connection, InetSocketAddress remoteAddr, Object disconnectionData) {
    log.info("disconnected {}   {}", connection, remoteAddr);
    removeAndCloseConnection(connection);
    if (SessionPolicyOnDisconnect.CloseSession == sessionPolicyOnDisconnect) {
      log.info("shutting down PersistentAsyncServer on disconnect");
      shutdown();
    }
  }

  public void removeAndCloseConnection(Connection connection) {
    log.info("removeAndCloseConnection {}", connection);
    if (connection.getRemoteSocketAddress() != null) {
      connections.remove(connection.getRemoteSocketAddress());
    }
  }

  /*
  @Override
  public void sendFailed(Connection connection, Object data) {
      Log.fine("sendFailed", connection);
      System.out.println("Send failed: "+ connection +":"+ data);
      connections.remove(connection.getChannel().socket().getRemoteSocketAddress());
  }

  @Override
  public void sendSucceeded(Connection connection, Object data) {
  }
  */

  //////////////////////////////////////////////////////////////////////

  private void newConnectionSendAsynchronous(
      InetSocketAddress dest,
      Object data,
      UUIDBase uuid,
      AsyncSendListener listener,
      long deadline) {
    asyncConnector.addWork(new OutgoingAsyncMessage(dest, data, uuid, listener, deadline));
  }

  class OutgoingAsyncMessage {
    private final InetSocketAddress dest;
    private final Object data;
    private final UUIDBase uuid;
    private final AsyncSendListener listener;
    private final long deadline;
    private final String context;

    public OutgoingAsyncMessage(
        InetSocketAddress dest,
        Object data,
        UUIDBase uuid,
        AsyncSendListener listener,
        long deadline) {
      this.dest = dest;
      this.data = data;
      this.uuid = uuid;
      this.listener = listener;
      this.deadline = deadline;
      context = Thread.currentThread().getName();
    }

    public void failed() {
      if (listener != null) {
        log.debug("Informing of failure: {}", this);
        listener.failed(uuid);
      } else {
        log.debug("No listener: {}", this);
      }
    }

    public InetSocketAddress getDest() {
      return dest;
    }

    public Object getData() {
      return data;
    }

    public UUIDBase getUUID() {
      return uuid;
    }

    public AsyncSendListener getListener() {
      return listener;
    }

    public long getDeadline() {
      return deadline;
    }

    public String getContext() {
      return context;
    }

    public String toString() {
      return dest + ":" + data + ":" + uuid + ":" + listener + ":" + deadline;
    }
  }

  class AsyncConnector extends BaseWorker<OutgoingAsyncMessage> {
    public AsyncConnector(LWTPool lwtPool) {
      super(
          lwtPool, true,
          0); // disallow direct calls to force connections to not occur in a receiving thread
    }

    @Override
    public void doWork(OutgoingAsyncMessage msg) {
      if (AsyncGlobals.debug && debug) {
        log.debug("AsyncConnector.doSend {}", msg.getUUID());
      }
      try {
        Connection connection;

        connection = getConnectionFast(msg.getDest(), msg.getDeadline(), msg.getContext());
        connection.sendAsynchronous(
            msg.getData(), msg.getUUID(), msg.getListener(), msg.getDeadline());
      } catch (UnhealthyConnectionAttemptException ucae) {
        log.info("Attempted connect to unhealthy address: {}", msg.getDest());
        if (msg.getListener() != null && msg.getUUID() != null) {
          msg.getListener().failed(msg.getUUID());
        }
      } catch (AuthFailedException | IOException e) {
        msg.failed();
        log.info("{} {}", e, msg.getDest());
        log.error("", e);
        if (msg.getListener() != null && msg.getUUID() != null) {
          msg.getListener().failed(msg.getUUID());
        }
      }
    }
  }

  class ConnectionDebugger implements Runnable {
    private static final int debugIntervalMS = 10 * 1000;

    ConnectionDebugger() {
      new Thread(this).start();
    }

    private void debugConnections() {
      log.debug("");
      log.info("Connections:");
      for (T connection : connections.values()) {
        debugConnection(connection);
      }
      log.info("");
      LWTPoolProvider.defaultConcurrentWorkPool.debug();
      log.info("");
    }

    private void debugConnection(T connection) {
      log.info(connection.debugString());
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(debugIntervalMS);
          debugConnections();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void writeStats() {
    asyncServer.writeStats();
  }

  public ConnectionController getConnectionController() {
    return asyncServer.getConnectionController();
  }

  class ConnectionQueueWatcher implements Runnable {
    private final UUIDBase uuid;
    private final MultipleConnectionQueueLengthListener listener;

    private static final int watchIntervalMS = 4 * 1000;

    ConnectionQueueWatcher(MultipleConnectionQueueLengthListener listener, UUIDBase uuid) {
      this.listener = listener;
      this.uuid = uuid;
      new Thread(this, "ConnectionQueueWatcher").start();
    }

    private void checkConnections() {
      int totalQueueLength;
      int longestQueueLength;
      Connection maxQueuedConnection;

      maxQueuedConnection = null;
      longestQueueLength = 0;
      totalQueueLength = 0;
      for (Connection connection : connections.values()) {
        long queueLength;

        queueLength = connection.getQueueLength();
        totalQueueLength += queueLength;
        if (queueLength > longestQueueLength) {
          maxQueuedConnection = connection;
        }
      }
      listener.queueLength(uuid, totalQueueLength, maxQueuedConnection);
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(watchIntervalMS);
          checkConnections();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
