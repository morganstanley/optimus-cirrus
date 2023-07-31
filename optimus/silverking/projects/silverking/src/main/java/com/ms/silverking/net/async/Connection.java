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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.InetSocketAddressComparator;
import com.ms.silverking.net.security.AuthenticationResult;
import com.ms.silverking.net.security.Authenticable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection state for TCP connection to remote socket. Specialized by subclasses
 * to optimize connections for specific types of communication.
 * <p>
 * AsyncBase is responsible for calling disconnect() when an IOException occurs. Thus the
 * reads and writes here do not need to call disconnect, but bubble fatal exceptions up
 * where AsyncBase can handle them.
 */
public abstract class Connection implements ChannelRegistrationWorker, Comparable<Connection>, Authenticable {
  protected final SocketChannel channel;
  private SelectionKey selectionKey;
  private final InetSocketAddress remoteSocketAddress;
  private final InetSocketAddress localSocketAddress;
  private final SelectorController<? extends Connection> selectorController;
  private ConnectionListener connectionListener;
  private final ConcurrentMap<UUIDBase, ActiveSend> activeBlockingSends;
  protected final boolean verbose;
  protected boolean connected;

  private static Logger log = LoggerFactory.getLogger(Connection.class);

  // Connection stats
  protected final AtomicLong cumulativeSends;
  protected final AtomicLong cumulativeReceives;

  private volatile boolean writeEnablePending;

  protected final boolean debug;

  static final boolean statsEnabled = false;

  // FUTURE - move the synchronous sending logic out of this class

  /*
   * NIO channels support one write at a time and one read at a time where the
   * single write and the single read may be concurrent with each other. These
   * locks are used to enforce this constraint. The channelWriteLock is only
   * ever held by a writer thread. The channelReadLock is only ever held by
   * a reader thread. These locks should never be held concurrently
   * by a single thread.
   */
  protected final Lock channelWriteLock;
  //protected final Lock channelReceiveLock;
  protected volatile boolean writing;
  /*
   * Protects internal connection accounting. For example, disconnection
   * logic. If held concurrently with channelWriteLock or channelReceiveLock,
   * this lock MUST be acquired AFTER the other lock and released BEFORE the
   * other lock.
   */
  protected final Lock connectionLock;

  public Connection(SocketChannel channel, SelectorController<? extends Connection> selectorController,
      ConnectionListener connectionListener, boolean verbose, boolean debug, boolean allowSynchronous) {
    this.channel = channel;
    this.selectorController = selectorController;
    this.connectionListener = connectionListener;
    remoteSocketAddress = (InetSocketAddress) channel.socket().getRemoteSocketAddress();
    localSocketAddress = (InetSocketAddress) channel.socket().getLocalSocketAddress();
    channelWriteLock = new ReentrantLock();
    //channelReceiveLock = new ReentrantLock();
    connectionLock = new ReentrantLock();
    if (allowSynchronous) {
      activeBlockingSends = new ConcurrentHashMap<UUIDBase, ActiveSend>();
    } else {
      activeBlockingSends = null;
    }
    connected = true;
    this.verbose = verbose;
    this.debug = debug;

    if (statsEnabled) {
      cumulativeSends = new AtomicLong();
      cumulativeReceives = new AtomicLong();
    } else {
      cumulativeSends = null;
      cumulativeReceives = null;
    }
  }

  public Connection(SocketChannel channel, SelectorController<? extends Connection> selectorController,
      ConnectionListener connectionListener) {
    this(channel, selectorController, connectionListener, true, false, false);
  }

  //////////////////////////////////////////////////////////////////////
  // Authentication and Authorization
  // * Authenticable
  protected AuthenticationResult authenticationResult = null;
  protected Optional<String> authenticatedUser;

  @Override
  public void setAuthenticationResult(AuthenticationResult authenticationResult) {
    this.authenticationResult = authenticationResult;
    this.authenticatedUser = authenticationResult.getAuthenticatedId();
  }

  @Override
  public Optional<AuthenticationResult> getAuthenticationResult() {
    return Optional.ofNullable(authenticationResult);
  }
  //////////////////////////////////////////////////////////////////////

  public void setConnectionListener(ConnectionListener connectionListener) {
    if (this.connectionListener != null) {
      log.info("Ignoring attempt to reset connection listener: {}", this);
    } else {
      this.connectionListener = connectionListener;
    }
  }

  public final SocketChannel getChannel() {
    return channel;
  }

  public final SelectorController<? extends Connection> getSelectorController() {
    return selectorController;
  }

  public final InetSocketAddress getLocalSocketAddress() {
    return localSocketAddress;
  }

  public final InetSocketAddress getRemoteSocketAddress() {
    return remoteSocketAddress;
  }


  public final IPAndPort getLocalIPAndPort() {
    return new IPAndPort(localSocketAddress);
  }

  public final IPAndPort getRemoteIPAndPort() {
    return new IPAndPort(remoteSocketAddress);
  }

  //////////////////////////////////////////////////////////////////////

  // FUTURE - consider locking, removing the write/read locks

  /**
   * Called by writer threads when a socket is available for writing.
   */
  public final void writeAllPending() throws IOException {
    if (AsyncGlobals.debug) {
      log.debug("Connection.writeAllPending");
    }
    if (!connected) {
      log.debug("writeAllPending() not connected");
      return;
    }
    // FUTURE - Consider: rather than locking, just return and allow the
    // current thread to pick this up
    channelWriteLock.lock();
    writing = true;
    try {
      writeEnablePending = false;
      // writeEnablePending could be set to true here; this will just
      // result in extra calls which are not harmful.
      writeAllPending_locked();
    } finally {
      channelWriteLock.unlock();
      writing = false;
    }
    if (AsyncGlobals.debug) {
      log.debug("out Connection.writeAllPending");
    }
  }

  /**
   * Implemented by subclasses to actually write pending data to the channel.
   * Upon invocation, the channelWriteLock will have already been locked, and
   * writes will have been deselected.
   *
   * @throws IOException
   */
  protected abstract long writeAllPending_locked() throws IOException;

  //////////////////////////////////////////////////////////////////////

  /**
   * Called by user threads to send data on this channel asynchronously.
   */
  public void sendAsynchronous(Object data, long deadline) throws IOException {
    sendAsynchronous(data, null, null, deadline);
  }

  /**
   * Called by user threads to send data on this channel asynchronously.
   */
  public abstract void sendAsynchronous(Object data, UUIDBase sendID, AsyncSendListener asyncSendListener,
      long deadline) throws IOException;

  /**
   * Called by user threads to send data on this channel asynchronously.
   * This is a single synchronous send attempt. Retries must be handled
   * outside of this method.
   *
   * @param data
   * @param sendID
   * @param asyncSendListener
   * @throws IOException
   */
  public void sendSynchronous(Object data, UUIDBase sendID, AsyncSendListener asyncSendListener, long deadline)
      throws IOException {
    ActiveSend activeSend;

    if (sendID == null) {
      throw new RuntimeException("null sendID");
    }
    activeSend = new ActiveSend();
    // Lock the connectionLock to ensure that we cannot add
    // an active send after the connection is disconnected.
    connectionLock.lock();
    try {
      if (!connected) {
        throw new IOException("Connection disconnected");
      }
      activeBlockingSends.put(sendID, activeSend);
    } finally {
      connectionLock.unlock();
    }
    // switching to non-locking approach
    // we ensure that there does not exist an interleaving of
    // events that can leave a blocking send in a disconnected
    // connection
    if (!connected) {
      throw new IOException("Connection disconnected");
    }
    activeBlockingSends.put(sendID, activeSend);
    if (!connected) {
      activeBlockingSends.remove(sendID);
      throw new IOException("Connection disconnected");
    }
    try {
      sendAsynchronous(data, sendID, asyncSendListener, deadline);
      activeSend.waitForCompletion();
      return;
    } finally {
      activeBlockingSends.remove(sendID);
    }
  }

  //////////////////////////////////////////////////////////////////////

  /**
   * Called by reader threads when incoming data is available from a
   * socket.
   */
  public final int read() throws IOException {
    if (!connected) {
      return 0;
    }
    if (AsyncGlobals.debug && debug && remoteSocketAddress.equals(InetAddress.getLocalHost())) {
      log.debug("channelReceiveLock.lock();");
    }
    // Only a single thread calls this for a given Connection, so lock is elided.
    //channelReceiveLock.lock();
    //try {
    int numRead;
    boolean okToRead;

    okToRead = false;
    try {
      if (AsyncGlobals.debug && debug) {
        log.debug("calling locked read()");
      }
      numRead = lockedRead();
      if (AsyncGlobals.debug && debug) {
        log.debug("numRead {}", numRead);
      }
      okToRead = numRead >= 0;
    } catch (IOException ioe) {
      if (AsyncGlobals.debug && debug) {
        log.debug("IOException");
      }
      // don't allow reads if we have hit an exception
      // we will disconnect instead
      throw ioe;
    } finally {
      if (okToRead) {
        enableReads();
      }
    }
    return numRead;
    //} finally {
    //    channelReceiveLock.unlock();
    //    if (AsyncGlobals.debug && debug) {
    //        Log.fine("channelReceiveLock.unlock();");
    //    }
    //}
  }

  /**
   * Implemented by subclasses to actually perform the read.
   *
   * @return
   * @throws IOException
   */
  protected abstract int lockedRead() throws IOException;

  //////////////////////////////////////////////////////////////////////

  public void start() {
    selectorController.addKeyChangeRequest(
        new NewKeyChangeRequest(channel, KeyChangeRequest.Type.ADD_AND_CHANGE_OPS, SelectionKey.OP_READ, this));
  }

  public void close() {
    selectorController.addKeyChangeRequest(
        new NewKeyChangeRequest(channel, KeyChangeRequest.Type.CANCEL_AND_CLOSE, this));
    // channel is closed by the SelectorController
  }

  //////////////////////////////////////////////////////////////////////

  private void addInterestOpsAsync(int newOps) {
    selectorController.addKeyChangeRequest(new KeyChangeRequest(channel, KeyChangeRequest.Type.ADD_OPS, newOps));
  }

  private void addInterestOps(int newOps) {
    selectorController.addSelectionKeyOps(selectionKey, newOps);
  }

  private void removeInterestOps(int removeOps) {
    selectorController.removeSelectionKeyOps(selectionKey, removeOps);
  }

  protected final void disableReads() {
    if (connected) {
      // Below is unused.
      // If a disconnect happens after this check, we catch that later
      /**/
      //selectorController.addKeyChangeRequest(new KeyChangeRequest(channel, KeyChangeRequest.Type.REMOVE_OPS,
      //       SelectionKey.OP_READ));
      /**/
      //removeInterestOps(SelectionKey.OP_READ);
    } else {
      log.info("Ignoring disconnected disable reads");
    }
  }

  protected final void enableReads() {
    if (connected) {
      // if a disconnect happens after this check, we catch that later
      //selectorController.addKeyChangeRequest(new KeyChangeRequest(channel, KeyChangeRequest.Type.ADD_OPS,
      //       SelectionKey.OP_READ));
      addInterestOps(SelectionKey.OP_READ);
    } else {
      log.info("Ignoring disconnected enable reads");
    }
  }

  protected final void enableWrites() {
    if (connected) {
      if (!writeEnablePending) {
        writeEnablePending = true;
        // if a disconnect happens after this check, we catch that later
        addInterestOpsAsync(SelectionKey.OP_WRITE);
        // FUTURE - below fails; determine why we need the async version
        //addInterestOps(SelectionKey.OP_WRITE);
      }
    } else {
      log.info("Ignoring disconnected enable writes");
    }
  }
    
    /*
    protected final void enableWritesAsync() {
        if (connected) {
            // if a disconnect happens after this check, we catch that later
            selectorController.addKeyChangeRequest(new KeyChangeRequest(channel, KeyChangeRequest.Type.ADD_OPS,
                    SelectionKey.OP_WRITE));
        } else {
            Log.info("Ignoring disconnected enable writes");
        }
    }
    */

  protected final void enableWritesIfNotWriting() {
    //boolean    locked;

    //locked = channelWriteLock.tryLock();
    //locked = !writing;
    //try {
    //if (true || locked) {  // TODO (OPTIMUS-0000): determine why we must always enable
    enableWrites();
    //}
    //} finally {
    //    if (locked) {
    //        channelWriteLock.unlock();
    //    }
    //}
  }

  //////////////////////////////////////////////////////////////////////

  public boolean isConnected() {
    return connected;
  }

  /**
   * Perform subclass specific disconnect work after performing general
   * disconnection work, but before notifying the connectionListener.
   * connectionLock is locked during this call.
   */
  protected Object disconnect_locked() {
    return null;
  }

  /**
   * Called by AsyncBase when a read or write encounters an IOException.
   */
  void disconnect() {
    InetSocketAddress remoteAddr;
    Object disconnectionData;

    if (!connected) {
      return;
    }
    // multiple sets to false are ok here
    selectorController.addKeyChangeRequest(new KeyChangeRequest(channel, KeyChangeRequest.Type.CANCEL_AND_CLOSE));
    connected = false;
    connectionLock.lock();
    try {
      Socket socket;

      socket = channel.socket();
      if (socket != null) {
        remoteAddr = (InetSocketAddress) socket.getRemoteSocketAddress();
      } else {
        remoteAddr = null;
      }
      try {
        channel.close();
      } catch (IOException ioe) {
        log.info("{} " , remoteSocketAddress, ioe);
      }
      disconnectionData = disconnect_locked();
    } finally {
      connectionLock.unlock();
    }
    if (activeBlockingSends != null) {
      releaseActiveSends();
    }
    if (connectionListener != null) {
      connectionListener.disconnected(this, remoteAddr, disconnectionData);
    }
  }

  /**
   * After a disconnection, release all pending activeSends.
   */
  private void releaseActiveSends() {
    IOException ioe;

    assert connected == false; // for assertion purposes, we allow unlocked access
    ioe = new IOException("Channel closed" + remoteSocketAddress);
    for (ActiveSend activeSend : activeBlockingSends.values()) {
      activeSend.setException(ioe);
    }
  }

  //////////////////////////////////////////////////////////////////////

  /**
   * Must be called if a send fails.
   */
  protected void sendFailed(OutgoingData data) {
    ActiveSend activeSend;
    UUIDBase sendUUID;

    if (verbose) {
      log.info("sendFailed {}", remoteSocketAddress);
      data.displayForDebug();
    }
    sendUUID = data.getSendUUID();
    if (sendUUID != null && activeBlockingSends != null) {
      activeSend = activeBlockingSends.get(sendUUID);
    } else {
      activeSend = null;
    }
    if (activeSend != null) {
      activeSend.setException(new IOException("send failed"));
    } else {
      if (log.isDebugEnabled()) {
        log.debug("sendFailed() not an active send {}  {}", data.getSendUUID() , remoteSocketAddress);
      }
    }
    data.failed();
  }

  /**
   * Must be called if a send times out. This happens when a send
   * deadline is not met.
   */
  protected void sendTimedOut(OutgoingData data, long lastPollTime, long currPollTime, int currQueueSize) {
    ActiveSend activeSend;
    UUIDBase sendUUID;

    data.displayForDebug();
    sendUUID = data.getSendUUID();
    log.warn("sendTimedOut {} sendUUid {}", remoteSocketAddress, sendUUID);
    if (sendUUID != null && activeBlockingSends != null) {
      activeSend = activeBlockingSends.get(sendUUID);
      log.warn("activeSend in map {} map.size()={}", activeSend, activeBlockingSends.size());
    } else {
      activeSend = null;
    }
    if (activeSend != null) {
      activeSend.setException(new IOException("send timed out"));
    } else {
      log.warn("sendTimedOut() not an active send {}  {}", data.getSendUUID(), remoteSocketAddress);
    }
    data.timeout(lastPollTime, currPollTime, currQueueSize);
  }

  /**
   * Must be called if a send succeeds.
   *
   * @param data
   */
  protected void sendSucceeded(OutgoingData data) {
    ActiveSend activeSend;
    UUIDBase sendUUID;

    if (AsyncGlobals.debug && debug) {
      log.debug("sendSucceeded{}", remoteSocketAddress);
    }
    sendUUID = data.getSendUUID();
    if (sendUUID != null && activeBlockingSends != null) {
      activeSend = activeBlockingSends.get(sendUUID);
    } else {
      activeSend = null;
    }
    if (activeSend != null) {
      activeSend.setSentSuccessfully();
    } else {
      if (AsyncGlobals.debug && debug && log.isDebugEnabled()) {
        log.debug("sendSucceeded() not an active send {}  {}", data.getSendUUID() ,remoteSocketAddress);
      }
    }
    data.successful();
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public void channelRegistered(SelectionKey key) {
    synchronized (this) {
      key.attach(this);
      if (this.selectionKey != null) {
        log.info("Duplicate channel registration!");
      }
      this.selectionKey = key;
    }
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public int compareTo(Connection other) {
    return InetSocketAddressComparator.instance.compare(getRemoteSocketAddress(), other.getRemoteSocketAddress());
  }

  //////////////////////////////////////////////////////////////////////

  public String toString() {
    return "Connection:" + remoteSocketAddress + " " + channel.socket().getLocalPort();
  }

  public String debugString() {
    return toString() + ":" + selectorController.interestOpsDebugString(channel);
  }

  public String statString() {
    return String.format("%d:%d", cumulativeSends.longValue(), cumulativeReceives.longValue());
  }

  public long getQueueLength() {
    return 0;
  }

  public Optional<String> getAuthenticatedUser() {
    return this.authenticatedUser;
  }
}
