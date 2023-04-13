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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.BaseWorker;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Contains a single thread that monitors an NIO Selector passing events
 * to the appropriate worker.
 * <p>
 * Note that multiple SelectorControllers may share output workers. This
 * allows multiple selection threads to be used to pump events to common
 * event processing workers.
 * <p>
 * Any given Connection is only registered with a single SelectorController.
 */
public class SelectorController<C extends Connection> implements Runnable {
  private final Selector selector;

  private static Logger log = LoggerFactory.getLogger(SelectorController.class);

  private final BaseWorker<Triple<ServerSocketChannel, SelectorController, SelectionKey>> acceptWorker;
  private final BaseWorker<C> connectWorker;
  private final BaseWorker<C> readWorker;
  private final BaseWorker<C> writeWorker;
  private final SelectorThread selectorThread;

  private final int selectionThreadWorkLimit;

  private final Queue<NewServerSocketChannel> newServerChannels;
  private final Queue<KeyChangeRequest> keyChangeRequests;
  private final boolean debug;

  private volatile boolean wakeupPending;

  private volatile boolean running;

  private static final String threadBaseName = "SelectorController";

  private static final boolean allowReceiveWorkInSelectionThread = true;
  private static final boolean allowSendWorkInSelectionThread = true;

  public static final int NO_SELECTION_THREAD_WORK_LIMIT = Integer.MAX_VALUE;
  public static final int defaultSelectionThreadWorkLimit = NO_SELECTION_THREAD_WORK_LIMIT;

  private static ConcurrentMap<String, AtomicInteger> ccSelectorThreadIDs = new ConcurrentHashMap<>();

  public SelectorController(BaseWorker<Triple<ServerSocketChannel, SelectorController, SelectionKey>> acceptWorker,
      BaseWorker<C> connectWorker, BaseWorker<C> readWorker, BaseWorker<C> writeWorker, String controllerClass,
      int selectionThreadWorkLimit, boolean debug) throws IOException {
    AtomicInteger selectorThreadID;

    this.acceptWorker = acceptWorker;
    this.connectWorker = connectWorker;
    this.readWorker = readWorker;
    this.writeWorker = writeWorker;
    this.selectionThreadWorkLimit = selectionThreadWorkLimit;
    this.debug = debug;
    selector = Selector.open();
    newServerChannels = new ConcurrentLinkedQueue<NewServerSocketChannel>();
    keyChangeRequests = new ConcurrentLinkedQueue<KeyChangeRequest>();
    running = true;
    // FUTURE - consider making this an report that it is an LWT thread
    // to allow direct calls when the selector is idle
    ccSelectorThreadIDs.putIfAbsent(controllerClass, new AtomicInteger());
    selectorThreadID = ccSelectorThreadIDs.get(controllerClass);
    selectorThread = new SelectorThread(this,
        threadBaseName + "." + controllerClass + "." + selectorThreadID.getAndIncrement());
    selectorThread.start();
  }

  // ////////////////////////////////////////////////////////////////////

  /**
   * Shutdown simply terminates the selection thread.
   */
  public void shutdown() {
    running = false;
    try {
      selector.close();
    } catch (IOException e) {
      log.info("IOException when shutting down SelectorController", e);
    }
  }

  //////////////////////////////////////////////////////////////////////

  Selector getSelector() {
    return selector;
  }

  //////////////////////////////////////////////////////////////////////

  private boolean inThisSelectorThread() {
    return Thread.currentThread() == selectorThread;
  }

  //////////////////////////////////////////////////////////////////////

  private void displayKeys() {
    log.debug("   {}", selector.keys().size());
    for (SelectionKey key : selector.keys()) {
      StringBuilder sb;

      sb = new StringBuilder();
      sb.append('\t');
      sb.append(key);
      sb.append('\t');
      sb.append(key.attachment());
      sb.append('\t');
      sb.append(keyStatusString(key));
      log.debug("{}",sb);
    }
  }

  private String keyStatusString(SelectionKey key) {
    StringBuilder sb;

    sb = new StringBuilder();
    if (key.isValid()) {
      sb.append(key.isAcceptable() ? 'A' : 'a');
      sb.append(key.isConnectable() ? 'C' : 'c');
      sb.append(key.isReadable() ? 'R' : 'r');
      sb.append(key.isWritable() ? 'W' : 'w');
      sb.append(key.interestOps());
    } else {
      return "invalid";
    }
    return sb.toString();
  }

  private void doSelection() throws IOException {
    Iterator<SelectionKey> selectedKeys;
    int keyIndex;
    boolean workInSelectionThread;

    // Wait for an event one of the registered channels
    if (AsyncGlobals.debug && debug && log.isDebugEnabled()) {
      displayKeys();
    }
    if (AsyncGlobals.debug && debug) {
      log.debug("Calling select()");
    }

    notifySelect();
    if (running && !wakeupPending && selector.isOpen()) {
      selector.select();
    }
    // Right here somebody could have wanted a wakeup, but this will be ignored because pending was true.
    // This is not a problem, however, since we are now now awake and doing the processing that the
    // wakeup was needed for.
    wakeupPending = false;

    if (AsyncGlobals.debug && debug) {
      log.debug("Back from select()");
    }

    if (!running || !selector.isOpen()) {
      log.debug("The selector has already been closed; Leaving selection...");
    } else {
      notifyDoSelectHandle();
      // Iterate over the set of keys for which events are available
      selectedKeys = selector.selectedKeys().iterator();
      if (AsyncGlobals.debug && debug) {
        log.debug("Iterating through keys");
      }
      keyIndex = 0;
      while (selectedKeys.hasNext()) {
        SelectionKey key;

        key = (SelectionKey) selectedKeys.next();
        if (AsyncGlobals.debug && debug) {
          log.debug("key: {}", key);
        }
        selectedKeys.remove();
        workInSelectionThread = keyIndex++ < selectionThreadWorkLimit;
        //workInSelectionThread = true; // FUTURE - think about this and make configurable
        //workInSelectionThread = onFirstKey && !selectedKeys.hasNext();
        //selectorThread.setAllowBlocking(workInSelectionThread);
        if (key.isValid()) {
          // Handle the event
          int readyOps;

          readyOps = key.readyOps();
          if ((readyOps & SelectionKey.OP_READ) != 0) {
            C attachment;

            attachment = (C) key.attachment();
            if (AsyncGlobals.debug && debug) {
              log.debug("readQueue.put: {}", attachment);
            }
            removeSelectionKeyOps(key, SelectionKey.OP_READ);
            if (workInSelectionThread && allowReceiveWorkInSelectionThread) {
              readWorker.doWork(attachment);
            } else {
              readWorker.addWork(attachment, 0);
            }
          } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
            C attachment;

            attachment = (C) key.attachment();
            if (AsyncGlobals.debug && debug) {
              log.debug("writeQueue.put: {}", attachment);
            }
            removeSelectionKeyOps(key, SelectionKey.OP_WRITE);
            if (workInSelectionThread && allowSendWorkInSelectionThread) {
              writeWorker.doWork(attachment);
            } else {
              writeWorker.addWork(attachment, 0);
            }
          } else if (key.isAcceptable()) {
            if (AsyncGlobals.debug && debug) {
              log.debug("acceptQueue.put: {}", key.channel());
            }
            removeSelectionKeyOps(key, SelectionKey.OP_ACCEPT);
            acceptWorker.addWork(Triple.of((ServerSocketChannel) key.channel(), this, key));
          } else if (key.isConnectable()) {
            C attachment;

            attachment = (C) key.attachment();
            if (AsyncGlobals.debug && debug) {
              log.debug("connectQueue.put: {}", attachment);
            }
            removeSelectionKeyOps(key, SelectionKey.OP_CONNECT);
            connectWorker.addWork(attachment);
          }
        }
      }
      if (AsyncGlobals.debug && debug) {
        log.debug("Done iterating through keys");
      }
      if (AsyncGlobals.debug && debug && log.isDebugEnabled()) {
        log.debug("keys after selection");
        displayKeys();
        log.debug("Leaving selection");
      }
    }
  }

  // ////////////////////////////////////////////////////////////////////

  // note - SocketChannels are added via the key change request queue

  public void addServerChannel(ServerSocketChannel serverChannel) {
    addServerChannel(serverChannel, null);
  }

  public void addServerChannel(ServerSocketChannel serverChannel, ChannelRegistrationWorker crWorker) {
    if (AsyncGlobals.debug && debug) {
      log.debug("addServerChannel: {}", serverChannel);
    }
    newServerChannels.add(new NewServerSocketChannel(serverChannel, crWorker));
    selector.wakeup();
    // _addServerChannel(serverChannel);
  }

  private void addNewServerChannels() {
    NewServerSocketChannel channel;

    if (AsyncGlobals.debug && debug) {
      log.debug("addNewServerChannels");
    }
    do {
      channel = newServerChannels.poll();
      if (channel != null) {
        _addServerChannel(channel);
      }
    } while (channel != null);
  }

  private void _addServerChannel(NewServerSocketChannel newChannel) {
    try {
      ServerSocketChannel channel;
      SelectionKey key;
      ChannelRegistrationWorker crWorker;

      channel = newChannel.getServerSocketChannel();
      if (AsyncGlobals.debug && debug) {
        log.debug("register new server channel: {}", channel);
      }
      crWorker = newChannel.getChannelRegistrationWorker();
      if (crWorker != null) {
        // FUTURE - possibly change the name here to reflect the fact
        // that this is called before actual registration
        crWorker.channelRegistered(channel.keyFor(selector));
      }
      channel.register(selector, SelectionKey.OP_ACCEPT);
      if (AsyncGlobals.debug && debug) {
        log.debug("channel.isRegistered(): {}", channel.isRegistered());
        log.debug("{} ", channel.keyFor(selector).interestOps());
      }
      selector.wakeup();
    } catch (ClosedChannelException cce) {
      log.info("{}",cce);
    }
  }

  // ////////////////////////////////////////////////////////////////////

  private void processKeyChangeRequests() {
    KeyChangeRequest keyChangeRequest;

    if (AsyncGlobals.debug && debug) {
      log.debug("processKeyChangeRequests");
    }
    do {
      keyChangeRequest = keyChangeRequests.poll();
      if (keyChangeRequest != null) {
        try {
          processKeyChangeRequest(keyChangeRequest);
        } catch (CancelledKeyException cke) {
          log.info("Ignoring CancelledKeyException");
        }
      }
    } while (keyChangeRequest != null);
  }

  private void processKeyChangeRequest(KeyChangeRequest keyChangeRequest) {
    SelectionKey key;
    SelectableChannel channel;

    switch (keyChangeRequest.getType()) {
    case ADD_AND_CHANGE_OPS:
      NewKeyChangeRequest newKeyChangeRequest;
      ChannelRegistrationWorker crWorker;

      if (AsyncGlobals.debug && debug) {
        log.debug("ADD_AND_CHANGE_OPS");
      }
      try {
        keyChangeRequest.getChannel().register(selector, keyChangeRequest.getNewOps());
      } catch (ClosedChannelException cce) {
        log.error("",cce);
      }
      newKeyChangeRequest = (NewKeyChangeRequest) keyChangeRequest;
      crWorker = newKeyChangeRequest.getChannelRegistrationWorker();
      if (crWorker != null) {
        crWorker.channelRegistered(keyChangeRequest.getChannel().keyFor(selector));
      }
      break;
    case ADD_OPS:
      if (AsyncGlobals.debug && debug) {
        log.debug("ADD_OPS ");
      }
      key = keyChangeRequest.getChannel().keyFor(selector);
      if (key == null) {
        log.info(
            "Unexpected null keyChangeRequest.getChannel().keyFor(selector): " + keyChangeRequest + " " + selector);
      } else {
        synchronized (key) {
          key.interestOps(key.interestOps() | keyChangeRequest.getNewOps());
        }
      }
      if (AsyncGlobals.debug && debug) {
        log.debug("done ADD_OPS: {}", key.interestOps() | keyChangeRequest.getNewOps());
      }
      break;
    case REMOVE_OPS:
      if (AsyncGlobals.debug && debug) {
        log.debug("REMOVE_OPS");
      }
      key = keyChangeRequest.getChannel().keyFor(selector);
      if (key == null) {
        log.info(
            "Unexpected null keyChangeRequest.getChannel().keyFor(selector): {} {}" , keyChangeRequest, selector);
      } else {
        synchronized (key) {
          key.interestOps(key.interestOps() & ~keyChangeRequest.getNewOps());
        }
      }
      break;
    case CANCEL_AND_CLOSE:
      if (AsyncGlobals.debug && debug) {
        log.debug("CANCEL_AND_CLOSE");
      }
      channel = keyChangeRequest.getChannel();
      key = channel.keyFor(selector);
      if (key != null) {
        key.cancel();
      }
      try {
        channel.close();
      } catch (IOException ioe) {
        log.error("",ioe);
      }
      break;
    default:
      throw new RuntimeException("panic");
    }
  }

  //public void selectionKeysChanged() {
  //    wakeupSelector();
  //}

  public void addKeyChangeRequest(KeyChangeRequest keyChangeRequest) {
    if (AsyncGlobals.debug) {
      log.debug("addKeyChangeRequest: {}", keyChangeRequest);
    }
    keyChangeRequests.add(keyChangeRequest);
    wakeupSelector();
  }

  private void wakeupSelector() {
    if (!inThisSelectorThread()) {
      selector.wakeup();
    }
        /*
        if (!inThisSelectorThread() && !wakeupPending) {
            // chance that we get in here multiple times
            // we'll just have an extra wakeup
            wakeupPending = true;
            selector.wakeup();
        }
        */
  }

  public void addSelectionKeyOps(SelectionKey key, int newOps) {
    if (key == null) {
      return;
    }
    synchronized (key) {
      key.interestOps(key.interestOps() | newOps);
    }
    wakeupSelector();
  }

  public void removeSelectionKeyOps(SelectionKey key, int removeOps) {
    synchronized (key) {
      if (AsyncGlobals.debug && debug && log.isDebugEnabled()) {
        log.debug("remove {}", removeOps);
        log.debug("before {} {}", key , keyStatusString(key));
      }
      key.interestOps(key.interestOps() & ~removeOps);
      if (AsyncGlobals.debug && debug && log.isDebugEnabled()) {
        log.debug("after {} {}", key , keyStatusString(key));
      }
    }
    wakeupSelector();
  }

  public String interestOpsDebugString(SocketChannel channel) {
    SelectionKey key;
    int interestOps;
    StringBuilder sb;

    sb = new StringBuilder();
    key = channel.keyFor(selector);
    synchronized (key) {
      interestOps = key.interestOps();
    }
    sb.append((interestOps & SelectionKey.OP_ACCEPT) != 0 ? 'A' : '_');
    sb.append((interestOps & SelectionKey.OP_CONNECT) != 0 ? 'C' : '_');
    sb.append((interestOps & SelectionKey.OP_READ) != 0 ? 'R' : '_');
    sb.append((interestOps & SelectionKey.OP_WRITE) != 0 ? 'W' : '_');
    return sb.toString();
  }

  // ////////////////////////////////////////////////////////////////////

  public void run() {
    notifyStart();
    try {
      while (running) {
        try {
          addNewServerChannels();
          processKeyChangeRequests();
          doSelection();
        } catch (Exception e) {
          log.error("",e);
          notifyException(e);
          ThreadUtil.pauseAfterException();
        }
      }
      // Ensure that any pending closes are handled
      processKeyChangeRequests();
    } finally {
      try {
        if (selector.isOpen()) {
          selector.close();
        }
      } catch (IOException e) {
        notifyException(e);
        log.error("Could not close selector {}" , selector, e);
      } finally {
        notifyStop();
      }
    }
  }

  public String getThreadName() {
    return selectorThread.getName();
  }

  protected void notifySelect() {}
  protected void notifyStart() {}
  protected void notifyStop() {}
  protected void notifyDoSelectHandle() {}
  protected void notifyException(Exception e) {}
}
