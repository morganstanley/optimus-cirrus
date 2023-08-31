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
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.ConcurrentLinkedDequeWithSize;
import com.ms.silverking.id.UUIDBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection that maintains an output queue. */
public abstract class QueueingConnection<D extends OutgoingData, I extends IncomingData>
    extends Connection {
  private final ConcurrentLinkedQueue<D> priorityOutputQueue;
  private final ConcurrentLinkedDequeWithSize<D> outputQueue;
  private final boolean failSendsOnError;
  private I incomingData;
  private final QueueingConnectionLimitListener limitListener;
  private final int queueLimit;
  private OutgoingData outgoingData;
  private long lastPollTime;
  // private long    totalBytesWritten; // for debugging only

  private static Logger log = LoggerFactory.getLogger(QueueingConnection.class);

  // QueueingConnection stats
  protected final AtomicLong cumulativeQueuedSends;
  protected final AtomicLong cumulativeDirectSends;
  protected final AtomicLong cumulativeExpiredSends;

  private static final boolean debug = false;
  private static final boolean debugMessageTimeouts = false;
  private static final long messageTimeoutMargin = 20;

  static final boolean enableDeadline =
      Boolean.parseBoolean(System.getProperty("com.ms.silverking.queueingConn.deadline", "false"));

  public QueueingConnection(
      SocketChannel channel,
      SelectorController<? extends Connection> selectorController,
      ConnectionListener connectionListener,
      boolean failSendsOnError,
      boolean debug,
      QueueingConnectionLimitListener limitListener,
      int queueLimit) {
    super(channel, selectorController, connectionListener, true, debug, false);
    priorityOutputQueue = new ConcurrentLinkedQueue<>();
    outputQueue = new ConcurrentLinkedDequeWithSize<>();
    this.failSendsOnError = failSendsOnError;
    incomingData = createIncomingData(debug);
    this.limitListener = limitListener;
    this.queueLimit = queueLimit;
    if (statsEnabled) {
      cumulativeQueuedSends = new AtomicLong();
      cumulativeDirectSends = new AtomicLong();
      cumulativeExpiredSends = new AtomicLong();
    } else {
      cumulativeQueuedSends = null;
      cumulativeDirectSends = null;
      cumulativeExpiredSends = null;
    }
    this.lastPollTime = -1;
  }

  public QueueingConnection(
      SocketChannel channel,
      SelectorController<? extends Connection> selectorController,
      ConnectionListener connectionListener,
      boolean failSendsOnError,
      boolean debug) {
    this(
        channel,
        selectorController,
        connectionListener,
        failSendsOnError,
        debug,
        null,
        Integer.MAX_VALUE);
  }

  protected abstract I createIncomingData(boolean debug);

  public QueueingConnection(
      SocketChannel channel,
      SelectorController<? extends Connection> selectorController,
      ConnectionListener connectionListener,
      boolean failSendsOnError) {
    this(channel, selectorController, connectionListener, failSendsOnError, false);
  }

  protected abstract D wrapForSend(
      Object data, UUIDBase sendUUID, AsyncSendListener asyncSendListener, long deadline)
      throws IOException;

  @Override
  protected long writeAllPending_locked() throws IOException {
    long bytesWritten;
    boolean foundWrite;

    /*
     * Write until either no more data, or we fill the buffer
     *
     * Lock in Connection.writeAllPending() guarantees that there is
     * only one thread pulling from the queue at a time.
     */
    if (debug) {
      log.info("in writeAllPending_locked");
    }
    bytesWritten = 0;
    try {
      while (!outputQueue.isEmpty()) {
        boolean allWritten;

        if (outgoingData == null) {
          long currPollTime = OutgoingData.currAbsMillis();
          outgoingData = outputQueue.peek();
          if (outgoingData.deadlineExpired()) {
            if (statsEnabled) {
              cumulativeExpiredSends.incrementAndGet();
            }
            if (enableDeadline) {
              outputQueue.remove();
            }
            log.warn(
                "QueueingConnection deadline expired: lastPollTime={} queueSize={} data={} ",
                lastPollTime < 0 ? "N/A(First msg in queue)" : lastPollTime,
                outputQueue.size(),
                outgoingData);
            sendTimedOut(outgoingData, lastPollTime, currPollTime, outputQueue.size());
            if (enableDeadline) {
              outgoingData = null;
            }
          }
          lastPollTime = currPollTime;
        }
        if (outgoingData != null) {
          try {
            if (debug) {
              log.info("writeToChannel {}", outgoingData);
            }
            allWritten = outgoingData.writeToChannel(channel);
            if (allWritten) {
              if (statsEnabled) {
                cumulativeQueuedSends.incrementAndGet();
                cumulativeSends.incrementAndGet();
              }
              outputQueue.remove();
              // FUTURE - Consider making this go through a worker so
              // we can continue sending on this channel
              sendSucceeded(outgoingData);
              bytesWritten += outgoingData.getTotalBytes();
              outgoingData = null;
            } else {
              // write blocked; exit so selector can watch for
              // available buffer space
              break;
            }
          } catch (IOException ioe) {
            // if (verbose) {
            log.info("send failed due to exception: {}  remote: {}", ioe, getRemoteSocketAddress());
            // }
            sendFailed(outgoingData);
            disconnect();
            throw ioe;
          }
        }
      }
    } finally {
      // System.out.println("outputQueue.size(): "+ outputQueue.size());
      // if (!outputQueue.isEmpty() || !priorityOutputQueue.isEmpty()) {
      if (!outputQueue.isEmpty()) {
        // didn't drain the queue; update the selector
        enableWrites();
      }
      if (limitListener != null && outputQueue.size() <= queueLimit) {
        // FUTURE - think about why level-triggered approach is required
        // instead of an edge-triggered approach.
        limitListener.queueBelowLimit();
      }
    }
    if (debug) {
      log.info("out writeAllPending_locked");
    }
    // totalBytesWritten += bytesWritten; // for debugging only
    return bytesWritten;
  }

  /*
  private long writeQueuePending(ConcurrentLinkedQueue<D>    queue) throws IOException {
      if (queue.isEmpty()) {
          return 0;
      } else {
          D        outgoingData;
          boolean    allWritten;
          long    bytesWritten;

          bytesWritten = 0;
          outgoingData = queue.peek();
          if (outgoingData.deadlineExpired()) {
              //Log.fine("QueueingConnection deadline expired");
              queue.remove();
              sendTimedOut(outgoingData);
          } else {
              try {
                  allWritten = outgoingData.writeToChannel(channel);
                  if (allWritten) {
                      queue.remove();
                      sendSucceeded(outgoingData);
                      bytesWritten += outgoingData.getTotalBytes();
                  } else {
                      // write blocked; exit so selector can watch for
                      // available buffer space
                      return -1;
                  }
              } catch (IOException ioe) {
                  //if (verbose) {
                      Log.info("send failed: ", getRemoteSocketAddress());
                  //}
                  sendFailed(outgoingData);
                  disconnect();
                  throw ioe;
              }
          }
          return bytesWritten;
      }
  }
  */

  @Override
  public void sendAsynchronous(
      Object data, UUIDBase sendID, AsyncSendListener asyncSendListener, long deadline)
      throws IOException {
    boolean writeEnableRequired;
    boolean locked;

    locked = false;
    writeEnableRequired = true;
    if (debug && log.isDebugEnabled()) {
      log.debug(
          "sendAsynchronous {} {} {} {} {}",
          outputQueue.size(),
          sendID,
          System.currentTimeMillis(),
          this,
          Thread.currentThread().getName());
      // Thread.dumpStack();
    }
    if (debugMessageTimeouts) {
      if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() > deadline - messageTimeoutMargin) {
        log.info("{}", data);
        log.info("Message is about to time out");
        log.info("{} {}", SystemTimeUtil.skSystemTimeSource.absTimeMillis(), deadline);
        Thread.dumpStack();
      }
    }
    try {
      D wrappedData;
      Thread currentThread;

      if (!connected) {
        throw new IOException("not connected");
      }
      wrappedData = wrapForSend(data, sendID, asyncSendListener, deadline);
      /*
      currentThread = Thread.currentThread();
      if ((currentThread instanceof SelectorThread)
              && ((SelectorThread)currentThread).getAllowBlocking()) {
          locked = channelWriteLock.tryLock();
      } else {
          locked = false;
      }
      */
      if (outputQueue.isEmpty()) {
        locked = channelWriteLock.tryLock();
      }
      if (locked && outputQueue.isEmpty()) {
        boolean allWritten;

        allWritten = wrappedData.writeToChannel(channel);
        if (!allWritten) {
          // if (DebugUtil.delayedDebug()) {
          //    System.out.println("queued data at front");
          // }
          // We must add this to the front of the queue
          // so that it is sent next.
          outputQueue.push(wrappedData);
        } else {
          if (statsEnabled) {
            cumulativeDirectSends.incrementAndGet();
            cumulativeSends.incrementAndGet();
          }
          // if (DebugUtil.delayedDebug()) {
          //    System.out.println("written directly");
          // }
          writeEnableRequired = false;
          if (debug) {
            log.info(
                "sendAsynchronous result {} {} {} {}",
                allWritten,
                outputQueue.size(),
                sendID,
                System.currentTimeMillis());
          }
        }
      } else {
        // if (DebugUtil.delayedDebug()) {
        //    System.out.println("queued data");
        // }
        outputQueue.add(wrappedData);
        wrappedData.pushIntoOutputQueue(outputQueue.size());
        if (limitListener != null && outputQueue.size() > queueLimit) {
          // if (DebugUtil.delayedDebug()) {
          //    System.out.println("limitListener.queueAboveLimit()");
          // }
          // FUTURE - think about why the above level-triggered approach
          // is required instead of an edge-triggered approach.
          // System.out.println("::>"+ outputQueue.size() +"\t"+ queueLimit);
          limitListener.queueAboveLimit();
          log.warn(
              "sendAsynchronous result triggers queue limit: qSize={} sendId={} limit={}",
              outputQueue.size(),
              sendID,
              queueLimit);
        } else {
          // System.out.println("::<"+ outputQueue.size() +"\t"+ queueLimit);
        }
      }
      /*
      if (wrappedData.getPriority() == OutgoingData.Priority.HIGH) {
          priorityOutputQueue.add(wrappedData);
      } else {
          outputQueue.add(wrappedData);
      }
      */
    } finally {
      if (locked) {
        channelWriteLock.unlock();
      }
      if (connected && writeEnableRequired) {
        enableWritesIfNotWriting();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////

  protected Object disconnect_locked() {
    Queue<D> queuedData;
    D datum;

    queuedData = new LinkedList<D>();
    do {
      if (!outputQueue.isEmpty()) {
        datum = outputQueue.remove();
        if (datum != null) {
          queuedData.add(datum);
        }
      } else {
        datum = null;
      }
    } while (datum != null);
    if (failSendsOnError) {
      for (D data : queuedData) {
        sendFailed(data);
      }
    }
    return queuedData;
  }

  ///////////////////////////////////////////////////////////////////////

  @Override
  protected int lockedRead() throws IOException {
    while (true) {
      ReadResult readResult;

      // if we throw an exception, this connection will be closed
      if (debug) {
        log.info("incomingData.readFromChannel {} ", this);
        // Thread.dumpStack();
      }
      try {
        readResult = incomingData.readFromChannel(channel);
      } catch (RuntimeException re) {
        log.error(
            "Exception proceessing connection {}  {}",
            getRemoteSocketAddress(),
            incomingData.toString(),
            re);
        throw re;
      }
      if (debug) {
        log.debug("readResult: {}", readResult);
      }
      switch (readResult) {
        case CHANNEL_CLOSED:
          return -1;
        case ERROR:
          return -1;
        case COMPLETE:
          int lastNumRead;
          I completeData;

          if (statsEnabled) {
            cumulativeReceives.incrementAndGet();
          }
          // lastNumRead = incomingData.getLastNumRead();
          completeData = incomingData;
          // channelReceiveLock.unlock();
          try {
            readComplete(completeData);
          } finally {
            //    channelReceiveLock.lock();
            incomingData = createIncomingData(debug);
          }
          // return lastNumRead;
        case INCOMPLETE:
          return incomingData.getLastNumRead();
        default:
          throw new RuntimeException("panic");
      }
    }
  }

  protected abstract void readComplete(I incomingData) throws IOException;

  @Override
  public String debugString() {
    return super.debugString() /*+":"+ totalBytesWritten*/ + ":" + outputQueue.size();
  }

  @Override
  public String statString() {
    return String.format(
        "%s:%d:%d:%d",
        super.statString(),
        cumulativeQueuedSends.longValue(),
        cumulativeDirectSends.longValue(),
        cumulativeExpiredSends.longValue());
  }

  @Override
  public long getQueueLength() {
    if (connected) {
      long size;

      size = outputQueue.size();
      if (size > 0) {
        // This is a temporary workaround
        // It seems under some situatiuons that writing can hang. This works around that
        enableWritesIfNotWriting();
      }
      return size;
    } else {
      return 0;
    }
  }
}
