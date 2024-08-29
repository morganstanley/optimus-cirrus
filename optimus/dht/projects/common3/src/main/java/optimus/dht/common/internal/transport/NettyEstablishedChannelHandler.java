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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import optimus.dht.common.api.RemoteInstanceIdentity;
import optimus.dht.common.api.transport.CorruptedStreamException;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;
import optimus.dht.common.api.transport.SentMessageMetrics;
import optimus.dht.common.api.transport.EstablishedStreamHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class NettyEstablishedChannelHandler extends ChannelDuplexHandler {

  private static final Logger logger =
      LoggerFactory.getLogger(NettyEstablishedChannelHandler.class);

  private static final int HEADER_SIZE = 18;
  private static final int FOOTER_SIZE = 8;

  private enum State {
    HEADER,
    BODY,
    FOOTER
  }

  private final int bufferSize;
  private final RemoteInstanceIdentity remoteInstanceIdentity;
  private final Map<Integer, EstablishedStreamHandler> handlers;
  private final EstablishedTransportMessagesQueue messagesQueue;

  // write part
  private State writeState = State.HEADER;
  private MessageStream writeStream;
  private long writeSize;
  private long writeRemaining;
  private MutableSentMessageMetrics writeMetrics;
  private Consumer<SentMessageMetrics> writeMetricsCallback;
  private long firstWriteNano;

  // read part
  private State readState = State.HEADER;
  private ByteBuf headerBuf;
  private ByteBuf footerBuf;
  private EstablishedStreamHandler currentHandler;
  private long messagePosition = 0;
  private long messageSize = 0;
  private long firstReadNano;
  private long firstReadTimestamp;
  private long messageRemoteTimestamp;
  private long readHandlerTime;

  public NettyEstablishedChannelHandler(
      int bufferSize,
      ChannelSharedState sharedState,
      Map<Integer, EstablishedStreamHandler> handlers,
      EstablishedTransportMessagesQueue messagesQueue) {
    this.bufferSize = bufferSize;
    this.remoteInstanceIdentity = sharedState.remoteInstanceIdentity();
    this.handlers = handlers;
    this.messagesQueue = messagesQueue;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    if (!ctx.channel().isActive()) {
      throw new IllegalStateException(
          "This handler can only be added to an already active channel");
    }

    headerBuf = Unpooled.buffer(HEADER_SIZE, HEADER_SIZE);
    footerBuf = Unpooled.buffer(FOOTER_SIZE, FOOTER_SIZE);

    ctx.channel()
        .config()
        .setAutoRead(false); // disable autoread - for future backpressure machanism
    ctx.read(); // make sure that read operation is scheduled

    processWrite(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    headerBuf.release();
    footerBuf.release();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.read(); // schedule next read, after we are done with this one
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf byteBuf = (ByteBuf) msg;

    try {
      while (byteBuf.isReadable()) {

        switch (readState) {
          case HEADER:
            if (!headerBuf.isReadable()) { // only set those on the very first header read
              firstReadNano = System.nanoTime();
              firstReadTimestamp = System.currentTimeMillis();
              readHandlerTime = 0;
            }

            if (headerBuf.isReadable()) { // if we already have partial header
              headerBuf.writeBytes(
                  byteBuf, Math.min(headerBuf.writableBytes(), byteBuf.readableBytes()));
              if (headerBuf.isReadable(HEADER_SIZE)) {
                readHeader(headerBuf);
                headerBuf.clear();
              }
            } else if (byteBuf.isReadable(
                HEADER_SIZE)) { // no partial header, enough already available
              readHeader(byteBuf);
            } else { // no partial header, not enough already available
              headerBuf.writeBytes(byteBuf);
            }
            break;

          case BODY:
            long remainingInFrame = messageSize - messagePosition;
            int readableBytes = byteBuf.readableBytes();

            int toRead;
            ByteBuf readBuffer;
            if (readableBytes <= remainingInFrame) {
              toRead = readableBytes;
              readBuffer = byteBuf;
            } else {
              toRead = (int) remainingInFrame;
              readBuffer = byteBuf.readSlice(toRead);
            }
            currentHandler.dataReceived(readBuffer);
            messagePosition += toRead;
            if (messagePosition == messageSize) {
              readState = State.FOOTER;
            }
            break;

          case FOOTER:
            if (footerBuf.isReadable()) { // if we already have partial footer
              footerBuf.writeBytes(
                  byteBuf, Math.min(footerBuf.writableBytes(), byteBuf.readableBytes()));
              if (footerBuf.isReadable(FOOTER_SIZE)) {
                readFooter(footerBuf);
                footerBuf.clear();
              }
            } else if (byteBuf.isReadable(
                FOOTER_SIZE)) { // no partial footer, enough already available
              readFooter(byteBuf);
            } else { // no partial footer, not enough already available
              footerBuf.writeBytes(byteBuf);
            }
            break;
        }
      }

    } finally {
      byteBuf.release();
    }
  }

  private void readHeader(ByteBuf byteBuf) {
    int moduleId = byteBuf.readShortLE();
    messageSize = byteBuf.readLongLE();
    messageRemoteTimestamp = byteBuf.readLongLE();
    messagePosition = 0;
    currentHandler = handlers.get(moduleId);
    if (currentHandler == null) {
      throw new CorruptedStreamException(
          "Received message for unknown moduleId="
              + moduleId
              + ", knownModules="
              + handlers.keySet());
    }
    readState = State.BODY;
    long nanoTime = System.nanoTime();
    currentHandler.messageStart(messageSize);
    readHandlerTime += (System.nanoTime() - nanoTime);
    if (logger.isTraceEnabled()) {
      logger.trace(
          "Starting to read a new message, client={}, moduleId={}, size={}",
          remoteInstanceIdentity.uniqueId(),
          moduleId,
          messageSize);
    }
  }

  private void readFooter(ByteBuf byteBuf) {
    long remoteSendTime = byteBuf.readLongLE();
    long beforeNanoTime = System.nanoTime();
    currentHandler.messageEnd();
    long afterNanoTime = System.nanoTime();
    readHandlerTime += (afterNanoTime - beforeNanoTime);
    ReceivedMessageMetrics metrics =
        new ReceivedMessageMetrics(
            messageSize,
            firstReadNano,
            firstReadTimestamp,
            beforeNanoTime,
            afterNanoTime,
            readHandlerTime,
            messageRemoteTimestamp,
            remoteSendTime);
    readState = State.HEADER;
    currentHandler.messageCompleted(metrics);
    currentHandler = null;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    throw new UnsupportedOperationException("All writes must go via message queue");
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    processWrite(ctx);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().isWritable()) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "Calling processWrite after channel became writeable again for uniqueId={}",
            remoteInstanceIdentity.uniqueId());
      }
      processWrite(ctx);
    }
    ctx.fireChannelWritabilityChanged();
  }

  private boolean canWrite(ChannelHandlerContext ctx) {
    return ctx.channel().isWritable() && (writeStream != null || !messagesQueue.isEmpty());
  }

  public void processWrite(ChannelHandlerContext ctx) {

    if (!canWrite(ctx)) {
      return;
    }

    do {
      int nextBufferSize =
          (int) Math.min(bufferSize, writeRemaining + 2 * messagesQueue.queueElementsSize());
      ByteBuf buffer = ctx.alloc().buffer(Math.max(nextBufferSize, HEADER_SIZE));
      List<Runnable> actionsForBuffer = null;

      do {

        if (writeState == State.HEADER) {

          if (!buffer.isWritable(HEADER_SIZE)) {
            // Not enough space to send header in this buffer - exit inner loop
            break;
          }

          EstablishedTransportMessagesQueue.QueueElement queueElement = messagesQueue.poll();
          writeStream = queueElement.messageStream();
          writeSize = writeStream.size();
          writeRemaining = writeSize;
          writeMetrics = queueElement.metrics();
          writeMetricsCallback = queueElement.metricsCallback();
          short moduleId = queueElement.moduleId();

          writeMetrics.touchAccessed();
          firstWriteNano = System.nanoTime();
          if (actionsForBuffer == null) {
            actionsForBuffer = new ArrayList<>();
          }
          MutableSentMessageMetrics thisMessageWriteMetrics = writeMetrics;
          actionsForBuffer.add(() -> thisMessageWriteMetrics.touchFirstWrite());

          buffer.writeShortLE(moduleId);
          buffer.writeLongLE(writeSize);
          buffer.writeLongLE(System.currentTimeMillis());

          long nanoTime = System.nanoTime();
          writeStream.messageStart();
          writeMetrics.addToHandlerTime(System.nanoTime() - nanoTime);

          if (logger.isTraceEnabled()) {
            logger.trace(
                "Starting to send a new message, client={}, moduleId={}, size={}",
                remoteInstanceIdentity.uniqueId(),
                moduleId,
                writeSize);
          }

          writeState = State.BODY;

        } else if (writeState == State.BODY) {

          int preWritePosition = buffer.writerIndex();
          long nanoTime = System.nanoTime();
          writeStream.generate(buffer, buffer.writableBytes());
          writeMetrics.addToHandlerTime(System.nanoTime() - nanoTime);
          int writtenBytes = buffer.writerIndex() - preWritePosition;
          writeRemaining -= writtenBytes;

          if (writeRemaining == 0) {
            writeState = State.FOOTER;
          }

        } else {

          if (!buffer.isWritable(FOOTER_SIZE)) {
            // Not enough space to send header in this buffer - exit inner loop
            break;
          }

          buffer.writeLongLE(System.nanoTime() - firstWriteNano);

          final MessageStream thisMessageStream = writeStream;
          final long thisMessageSize = writeSize;
          final MutableSentMessageMetrics thisMessageWriteMetrics = writeMetrics;
          final Consumer<SentMessageMetrics> thisMessageWriteMetricsCallback = writeMetricsCallback;

          writeState = State.HEADER;
          writeStream = null;
          writeSize = 0;
          writeMetrics = null;
          writeMetricsCallback = null;

          if (actionsForBuffer == null) {
            actionsForBuffer = new ArrayList<>();
            actionsForBuffer.add(() -> thisMessageWriteMetrics.touchLastWrite());
          } else {
            // replaces the last element (which must be touchFirst), with touchFirstAndLast
            actionsForBuffer.set(
                actionsForBuffer.size() - 1,
                () -> thisMessageWriteMetrics.touchFirstAndLastWrite());
          }

          actionsForBuffer.add(
              () -> {
                messagesQueue.clearMostRecentElement();
                SentMessageMetrics metrics = thisMessageWriteMetrics.toMetrics(thisMessageSize);
                thisMessageStream.messageSent(metrics);
                if (thisMessageWriteMetricsCallback != null) {
                  thisMessageWriteMetricsCallback.accept(metrics);
                }
                if (logger.isTraceEnabled()) {
                  logger.trace(
                      "Entire message was sent, client={}, size={}",
                      remoteInstanceIdentity.uniqueId(),
                      thisMessageSize);
                }
              });
        }

      } while (buffer.isWritable() && (writeStream != null || !messagesQueue.isEmpty()));

      if (actionsForBuffer != null) {
        final List<Runnable> actions = actionsForBuffer;
        ctx.write(buffer, ctx.newPromise())
            .addListener(
                future -> {
                  if (future.isSuccess()) {
                    for (Runnable action : actions) {
                      action.run();
                    }
                  }
                });
      } else {
        ctx.write(buffer, ctx.voidPromise());
      }

    } while (canWrite(ctx));

    ctx.flush();
  }
}
