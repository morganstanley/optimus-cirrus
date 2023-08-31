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
package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SocketChannel;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.MessageFormat;
import com.ms.silverking.cloud.dht.trace.TracerFactory;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.async.IncomingData;
import com.ms.silverking.net.async.ReadResult;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FUTURE - add header checksums, timeouts on reception

/**
 * Extension of IncomingData for MessageGroups.
 *
 * <p>This class is designed to receive data organized into ByteBuffers. It first receives metadata
 * regarding the buffers to be received, after which it receives the buffers themselves.
 *
 * <p>Header format <fieldName> (<number of bytes>): numberOfBuffers (4) messageType (1) options (3)
 * uuid (16) context (8)
 *
 * <p>bufferLength[0] (4) ... bufferLength[numberOfBuffers - 1] (4)
 *
 * <p>buffer[0] buffer[1] ...
 */
public final class IncomingMessageGroup implements IncomingData {
  private final ByteBuffer leadingBuffer;
  private ByteBuffer bufferLengthsBuffer;
  private IntBuffer bufferLengthsBufferInt;
  private int curBufferIndex;
  private ByteBuffer[] buffers;
  private int lastNumRead;
  private ReadState readState;
  private MessageType messageType;
  private int options;
  private UUIDBase uuid;
  private long context;
  private long version; // FUTURE - unused
  private byte[] originator;
  private int deadlineRelativeMillis;
  private ForwardingMode forward;

  private static Logger log = LoggerFactory.getLogger(IncomingMessageGroup.class);

  private enum ReadState {
    INIT_PREAMBLE_SEARCH,
    PREAMBLE_SEARCH,
    HEADER_LENGTH,
    BUFFER_LENGTHS,
    BUFFERS,
    DONE,
    CHANNEL_CLOSED
  }

  private static final int maxBufferSize = Integer.MAX_VALUE;
  private static final int errorTolerance = 4;

  // FUTURE - think about limiting number of buffers and message group size
  // see     public MessageGroup(MessageType messageType, long context, List<ByteBuffer> buffers)
  private static final int maxNumBuffers = 131072;
  private static final int minNumBuffers = 1;

  public IncomingMessageGroup(boolean debug) {
    // FUTURE - think about allocate direct here
    // leadingBuffer = ByteBuffer.allocateDirect(MessageFormat.leadingBufferSize);
    leadingBuffer = ByteBuffer.allocate(MessageFormat.leadingBufferSize);
    readState = ReadState.INIT_PREAMBLE_SEARCH;
    // this.debug = debug;
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public UUIDBase getUUID() {
    return uuid;
  }

  public long getContext() {
    return context;
  }

  public byte[] getOriginator() {
    return originator;
  }

  public int getDeadlineRelativeMillis() {
    return deadlineRelativeMillis;
  }

  public long getVersion() {
    if (version == 0) {
      throw new RuntimeException("version not set");
    }
    return version;
  }

  public ByteBuffer[] getBuffers() {
    return buffers;
  }

  public MessageGroup getMessageGroup() {
    for (ByteBuffer buffer : buffers) {
      buffer.flip();
    }
    return new MessageGroup(
        messageType, options, uuid, context, buffers, originator, deadlineRelativeMillis, forward);
  }

  public int getLastNumRead() {
    return lastNumRead;
  }

  private boolean matchesStart(byte[] pattern, ByteBuffer buf) {
    for (int i = 0; i < pattern.length; i++) {
      if (pattern[i] != buf.get(i)) {
        return false;
      }
    }
    return true;
  }

  public ReadResult readFromChannel(SocketChannel channel) throws IOException {
    int numRead;

    lastNumRead = 0;
    do {
      try {
        switch (readState) {
          case INIT_PREAMBLE_SEARCH:
            leadingBuffer.clear();
            readState = ReadState.PREAMBLE_SEARCH;
            // break; fall through to PREAMBLE_SEARCH
          case PREAMBLE_SEARCH:
            numRead = channel.read(leadingBuffer);
            if (numRead < 0) {
              return ReadResult.CHANNEL_CLOSED;
            }
            lastNumRead += numRead;
            if (leadingBuffer.hasRemaining()) {
              return ReadResult.INCOMPLETE;
            } else {
              byte[] candidatePreamble;
              // we have a full preamble buffer, see if we match
              // candidatePreamble = preambleAndHeaderLengthBuffer.array();
              // if (Arrays.matchesStart(preamble, candidatePreamble)) {
              if (matchesStart(MessageGroupGlobals.preamble, leadingBuffer)) {
                long uuidMSL;
                long uuidLSL;

                readState = ReadState.HEADER_LENGTH;
                if (leadingBuffer.get(MessageFormat.protocolVersionOffset)
                    != MessageGroupGlobals.protocolVersion) {
                  throw new RuntimeException(
                      "Unexpected protocolVersion: " + MessageGroupGlobals.protocolVersion);
                }
                allocateBufferLengthsBuffer(leadingBuffer.getInt(MessageFormat.lengthOffset));
                // TODO (OPTIMUS-40461): Consider adding a safe msgType deserialization, and server
                // shall return
                //  "UnsupportedMessageErrorResult" back to client if it cannot handle the message
                // type
                messageType = EnumValues.messageType[leadingBuffer.get(MessageFormat.typeOffset)];
                options =
                    leadingBuffer.get(
                        MessageFormat
                            .optionsOffset); // only support one byte of options presently; ignore
                // the other 2
                uuidMSL = leadingBuffer.getLong(MessageFormat.uuidMSLOffset);
                uuidLSL = leadingBuffer.getLong(MessageFormat.uuidLSLOffset);
                uuid = new UUIDBase(uuidMSL, uuidLSL);
                context = leadingBuffer.getLong(MessageFormat.contextOffset);
                originator = new byte[ValueCreator.BYTES];
                leadingBuffer.position(MessageFormat.originatorOffset);
                leadingBuffer.get(originator);
                deadlineRelativeMillis =
                    leadingBuffer.getInt(MessageFormat.deadlineRelativeMillisOffset);
                // For debugging
                // System.out.printf("%x:%x %s deadlineRelativeMillis %d\n",
                //        uuidMSL, uuidLSL,
                //        IPAddrUtil.addrAndPortToString(originator), deadlineRelativeMillis);
                forward = EnumValues.forwardingMode[leadingBuffer.get(MessageFormat.forwardOffset)];
                readState = ReadState.BUFFER_LENGTHS;
                // TODO (OPTIMUS-0000): ADD FALLTHROUGH FOR THIS CASE
              } else {
                // if (debug) {
                log.warn("*** No preamble match from channel {}***", channel);
                // }
                log.warn("{}", leadingBuffer);
                log.warn(
                    "{}", StringUtil.byteBufferToHexString(leadingBuffer.duplicate().position(0)));
                /*
                // mismatch - search for real preamble
                leadingBuffer.clear();
                //if (candidatePreamble[1] == preamble[0]) {
                if (leadingBuffer.get(1) == MessageGroupGlobals.preamble[0]) {
                    leadingBuffer.put(MessageGroupGlobals.preamble[0]);
                }
                */
                if (TracerFactory.isInitialized()) {
                  TracerFactory.getTracer().onPreambleMismatch(channel);
                }
                return ReadResult.ERROR;
              }
            }
            break;
          case BUFFER_LENGTHS:
            if (bufferLengthsBuffer.remaining() <= 0) {
              throw new IOException("bufferLengthsBuffer.remaining() <= 0");
            }
            numRead = channel.read(bufferLengthsBuffer);
            if (numRead < 0) {
              return ReadResult.CHANNEL_CLOSED;
            } else if (numRead == 0) {
              return ReadResult.INCOMPLETE;
            } else {
              lastNumRead += numRead;
              if (bufferLengthsBuffer.remaining() == 0) {
                allocateBuffers();
                readState = ReadState.BUFFERS;
              }
              break;
            }
          case BUFFERS:
            ByteBuffer curBuffer;

            // TODO (OPTIMUS-0000): MERGE THESE READS EVENTUALLY
            curBuffer = buffers[curBufferIndex];
            // if (curBuffer.remaining() <= 0) {
            //    throw new IOException("curBuffer.remaining() <= 0");
            // }
            if (curBuffer.remaining() > 0) {
              numRead = channel.read(curBuffer);
            } else {
              numRead = 0;
            }
            if (numRead < 0) {
              return ReadResult.CHANNEL_CLOSED;
            } else if (numRead == 0) {
              if (curBuffer.remaining() > 0) {
                return ReadResult.INCOMPLETE;
              } else {
                curBufferIndex++;
                assert curBufferIndex <= buffers.length;
                if (curBufferIndex == buffers.length) {
                  readState = ReadState.DONE;
                  return ReadResult.COMPLETE;
                } else {
                  break;
                }
              }
            } else {
              lastNumRead += numRead;
              if (curBuffer.remaining() == 0) {
                curBufferIndex++;
                assert curBufferIndex <= buffers.length;
                if (curBufferIndex == buffers.length) {
                  readState = ReadState.DONE;
                  return ReadResult.COMPLETE;
                } else {
                  break;
                }
              } else {
                break;
              }
            }
          case DONE:
            return ReadResult.COMPLETE;
          case CHANNEL_CLOSED:
            throw new IOException("Channel closed");
          default:
            throw new RuntimeException("panic");
        }
      } catch (IOException ioe) {
        if (ioe.getMessage().startsWith("Connection reset")) {
          readState = ReadState.CHANNEL_CLOSED;
          return ReadResult.CHANNEL_CLOSED;
        } else {
          throw ioe;
        }
      }
    } while (true);
  }

  private void allocateBufferLengthsBuffer(int numBuffers) throws IOException {
    if (numBuffers < minNumBuffers) {
      throw new IOException("numBuffers < " + minNumBuffers);
    }
    if (numBuffers > maxNumBuffers) {
      throw new IOException("numBuffers > maxNumBuffers\t" + numBuffers + " > " + maxNumBuffers);
    }
    try {
      bufferLengthsBuffer = ByteBuffer.allocate(numBuffers * NumConversion.BYTES_PER_INT);
    } catch (OutOfMemoryError oome) {
      log.info("OutOfMemoryError caught in buffer allocation");
      throw new IOException("OutOfMemoryError caught in buffer allocation");
    }
    bufferLengthsBufferInt = bufferLengthsBuffer.asIntBuffer();
    buffers = new ByteBuffer[numBuffers];
  }

  private void allocateBuffers() throws IOException {
    for (int i = 0; i < buffers.length; i++) {
      int size;

      size = bufferLengthsBufferInt.get(i);
      if (size > maxBufferSize || size < 0) {
        throw new IOException("bad buffer size: " + size);
      }
      try {
        buffers[i] = ByteBuffer.allocate(size);
        // buffers[i] = ByteBuffer.allocateDirect(size);
      } catch (OutOfMemoryError oome) {
        log.info("OutOfMemoryError caught in buffer allocation");
        throw new IOException("OutOfMemoryError caught in buffer allocation");
      }
    }
  }

  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append("*********************************\n");
    sb.append(bufferToString(bufferLengthsBuffer));
    sb.append("buffers.length\t" + buffers.length);
    for (ByteBuffer buffer : buffers) {
      sb.append(bufferToString(buffer));
    }
    sb.append(lastNumRead + "\t");
    sb.append(lastNumRead);
    sb.append(readState);
    sb.append("\n*********************************\n");
    return sb.toString();
  }

  public String bufferToString(ByteBuffer buf) {
    if (buf == null) {
      return "[null]";
    } else {
      StringBuilder sb;
      ByteBuffer dup;

      dup = buf.duplicate();
      dup.rewind();
      sb = new StringBuilder();
      sb.append('[');
      sb.append(dup.limit());
      sb.append('\t');
      while (dup.remaining() > 0) {
        byte b;

        b = dup.get();
        sb.append(Integer.toHexString(b) + ":");
      }
      sb.append(']');
      return sb.toString();
    }
  }
}
