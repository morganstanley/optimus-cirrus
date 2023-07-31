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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SocketChannel;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is designed to receive data organized into ByteBuffers.
 * It first receives metadata regarding the buffers to be received,
 * after which it receives the buffers themselves.
 * <p>
 * Header format <fieldName> (<number of bytes>):
 * numberOfBuffers    (4)
 * <p>
 * bufferLength[0]    (4)
 * ...
 * bufferLength[numberOfBuffers - 1] (4)
 * <p>
 * buffer[0]
 * buffer[1]
 * ...
 */
public final class NewIncomingBufferedData implements IncomingData {
  private final ByteBuffer preambleAndHeaderLengthBuffer;
  //private final ByteBuffer    headerLengthBuffer;
  private final IntBuffer headerLengthBufferInt;
  private ByteBuffer bufferLengthsBuffer;
  private IntBuffer bufferLengthsBufferInt;
  private int curBufferIndex;
  private ByteBuffer[] buffers;
  private int lastNumRead;
  private ReadState readState;

  private static Logger log = LoggerFactory.getLogger(NewIncomingBufferedData.class);

  private enum ReadState {
    INIT_PREAMBLE_SEARCH, PREAMBLE_SEARCH, HEADER_LENGTH, BUFFER_LENGTHS, BUFFERS, DONE, CHANNEL_CLOSED
  }

  ;

  //private static final int maxBufferSize = Integer.MAX_VALUE >> 2;
  private static final int maxBufferSize = 1024 * 1024 * 1024;
  private static final int errorTolerance = 0;

  public static final byte[] preamble = { (byte) 0xab, (byte) 0xad };

  private static final int maxNumBuffers = 2048;
  private static final int minNumBuffers = 1;

  public static void setClient() {
    //isClient = true;
  }

  public NewIncomingBufferedData(boolean debug) {
    preambleAndHeaderLengthBuffer = ByteBuffer.allocateDirect(preamble.length + NumConversion.BYTES_PER_INT);
    //headerLengthBuffer = ByteBuffer.allocate(NumConversion.BYTES_PER_INT);
    preambleAndHeaderLengthBuffer.position(preamble.length);
    headerLengthBufferInt = preambleAndHeaderLengthBuffer.asIntBuffer();
    preambleAndHeaderLengthBuffer.position(0);
    readState = ReadState.INIT_PREAMBLE_SEARCH;
    //this.debug = debug;
  }

  public ByteBuffer[] getBuffers() {
    return buffers;
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
    int readErrors;

    readErrors = 0;
    lastNumRead = 0;
    do {
      try {
        switch (readState) {
        case INIT_PREAMBLE_SEARCH:
          preambleAndHeaderLengthBuffer.clear();
          readState = ReadState.PREAMBLE_SEARCH;
          //break; fall through to PREAMBLE_SEARCH
        case PREAMBLE_SEARCH:
          numRead = channel.read(preambleAndHeaderLengthBuffer);
          if (numRead < 0) {
            return ReadResult.CHANNEL_CLOSED;
          }
          lastNumRead += numRead;
          if (preambleAndHeaderLengthBuffer.hasRemaining()) {
            return ReadResult.INCOMPLETE;
          } else {
            byte[] candidatePreamble;

            // we have a full preamble buffer, see if we match
            //candidatePreamble = preambleAndHeaderLengthBuffer.array();
            //if (Arrays.matchesStart(preamble, candidatePreamble)) {
            if (matchesStart(preamble, preambleAndHeaderLengthBuffer)) {
              readState = ReadState.HEADER_LENGTH;
              allocateBufferLengthsBuffer(headerLengthBufferInt.get(0));
              readState = ReadState.BUFFER_LENGTHS;
              // TODO (OPTIMUS-0000): ADD FALLTHROUGH FOR THIS CASE
            } else {
              // mismatch - search for real preamble
              preambleAndHeaderLengthBuffer.clear();
              //if (candidatePreamble[1] == preamble[0]) {
              if (preambleAndHeaderLengthBuffer.get(1) == preamble[0]) {
                preambleAndHeaderLengthBuffer.put(preamble[0]);
              }
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
          //if (curBuffer.remaining() <= 0) {
          //    throw new IOException("curBuffer.remaining() <= 0");
          //}
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
          readErrors++;
          if (readErrors <= errorTolerance) {
            log.error("Ignoring read error {}" , readErrors, ioe);
            preambleAndHeaderLengthBuffer.clear();
            readState = ReadState.INIT_PREAMBLE_SEARCH;
            return ReadResult.INCOMPLETE;
          } else {
            throw ioe;
          }
        }
      }
    } while (true);
  }

  private void allocateBufferLengthsBuffer(int numBuffers) throws IOException {
    if (numBuffers < minNumBuffers) {
      throw new IOException("numBuffers < " + minNumBuffers);
    }
    if (numBuffers > maxNumBuffers) {
      throw new IOException("numBuffers > " + maxNumBuffers);
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
        //buffers[i] = ByteBuffer.allocateDirect(size);
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
