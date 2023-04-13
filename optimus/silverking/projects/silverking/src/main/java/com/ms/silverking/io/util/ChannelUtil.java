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
package com.ms.silverking.io.util;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class ChannelUtil {
  private static final int bufferSendLimit = 8;

  private static long totalBytes(ByteBuffer[] buffers) {
    long total;

    total = 0;
    for (Buffer buffer : buffers) {
      total += buffer.limit();
    }
    return total;
  }

  /**
   * Emperical observation shows that throughput degrades significantly for
   * gathered writes over a certain number of buffers. This method
   * writes the buffer without exceeding this threshold.
   *
   * @param buffers
   * @param channel
   * @throws IOException
   */
    /*
    public static void sendBuffers(ByteBuffer[] buffers, GatheringByteChannel outChannel) 
            throws IOException {
        sendBuffers(buffers, totalBytes(buffers), outChannel);
    }
    
    private static void sendBuffers(ByteBuffer[] buffers, long totalToWrite, GatheringByteChannel outChannel)
            throws IOException {
        if (buffers.length > bufferSendLimit) {
            int curGroupMax;
            int prevGroupMax;

            curGroupMax = Integer.MIN_VALUE;
            prevGroupMax = -1;
            while (curGroupMax < buffers.length - 1) {
                ByteBuffer[] splitBuffers;
                long subTotal;

                curGroupMax = Math.min(buffers.length - 1, prevGroupMax + bufferSendLimit);
                splitBuffers = new ByteBuffer[curGroupMax - prevGroupMax];
                subTotal = 0;
                for (int i = 0; i < splitBuffers.length; i++) {
                    splitBuffers[i] = buffers[prevGroupMax + i + 1];
                    subTotal += splitBuffers[i].capacity();
                }
                sendBuffers(splitBuffers, subTotal, outChannel);
                prevGroupMax = curGroupMax;
            }
        } else {
            long totalWritten;

            //for (int i = 0; i < buffers.length; i++) {
            //    buffers[i].rewind();
            //}
            totalWritten = 0;
            while (totalWritten < totalToWrite) {
                long written;

                written = outChannel.write(buffers);
                if (written > 0) {
                    totalWritten += written;
                }
            }
            if (totalWritten != totalToWrite) {
                throw new RuntimeException("totalWritten != totalToWrite");
            }
        }
    }
    */
  public static long writeBuffersBatched(ByteBuffer[] buffers, GatheringByteChannel channel) throws IOException {
    if (buffers.length < bufferSendLimit) {
      return channel.write(buffers);
    } else {
      int startIndex;
      int batchSize;

      startIndex = 0;
      while (!buffers[startIndex].hasRemaining()) {
        startIndex++;
      }
      batchSize = Math.min(buffers.length - startIndex, bufferSendLimit);
      return channel.write(buffers, startIndex, batchSize);
            /*
            while (true) {
                int     startIndex;
                int     batchSize;
                
                startIndex = 0;
                while (!buffers[startIndex].hasRemaining()) {
                    startIndex++;
                }
                batchSize = Math.min(buffers.length - startIndex, bufferSendLimit);
                return channel.write(buffers, startIndex, batchSize);
            }
            */
    }
  }
}
