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

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.net.protocol.KeyValueMessageFormat;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent used by both MessageGroupPutEntry and MessageGroupRetrievalResponseEntry.
 * This code avoids copying the data from the generating message by
 * creating new ByteBuffers that are views into the original buffers.
 */
public abstract class MessageGroupKVEntry extends MessageGroupKeyEntry {
  private final int bufferIndex;
  private final int bufferOffset;
  protected int storedLength;

  private static Logger log = LoggerFactory.getLogger(MessageGroupKVEntry.class);

  protected ByteBuffer valBuffer;

  public MessageGroupKVEntry(ByteBuffer keyBuffer, int offset) {
    super(keyBuffer, offset);
    bufferIndex = keyBuffer.getInt(offset + KeyValueMessageFormat.bufferIndexOffset);
    bufferOffset = keyBuffer.getInt(offset + KeyValueMessageFormat.bufferOffsetOffset);
  }

  protected void initValBuffer(ByteBuffer[] buffers) {
    if (bufferIndex >= 0) {
      try {
        // FIXME - NEED TO LOOK INTO USAGE OF NATIVE BUFFERS

        if (!buffers[bufferIndex].hasArray()) {
          int length;

          length = storedLength;
          valBuffer = (ByteBuffer) ((ByteBuffer) buffers[bufferIndex].duplicate().position(bufferOffset)).slice().limit(
              length);
        } else {
          byte[] array;
          int length;

          array = buffers[bufferIndex].array();
          length = storedLength;
          valBuffer = ByteBuffer.wrap(array, // FUTURE - this is using array, consider buffer
              bufferOffset, length);
          //Math.min(array.length, length));
        }
      } catch (RuntimeException re) {
        System.out.println(re);
        re.printStackTrace();
        re.printStackTrace(System.out);
        System.out.println(super.toString());
        System.out.printf("bufferIndex %d  bufferOffset %d  storedLength %d\n", bufferIndex, bufferOffset,
            storedLength);
        System.out.println(buffers.length);
        System.out.println(buffers[bufferIndex]);
        System.out.println(buffers[bufferIndex].array().length);
        System.out.println(storedLength);
        System.out.println();
        for (int i = 0; i < buffers.length; i++) {
          System.out.println(i + "\t" + buffers[i]);
          System.out.println(StringUtil.byteBufferToHexString(buffers[i]));
        }
        System.out.println("..........");
        throw re;
      }
    } else {
      valBuffer = null;
    }
  }

  public int getBufferIndex() {
    return bufferIndex;
  }

  public int getBufferOffset() {
    return bufferOffset;
  }

  public int getStoredLength() {
    return storedLength;
  }

  public ByteBuffer getValue() {
    return valBuffer;
  }

  public boolean hasValue() {
    return valBuffer != null;
  }

  public int getErrorCode() {
    if (bufferIndex >= 0) {
      throw new RuntimeException("bufferIndex " + bufferIndex);
    } else {
      return -(bufferIndex + 1);
    }
  }

  public int entryLength() {
    return bytesPerEntry;
  }

  public int getValueLength() {
    return valBuffer != null ? valBuffer.limit() : 0;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + bufferIndex + ":" + bufferOffset + ":" + storedLength;
  }
}
