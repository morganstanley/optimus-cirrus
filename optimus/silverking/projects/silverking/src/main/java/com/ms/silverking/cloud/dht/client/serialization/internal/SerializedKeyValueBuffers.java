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
package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;

/**
 * Serialized K/V pairs.
 *
 * <p>For now, only use this for outgoing pairs.
 *
 * <p>Currently unused FUTURE - consider removing
 *
 * <p>FUTURE - Incoming may not use this since incoming from peers needs to carefully avoid copies
 * and want to read straight into temporary storage. This is a scaffolding class until that can be
 * reviewed.
 *
 * <p>FUTURE - Think about moving the value length into the value of the concat buffer. Dedicated
 * doesn't need this. Probably make that move.
 *
 * <p>Key format: <field> (<bytes>) MSL (8) LSL (8) BufferIndex (2) BufferOffset (4) ValueLength (4)
 */
public class SerializedKeyValueBuffers {
  private final ByteBuffer keyBuffer;
  private final List<ByteBuffer> valueBuffers;
  private final int concatBufferSize;
  private int numKeys;
  private int maxKeys;
  private boolean frozen;
  private short curConcatBufferIndex;

  private static final int DEFAULT_CONCAT_BUFFER_SIZE = 1024 * 1024;
  private static final int DEFAULT_KEY_BUFFER_SIZE = 1024 * 1024;
  private static final int BYTES_PER_KEY =
      2 * NumConversion.BYTES_PER_LONG
          + NumConversion.BYTES_PER_SHORT
          + 2 * NumConversion.BYTES_PER_INT;

  public enum BufferMode {
    concatenated,
    dedicated
  };

  public SerializedKeyValueBuffers(ByteBuffer[] buffers) {
    keyBuffer = buffers[0];
    numKeys = (keyBuffer.capacity() - NumConversion.BYTES_PER_INT) / BYTES_PER_KEY;
    maxKeys = numKeys;

    valueBuffers = new ArrayList<>(buffers.length - 1);
    for (int i = 1; i < buffers.length; i++) {
      valueBuffers.add(buffers[i]);
    }
    concatBufferSize = buffers[1].capacity();
    frozen = true;
  }

  public SerializedKeyValueBuffers(int keyBufferSize, int concatBufferSize) {
    this.concatBufferSize = concatBufferSize;
    keyBuffer = ByteBuffer.allocateDirect(keyBufferSize);
    maxKeys = (keyBufferSize - NumConversion.BYTES_PER_INT) / BYTES_PER_KEY;
    keyBuffer.position(NumConversion.BYTES_PER_INT);
    valueBuffers = new ArrayList<>();
    valueBuffers.add(ByteBuffer.allocate(concatBufferSize));
  }

  public SerializedKeyValueBuffers() {
    this(DEFAULT_KEY_BUFFER_SIZE, DEFAULT_CONCAT_BUFFER_SIZE);
  }

  private final void verifyNotFrozen() {
    if (frozen) {
      throw new RuntimeException("Frozen SerializedKeyValueBuffers");
    }
  }

  private final void verifyFrozen() {
    if (!frozen) {
      throw new RuntimeException("SerializedKeyValueBuffers not frozen");
    }
  }

  public void freeze() {
    verifyNotFrozen();
    frozen = true;
    keyBuffer.putInt(0, numKeys);
    keyBuffer.flip();
    for (ByteBuffer valueBuffer : valueBuffers) {
      valueBuffer.flip();
    }
  }

  public int getNumKeys() {
    verifyFrozen();
    return numKeys;
  }

  public ByteBuffer[] getBuffers() {
    ByteBuffer[] buffers;

    buffers = new ByteBuffer[valueBuffers.size() + 1];
    buffers[0] = keyBuffer;
    for (int i = 1; i < buffers.length; i++) {
      buffers[i] = valueBuffers.get(i - 1);
    }
    return buffers;
  }

  private int mslOffset(int index) {
    return NumConversion.BYTES_PER_INT + index * BYTES_PER_KEY;
  }

  private int lslOffset(int index) {
    return NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_LONG + index * BYTES_PER_KEY;
  }

  private int bufferIndexOffset(int index) {
    return NumConversion.BYTES_PER_INT + 2 * NumConversion.BYTES_PER_LONG + index * BYTES_PER_KEY;
  }

  private int bufferOffsetOffset(int index) {
    return NumConversion.BYTES_PER_INT
        + 2 * NumConversion.BYTES_PER_LONG
        + NumConversion.BYTES_PER_SHORT
        + index * BYTES_PER_KEY;
  }

  private int valueLengthOffset(int index) {
    return NumConversion.BYTES_PER_INT
        + 2 * NumConversion.BYTES_PER_LONG
        + NumConversion.BYTES_PER_SHORT
        + NumConversion.BYTES_PER_INT
        + index * BYTES_PER_KEY;
  }

  public DHTKey getKey(int index) {
    if (index >= numKeys) {
      throw new IndexOutOfBoundsException("index > " + (numKeys - 1));
    } else {
      return new SimpleKey(
          keyBuffer.getLong(mslOffset(index)), keyBuffer.getLong(lslOffset(index)));
    }
  }

  public ByteBuffer getValue(int index) {
    short bufferIndex;
    int bufferOffset;
    int valueLength;
    ByteBuffer valueBuffer;

    // TODO (OPTIMUS-0000): optimize this with running position
    bufferIndex = keyBuffer.getShort(bufferIndexOffset(index));
    bufferOffset = keyBuffer.getInt(bufferOffsetOffset(index));
    valueLength = keyBuffer.getInt(valueLengthOffset(index));
    valueBuffer = valueBuffers.get(bufferIndex).duplicate();
    valueBuffer.position(bufferOffset);
    valueBuffer.limit(bufferOffset + valueLength);
    return valueBuffer;
  }

  private short addValueBuffer(ByteBuffer valueBuffer) {
    short index;

    if (valueBuffers.size() == Short.MAX_VALUE) {
      return -1;
    }
    index = (short) valueBuffers.size();
    valueBuffers.add(valueBuffer);
    return index;
  }

  public boolean addKeyValue(DHTKey key, ByteBuffer value, BufferMode bufferMode) {
    if (numKeys >= maxKeys) {
      return false;
    } else {
      short valueBufferIndex;
      int valueBufferOffset;
      int valueLength;

      valueLength = value.remaining();
      switch (bufferMode) {
        case concatenated:
          ByteBuffer concatBuffer;

          concatBuffer = valueBuffers.get(curConcatBufferIndex);
          if (concatBuffer.remaining() < value.remaining()) {
            short newIndex;

            newIndex = addValueBuffer(ByteBuffer.allocate(concatBufferSize));
            if (newIndex < 0) {
              return false;
            }
            curConcatBufferIndex = newIndex;
            concatBuffer = valueBuffers.get(curConcatBufferIndex);
          }
          valueBufferIndex = curConcatBufferIndex;
          valueBufferOffset = concatBuffer.position();
          concatBuffer.put(value);
          break;
        case dedicated:
          int newIndex;

          valueBufferIndex = (short) valueBuffers.size();
          valueBufferOffset = 0;
          newIndex = addValueBuffer(value);
          if (newIndex < 0) {
            return false;
          }
          break;
        default:
          throw new RuntimeException("panic");
      }
      keyBuffer.putLong(key.getMSL());
      keyBuffer.putLong(key.getLSL());
      keyBuffer.putShort(valueBufferIndex);
      keyBuffer.putInt(valueBufferOffset);
      keyBuffer.putInt(valueLength);
      numKeys++;
      return true;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb;
    int offset;
    Formatter fm;

    sb = new StringBuilder();
    fm = new Formatter(sb);
    sb.append("numKeys: ");
    sb.append(numKeys);
    sb.append('\n');
    offset = NumConversion.BYTES_PER_INT;
    ;
    for (int i = 0; i < numKeys; i++) {
      long msl;
      long lsl;
      short bufferIndex;
      int bufferOffset;
      int valueLength;

      msl = keyBuffer.getLong(offset);
      offset += NumConversion.BYTES_PER_LONG;
      lsl = keyBuffer.getLong(offset);
      offset += NumConversion.BYTES_PER_LONG;
      bufferIndex = keyBuffer.getShort(offset);
      offset += NumConversion.BYTES_PER_SHORT;
      bufferOffset = keyBuffer.getInt(offset);
      offset += NumConversion.BYTES_PER_INT;
      valueLength = keyBuffer.getInt(offset);
      offset += NumConversion.BYTES_PER_INT;
      fm.format("\t[%16x:%16x:%4d:%9d:%9d]\n", msl, lsl, bufferIndex, bufferOffset, valueLength);
    }
    for (int i = 0; i < valueBuffers.size(); i++) {
      fm.format("valBuf[%d]\t%s\n", i, valueBuffers.get(i).toString());
    }
    return sb.toString();
  }
}
