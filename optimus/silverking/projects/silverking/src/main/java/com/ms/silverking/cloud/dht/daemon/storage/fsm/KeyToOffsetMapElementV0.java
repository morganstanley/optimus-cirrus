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
package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.collection.DHTKeyCuckooBase;
import com.ms.silverking.collection.cuckoo.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.IntArrayDHTKeyCuckoo;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

/**
 * Mimics LTV storage format for legacy storage.
 */
public class KeyToOffsetMapElementV0 extends KeyToIntegerMapElement {
  static final int legacyLengthOffset = 0;

  static final int headerSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES;

  private KeyToOffsetMapElementV0(ByteBuffer buf) {
    super(buf);
    //System.out.printf("buf %s\n", buf);
  }

  public static KeyToOffsetMapElementV0 fromFSMBuffer(ByteBuffer fsmBuf) {
    int htBufSize;
    int legacyPersistedSize;

    htBufSize = fsmBuf.getInt(KeyToOffsetMapElementV0.legacyLengthOffset);
    legacyPersistedSize = headerSize + htBufSize;
    //System.out.printf("htBufSize %d\n", htBufSize);
    return new KeyToOffsetMapElementV0((ByteBuffer) BufferUtil.duplicate(fsmBuf).limit(legacyPersistedSize));
  }

  public int getLegacyPersistedSize() {
    return buf.getInt(legacyLengthOffset);
  }

  public int getLength() {
    return getLegacyPersistedSize() + NumConversion.BYTES_PER_INT * 2;
  }

  public int getType() {
    return FSMElementType.OffsetMap.ordinal();
  }

  /**
   * Unsupported as the underlying storage is not LTV
   */
  public ByteBuffer getBuffer() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getValueBuffer() {
    return BufferUtil.duplicate(buf);
  }

  /*
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static KeyToOffsetMapElementV0 create(DHTKeyCuckooBase keyToOffset) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int legacyPersistedSize;

    mapSize = ((IntArrayDHTKeyCuckoo) keyToOffset).persistedSizeBytes();

    legacyPersistedSize = headerSize + mapSize;

    elementArray = ((IntArrayDHTKeyCuckoo) keyToOffset).getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    // Skip storing LT as the legacy format is just V

    //  Store legacy format
    elementBuf.putInt(mapSize); // legacy size does *not* include header etc., just the cuckoo map
    keyToOffset.getConfig().persist(elementBuf, elementBuf.position());
    //elementBuf.position(0/*legacyPersistedSize*/);
    elementBuf.rewind();
    //System.out.printf("legacyPersistedSize %d\n", legacyPersistedSize);

    return new KeyToOffsetMapElementV0(elementBuf);
  }
}
