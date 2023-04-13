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

import com.ms.silverking.collection.cuckoo.CuckooBase;
import com.ms.silverking.collection.cuckoo.CuckooConfig;
import com.ms.silverking.collection.cuckoo.IntArrayCuckoo;
import com.ms.silverking.collection.cuckoo.IntBufferCuckoo;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerToIntegerMapElement extends LTVElement {

  private static Logger log = LoggerFactory.getLogger(IntegerToIntegerMapElement.class);

  public IntegerToIntegerMapElement(ByteBuffer buf) {
    super(buf);
  }

  public CuckooBase<Integer> getIntegerToIntegerMap() {
    ByteBuffer rawHTBuf;
    ByteBuffer htBuf;
    WritableCuckooConfig segmentCuckooConfig;
    CuckooBase<Integer> integerToOffset;
    int htBufSize;
    int htTotalEntries;

    rawHTBuf = getValueBuffer();
    // FUTURE - cache the below number or does the segment cache do this well enough? (also cache the
    // segmentCuckooConfig...)
    htBufSize = rawHTBuf.getInt(0);
    try {
      htBuf = BufferUtil.sliceAt(rawHTBuf, NumConversion.BYTES_PER_INT + CuckooConfig.BYTES);
    } catch (RuntimeException re) {
      log.info("{}", rawHTBuf);
      throw re;
    }

    rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());

    htTotalEntries = htBufSize / (NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_INT);
    segmentCuckooConfig = new WritableCuckooConfig(CuckooConfig.read(rawHTBuf, NumConversion.BYTES_PER_INT),
        -1);
    segmentCuckooConfig = segmentCuckooConfig.newTotalEntries(htTotalEntries);

    integerToOffset = new IntBufferCuckoo(segmentCuckooConfig, htBuf);
    return integerToOffset;
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static IntegerToIntegerMapElement create(IntArrayCuckoo integerToOffset, FSMElementType type) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int headerSize;
    int elementSize;
    int legacyPersistedSize;

    mapSize = integerToOffset.persistedSizeBytes();

    legacyPersistedSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES + mapSize;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + legacyPersistedSize;
    headerSize = elementSize - mapSize;

    elementArray = integerToOffset.getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(type.ordinal());

    elementBuf.putInt(mapSize);
    integerToOffset.getConfig().persist(elementBuf, elementBuf.position());
    elementBuf.position(elementSize);

    elementBuf.rewind();

    return new IntegerToIntegerMapElement(elementBuf);
  }
}
