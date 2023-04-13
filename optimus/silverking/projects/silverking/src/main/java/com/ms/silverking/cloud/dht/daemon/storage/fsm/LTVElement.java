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

import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LTVElement {
  protected final ByteBuffer buf;

  private static final int lengthOffset = 0;
  private static final int typeOffset = lengthOffset + NumConversion.BYTES_PER_INT;
  protected static final int valueOffset = typeOffset + NumConversion.BYTES_PER_INT;
  private static final int headerSizeBytes = valueOffset;

  protected static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(LTVElement.class);

  public LTVElement(ByteBuffer buf) {
    if (debug) {
      log.debug("LTVElement buf {}", buf);
    }
    this.buf = buf.order(ByteOrder.nativeOrder());
  }

  public int getLength() {
    return buf.getInt(lengthOffset);
  }

  public int getType() {
    return buf.getInt(typeOffset);
  }

  public int getValueLength() {
    return getLength() - getHeaderSizeBytes();
  }

  public ByteBuffer getBuffer() {
    return BufferUtil.duplicate(buf);
  }

  public ByteBuffer getValueBuffer() {
    if (debug) {
      log.debug("buf {}", buf);
      log.debug("headerSizeBytes {}", headerSizeBytes);
      log.debug("getLength() {}", getLength());
    }
    return BufferUtil.sliceRange(buf, headerSizeBytes, getLength());
  }

  public int getValueOffset() {
    return valueOffset;
  }

  public static int getHeaderSizeBytes() {
    return headerSizeBytes;
  }

  @Override
  public String toString() {
    return String.format("%d %d %s", getLength(), getType(), StringUtil.byteBufferToHexString(getValueBuffer()));
  }

  /////////////////////

  public static ByteBuffer readElementBuffer(ByteBuffer buf, int offset) {
    int length;
    ByteBuffer elementBuf;

    try {
      length = buf.getInt(offset + lengthOffset);
      //System.out.printf("length %d\n", length);
      //elementBuf = BufferUtil.get(buf, offset, length);
      elementBuf = BufferUtil.sliceRange(buf, offset, offset + length);
    } catch (RuntimeException re) {
      log.info("{} {}", offset, lengthOffset);
      log.info("{}", buf);
      //System.out.printf("%s\n", StringUtil.byteBufferToHexString(buf));
      throw re;
    }
    return elementBuf;
  }
}
