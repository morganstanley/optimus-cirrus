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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;
import com.ms.silverking.cloud.dht.throttle.SkThrottlingDebt;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProtoMessageGroup for messages that contains values
 */
public final class ProtoValueMessageGroup extends ProtoValueMessageGroupBase {
  private int expectedSize;
  private static Logger log = LoggerFactory.getLogger(ProtoValueMessageGroup.class);

  public ProtoValueMessageGroup(UUIDBase uuid,
                                long context,
                                int opSize,
                                int valueBytes,
                                byte[] originator,
                                int deadlineRelativeMillis,
                                SkTraceId maybeTraceID) {
    super(
        TraceIDProvider.isValidTraceID(maybeTraceID)
        ? MessageType.RETRIEVE_RESPONSE_TRACE
        : MessageType.RETRIEVE_RESPONSE,
        uuid,
        context,
        opSize,
        ByteBuffer.allocate(RetrievalResponseMessageFormat.optionBytesSize),
        RetrievalResponseMessageFormat.size - KeyedMessageFormat.baseBytesPerKeyEntry,
        originator,
        deadlineRelativeMillis,
        ForwardingMode.FORWARD,
        maybeTraceID);
    expectedSize = valueBytes;
  }

  public void addValue(DHTKey dhtKey, ByteBuffer value, int compressedLength, boolean noCopy) {
    int storedValueSize = value.remaining();
    int compressedValueSize = compressedLength;
    totalValueBytes += storedValueSize;

    if (log.isDebugEnabled()) {
      log.debug("storedValueSize: {}", storedValueSize);
      log.debug("compressedValueSize: {}", compressedValueSize);
    }
    // need to serialize the key
    addKey(dhtKey);
    /*
     * The question to be solved here is whether to copy the value or
     * to create a new bytebuffer entry in the messagegroup.
     *
     * Leave in place when any of the following hold:
     *  a) single value
     *  b) large value
     *
     * Copy when:
     *  a) many small values
     */
    boolean copyValue;
    if (noCopy) {
      copyValue = false;
    } else {
      if (opSize == 1 || compressedValueSize >= dedicatedBufferSizeThreshold) {
        copyValue = false;
      } else {
        copyValue = true;
      }
    }
    if (copyValue) {
      if (valueBuffer == null || storedValueSize > valueBuffer.remaining()) {
        // in below line we don't need to consider compressedValueSize since dedicatedBufferSizeThreshold <= valueBufferSize
        valueBuffer = ByteBuffer.allocate(Math.max(expectedSize - totalValueBytes, storedValueSize));
        if (!addMultiValueBuffer(valueBuffer)) {
          throw new RuntimeException("Too many buffers");
        }
      }
      // record where the value will be written into the key buffer
      keyByteBuffer.putInt(curMultiValueBufferIndex);
      keyByteBuffer.putInt(valueBuffer.position());
      keyByteBuffer.putInt(storedValueSize);
      try {
        value.get(valueBuffer.array(), valueBuffer.position(), storedValueSize);
      } catch (BufferUnderflowException bfe) {
        log.info(value.remaining() + "\t" + storedValueSize);
        log.info(value.toString());
        log.info(valueBuffer.toString());
        throw bfe;
      }
      if (valueBuffer.remaining() > storedValueSize) {
        valueBuffer.position(valueBuffer.position() + storedValueSize);
      } else {
        assert valueBuffer.remaining() == storedValueSize;
        valueBuffer.position(valueBuffer.limit());
        valueBuffer = null;
      }
    } else {
      // record where the value will be located in the key buffer
      keyByteBuffer.putInt(bufferList.size());
      keyByteBuffer.putInt(value.position());
      keyByteBuffer.putInt(storedValueSize);
      value.position(value.limit());
      bufferList.add(value);
    }
  }

  public void addDebt(SkThrottlingDebt totalDebt) {
    ByteBuffer debt = SkThrottlingDebt.serialize(totalDebt);
    bufferList.add(debt);
  }

  public void addErrorCode(DHTKey key, OpResult result) {
    addKey(key);
    keyByteBuffer.putInt(-(result.ordinal() + 1));
    keyByteBuffer.putInt(-1);
    keyByteBuffer.putInt(-1);
  }
}
