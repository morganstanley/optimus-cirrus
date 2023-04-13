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
import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProtoMessageGroup for forwarded put messages
 */
public final class ProtoPutForwardMessageGroup extends ProtoValueMessageGroupBase {

  private static Logger log = LoggerFactory.getLogger(ProtoPutForwardMessageGroup.class);

  public ProtoPutForwardMessageGroup(UUIDBase uuid,
                                     long context,
                                     byte[] originator,
                                     ByteBuffer optionsByteBuffer,
                                     List<MessageGroupKeyEntry> destEntries,
                                     ChecksumType checksumType,
                                     int deadlineRelativeMillis,
                                     SkTraceId maybeTraceID) {
    super(
        TraceIDProvider.isValidTraceID(maybeTraceID)
        ? MessageType.LEGACY_PUT_TRACE
        : MessageType.LEGACY_PUT,
        uuid,
        context,
        destEntries.size(),
        optionsByteBuffer.asReadOnlyBuffer(),
        PutMessageFormat.size(checksumType) - KeyedMessageFormat.baseBytesPerKeyEntry,
        originator,
        deadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD,
        maybeTraceID);
    if (debug) {
      log.debug("optionsByteBuffer {} last {}", optionsByteBuffer, (bufferList.size() - 1));
    }
    for (int i = 0; i < destEntries.size(); i++) {
      MessageGroupPutEntry entry = (MessageGroupPutEntry) destEntries.get(i);
      addValue(entry, entry);
    }
  }

  public void addValue(DHTKey dhtKey, MessageGroupPutEntry entry) {
    if (debug) {
      log.debug("entry: {}", entry);
      log.debug("entry.getValue(): {}", entry.getValue());
    }
    ByteBuffer value = entry.getValue().asReadOnlyBuffer();
    if (debug) {
      log.debug("v0: {}", value);
    }

    int storedValueSize = value.remaining();
    int compressedValueSize = entry.getStoredLength();
    totalValueBytes += storedValueSize;
    int uncompressedValueSize = entry.getUncompressedLength();

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
    boolean copyValue = true;
    if (opSize == 1 || compressedValueSize >= dedicatedBufferSizeThreshold) {
      copyValue = false;
    }

    if (copyValue) {
      if (valueBuffer == null || storedValueSize > valueBuffer.remaining()) {
        // in below line we don't need to consider compressedValueSize since
        // dedicatedBufferSizeThreshold <= valueBufferSize
        valueBuffer = ByteBuffer.allocate(valueBufferSize);
        curMultiValueBufferIndex = bufferList.size();
        bufferList.add(valueBuffer);
      }
      // record where the value will be written into the key buffer
      keyByteBuffer.putInt(curMultiValueBufferIndex);
      keyByteBuffer.putInt(valueBuffer.position());
      keyByteBuffer.putInt(uncompressedValueSize);
      keyByteBuffer.putInt(compressedValueSize);
      keyByteBuffer.put(entry.getChecksum());

      value.get(valueBuffer.array(), valueBuffer.position(), storedValueSize);
      valueBuffer.position(valueBuffer.position() + storedValueSize);
    } else {
      keyByteBuffer.putInt(bufferList.size());
      keyByteBuffer.putInt(value.position());
      keyByteBuffer.putInt(uncompressedValueSize);
      keyByteBuffer.putInt(compressedValueSize);
      keyByteBuffer.put(entry.getChecksum());
      // FIXME - think about removing the need for size since strictly speaking it isn't necessary
      value.position(value.position() + storedValueSize);
      bufferList.add(value);
    }
  }

  @Override
  public MessageGroup toMessageGroup() {
    if (hasTraceID) {
      // Try to consume traceTraceID before gets flipped
      tryConsumeTraceID();
    }
    for (int i = 0; i < bufferList.size(); i++) {
      if (i != ProtoValueMessageGroupBase.getOptionsByteBufferBaseOffset(hasTraceID)) {
        bufferList.get(i).flip();
      }
    }
    return toMessageGroup(false);
  }
}
