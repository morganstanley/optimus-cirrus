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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Optional;

import com.ms.silverking.LegacySkTraceIdSerializer;
import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ProtoMessageGroup for keyed messages */
public abstract class ProtoKeyedMessageGroup extends ProtoMessageGroup {
  private final int keyBufferAdditionalBytesPerKey;
  protected ByteBuffer keyByteBuffer;
  protected final ByteBuffer optionsByteBuffer;
  private int totalKeys;
  protected final boolean hasTraceID;

  private static Logger log = LoggerFactory.getLogger(ProtoKeyedMessageGroup.class);

  private static final int keyBufferExpansionKeys = 32;
  protected static final int keyByteBufferIndex = 0;
  protected static final int traceIDBufferListIndex = 1;

  public ProtoKeyedMessageGroup(
      MessageType type,
      UUIDBase uuid,
      long context,
      ByteBuffer optionsByteBuffer,
      int numKeys,
      int keyBufferAdditionalBytesPerKey,
      byte[] originator,
      int deadlineRelativeMillis,
      ForwardingMode forward,
      SkTraceId maybeTraceID) {
    super(type, uuid, context, originator, deadlineRelativeMillis, forward);

    this.keyBufferAdditionalBytesPerKey = keyBufferAdditionalBytesPerKey;
    keyByteBuffer = allocateKeyBuffer(numKeys, keyBufferAdditionalBytesPerKey);

    // System.out.println("keyBufferSize: "+ keyBufferSize
    //        +"\tkeyBufferAdditionalBytesPerKey: "+ keyBufferAdditionalBytesPerKey);

    // TODO (OPTIMUS-40255): switch to new serialization logic once all servers support new format
    // commented out code
    ProtoSkTraceIdSerialize protoSkTraceIdSerialize = new ProtoSkTraceIdSerialize();
    LegacySkTraceIdSerializer legacySkTraceIdSerializer = new LegacySkTraceIdSerializer();

    bufferList.add(keyByteBuffer); // index 0
    hasTraceID = TraceIDProvider.hasTraceID(type);

    final byte[] tempMaybeTraceID;
    ByteBuffer maybeTraceIdByte;
    final byte[] maybeTraceIdLegacyByte;
    if (TraceIDProvider.hasTraceID(type)) {
      maybeTraceIdLegacyByte = legacySkTraceIdSerializer.serialize(maybeTraceID).toByteArray();
      // maybeTraceIdByte = protoSkTraceIdSerialize.serializeSkTraceId(maybeTraceID);
      // tempMaybeTraceID = new byte[maybeTraceIdByte.remaining()];
      // maybeTraceIdByte.get(tempMaybeTraceID);
      // bufferList.add(ByteBuffer.wrap(tempMaybeTraceID)); // index 1
      bufferList.add(ByteBuffer.wrap(maybeTraceIdLegacyByte));
    }

    this.optionsByteBuffer = optionsByteBuffer;
  }

  static int getOptionsByteBufferBaseOffset(boolean hasTraceID) {
    return hasTraceID ? 2 : 1;
  }

  /**
   * NOTE: Caller of this method shall be responsible for checking return value is valid (i.e. call
   * TraceIDProvider.isValidTraceID(byte[] maybeTraceID))
   */
  public static SkTraceId unsafeGetTraceIDCopy(MessageGroup mg) {
    if (TraceIDProvider.hasTraceID(mg.getMessageType())) {
      ProtoSkTraceIdSerialize protoSkTraceIdSerialize = new ProtoSkTraceIdSerialize();
      ByteBuffer buffer = mg.getBuffers()[traceIDBufferListIndex].asReadOnlyBuffer();
      int len = buffer.remaining();
      byte[] copy = new byte[len];
      buffer.get(copy);
      return protoSkTraceIdSerialize.backwardsCompatibleDeserialize(copy);
    } else {
      return TraceIDProvider.noTraceID;
    }
  }

  public static Optional<SkTraceId> tryGetTraceIDCopy(MessageGroup mg) {
    return Optional.ofNullable(unsafeGetTraceIDCopy(mg));
  }

  private static final ByteBuffer allocateKeyBuffer(
      int numKeys, int keyBufferAdditionalBytesPerKey) {
    int bytesPerEntry = KeyedMessageFormat.baseBytesPerKeyEntry + keyBufferAdditionalBytesPerKey;
    ByteBuffer keyBuffer =
        ByteBuffer.allocate(NumConversion.BYTES_PER_SHORT + bytesPerEntry * numKeys);
    keyBuffer.putShort((short) bytesPerEntry);
    return keyBuffer;
  }

  public int currentBufferKeys() {
    return totalKeys;
  }

  public void addKey(DHTKey dhtKey) {
    try {
      ++totalKeys;
      keyByteBuffer.putLong(dhtKey.getMSL());
      keyByteBuffer.putLong(dhtKey.getLSL());
    } catch (BufferOverflowException bfe) {
      log.debug("ProtoKeyedMessageGroup keyByteBuffer overflow. Expanding.");
      ByteBuffer newKeyByteBuffer =
          allocateKeyBuffer(
              currentBufferKeys() + keyBufferExpansionKeys, keyBufferAdditionalBytesPerKey);
      keyByteBuffer.flip();
      newKeyByteBuffer.put(keyByteBuffer);
      keyByteBuffer = newKeyByteBuffer;
      keyByteBuffer.putLong(dhtKey.getMSL());
      keyByteBuffer.putLong(dhtKey.getLSL());
    }
  }

  public boolean isNonEmpty() {
    return keyByteBuffer.position() != 0;
  }

  // Used before some subclasses force to do flip(), which get traceID wiped
  protected void tryConsumeTraceID() {
    if (hasTraceID) {
      ByteBuffer traceID = bufferList.get(traceIDBufferListIndex);
      if (traceID.position() != traceID.limit()) {
        traceID.position(traceID.limit());
      }
    }
  }

  @Override
  public MessageGroup toMessageGroup() {
    return this.toMessageGroup(true);
  }

  @Override
  protected MessageGroup toMessageGroup(boolean flip) {
    if (hasTraceID) {
      ByteBuffer traceID = bufferList.get(traceIDBufferListIndex);
      // Make sure traceID is not wiped after flip
      if (flip) {
        tryConsumeTraceID();
      } else {
        if (traceID.position() != 0) {
          traceID.position(0);
        }
      }
      return super.toMessageGroup(flip);
    } else {
      return super.toMessageGroup(flip);
    }
  }
}
