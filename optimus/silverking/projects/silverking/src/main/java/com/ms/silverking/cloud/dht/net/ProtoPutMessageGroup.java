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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.cloud.dht.client.impl.ChecksumProvider;
import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalPutOptions;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Compressor;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.util.ArrayUtil;

/**
 * ProtoMessageGroup for put messages
 * Clients create a MessageGroup object for data they want to serialize and transfer.
 * Servers takes a MessageGroup object they received and tries to parse it.
 * <p>
 * Data in optionsByteBuffer is stored in the following order with specific offsets.
 * Offsets are point in the byte[] that tells where data for a specific value is stored (start index)
 * i.e. secondaryTargetLengthOffset 28 means that data for the length is stored from index 28 (inclusive) onwards
 * <p>
 * |        Offset      | Size (bytes) |                                                          |
 * -----------------------------------------------------------------------------------------------|
 * |          0         |     8       | version                                                   |
 * |          8         |     8       | required previous version                                 |
 * |         16         |     2       | lock seconds                                              |
 * |         18         |     2       | ccss - (compression type, checksum type, StorageState)    |
 * |         20         |     8       | creator (usually 8 bytes, but may vary)                   |
 * |         28         |     2       | secondaryTarget length                                    |
 * |         30         |    ANY_1    | secondaryTarget data                                      |
 * |     30 + ANY_1     |     1       | user data length                                          |
 * |     31 + ANY_1     |    ANY_2    | user data                                                 |
 * | 31 + ANY_1 + ANY_2 |    4        | authorization user length                                 |
 * | 35 + ANY_1 + ANY_2 |    ANY_3    | authorization user                                        |
 * <p>
 * // TODO (OPTIMUS-43373): Remove comment once legacy put and put_trace is removed
 * Handling backwards compatibility
 * Some clients may still be using the old message group without storing the authorization user.
 * We use MessageType to determine which version the data they want to parse is in.
 * MessageType.LEGACY_PUT is used for the old version
 * MessageType.PUT is used for the new version
 * ProtoMessage with MessageType.LEGACY_PUT will have offsets as follow:
 * Note that userdata length is not specified and everything beyond the secondaryTarget data is userdata
 * |        Offset      | Size (bytes) |                                                          |
 * -----------------------------------------------------------------------------------------------|
 * |          0         |     8       | version                                                   |
 * |          8         |     8       | required previous version                                 |
 * |         16         |     2       | lock seconds                                              |
 * |         18         |     2       | ccss - (compression type, checksum type, StorageState)    |
 * |         20         |     8       | creator (usually 8 bytes, but may vary)                   |
 * |         28         |     2       | secondaryTarget length                                    |
 * |         30         |    ANY_1    | secondaryTarget data                                      |
 * |     30 + ANY_1     |    ANY_2    | user data                                                 |
 */
public final class ProtoPutMessageGroup<V> extends ProtoValueMessageGroupBase {
  private final BufferDestSerializer<V> bdSerializer;
  private final Compressor compressor;
  private final Checksum checksum;
  private final boolean checksumCompressedValues;
  private final EncrypterDecrypter encrypterDecrypter;
  private final long fragmentationThreshold;

  private static final byte[] emptyValue = new byte[0];

  public ProtoPutMessageGroup(UUIDBase uuid,
                              long context,
                              int putOpSize,
                              long version,
                              BufferDestSerializer<V> bdSerializer,
                              PutOptions putOptions,
                              ChecksumType checksumType,
                              byte[] originator,
                              byte[] creator,
                              int deadlineRelativeMillis,
                              EncrypterDecrypter encrypterDecrypter,
                              SkTraceId maybeTraceID) {
    super(TraceIDProvider.isValidTraceID(maybeTraceID)
          ? MessageType.LEGACY_PUT_TRACE
          : MessageType.LEGACY_PUT,
          uuid,
          context,
          putOpSize,
          ByteBuffer.allocate(optionBufferLength(putOptions)),
          PutMessageFormat.size(checksumType) - KeyedMessageFormat.baseBytesPerKeyEntry,
          originator,
          deadlineRelativeMillis,
          ForwardingMode.FORWARD,
          maybeTraceID);
    this.bdSerializer = bdSerializer;
    checksum = ChecksumProvider.getChecksum(checksumType);
    Compression compression = putOptions.getCompression();
    compressor = CodecProvider.getCompressor(compression);
    checksumCompressedValues = putOptions.getChecksumCompressedValues();
    this.encrypterDecrypter = encrypterDecrypter;
    this.fragmentationThreshold = putOptions.getFragmentationThreshold();

    optionsByteBuffer.putLong(version);
    optionsByteBuffer.putLong(putOptions.getRequiredPreviousVersion());
    optionsByteBuffer.putShort(putOptions.getLockSeconds());
    optionsByteBuffer.putShort(CCSSUtil.createCCSS(compression, checksumType));
    optionsByteBuffer.put(creator);

    Set<SecondaryTarget> secondaryTargets = putOptions.getSecondaryTargets();
    if (secondaryTargets == null) {
      optionsByteBuffer.putShort((short) 0);
    } else {
      byte[] serializedST = SecondaryTargetSerializer.serialize(secondaryTargets);
      optionsByteBuffer.putShort((short) serializedST.length);
      optionsByteBuffer.put(serializedST);
    }

    // TODO (OPTIMUS-42516): Replace with code below
    if (putOptions.getUserData() != null) {
      optionsByteBuffer.put(putOptions.getUserData());
    }

    // TODO (OPTIMUS-42516): Enable when servers have been updated to parse new and legacy code
    //    byte[] userData = putOptions.getUserData();
    //    if (userData == null) {
    //      optionsByteBuffer.put((byte)0);
    //    } else {
    //      optionsByteBuffer.put((byte)userData.length);
    //      optionsByteBuffer.put(userData);
    //    }

    // TODO (OPTIMUS-42516): Enable when adding authUser
    //  We don't want to enable it until all servers are ready to handle new PUT MessageType

    //    byte[] authorizationUser = putOptions.getAuthorizationUser();
    //    if (authorizationUser == null) {
    //      optionsByteBuffer.putInt(0);
    //      optionsByteBuffer.put(ArrayUtil.emptyByteArray);
    //    } else {
    //      optionsByteBuffer.putInt(authorizationUser.length);
    //      optionsByteBuffer.put(authorizationUser);
    //    }

    //TODO (OPTIMUS-43325)- a zoneId field needs to be added to puts.
  }

  public static InternalPutOptions getPutOptions(MessageGroup messageGroup) {
    // TODO (OPTIMUS-43373): Remove this legacy check once client side is using new puts

    Optional<SkTraceId> maybeTraceID = tryGetTraceIDCopy(messageGroup);
    SkTraceId traceID = maybeTraceID.isPresent()
                     ? maybeTraceID.get()
                     : TraceIDProvider.noTraceID;

    byte[] userData = getUserData(messageGroup);

    boolean isLegacy = messageIsLegacy(messageGroup);

    byte[] auth = isLegacy
                  ? PutOptions.noAuthorizationUser
                  : getAuthorizationUser(messageGroup);

    PutOptions putOptions = OptionsHelper.newPutOptions(userData, auth);
    return new InternalPutOptions(putOptions, traceID);
  }

  // Version

  public long getVersion() {
    return optionsByteBuffer.getLong(PutMessageFormat.versionOffset);
  }

  public static void setPutVersion(MessageGroup messageGroup, long version) {
    getOptionBuffer(messageGroup).putLong(PutMessageFormat.versionOffset, version);
  }

  public static long getPutVersion(MessageGroup messageGroup) {
    return getOptionBuffer(messageGroup).getLong(PutMessageFormat.versionOffset);
  }

  // Required Previous Version

  public long getRequiredPreviousVersion() {
    return optionsByteBuffer.getLong(PutMessageFormat.requiredPreviousVersionOffset);
  }

  public static long getPutRequiredPreviousVersion(MessageGroup messageGroup) {
    return getOptionBuffer(messageGroup).getLong(PutMessageFormat.requiredPreviousVersionOffset);
  }

  // Lock Seconds

  public short getLockSeconds() {
    return optionsByteBuffer.getShort(PutMessageFormat.lockSecondsOffset);
  }

  public static short getLockSeconds(MessageGroup messageGroup) {
    return getOptionBuffer(messageGroup).getShort(PutMessageFormat.lockSecondsOffset);
  }

  // CCSS

  public static short getCCSS(MessageGroup messageGroup) {
    return getOptionBuffer(messageGroup).getShort(PutMessageFormat.ccssOffset);
  }

  public static ChecksumType getChecksumType(MessageGroup messageGroup) {
    return CCSSUtil.getChecksumType(getCCSS(messageGroup));
  }

  // Value Creator

  public static byte[] getValueCreator(MessageGroup messageGroup) {
    byte[] valueCreatorArr = new byte[ValueCreator.BYTES];
    System.arraycopy(getOptionBuffer(messageGroup).array(),
                     PutMessageFormat.valueCreatorOffset,
                     valueCreatorArr,
                     0,
                     ValueCreator.BYTES);
    return valueCreatorArr;
  }

  // Secondary Targets

  private static int getSecondaryTargetLength(MessageGroup messageGroup) {
    return getOptionBuffer(messageGroup).getShort(PutMessageFormat.secondaryTargetDataOffset);
  }

  public static Set<SecondaryTarget> getSecondaryTargets(MessageGroup messageGroup) {
    int stLength = getSecondaryTargetLength(messageGroup);

    if (stLength == 0) {
      return null;
    } else {
      byte[] secondaryTargetArr = new byte[stLength];
      System.arraycopy(getOptionBuffer(messageGroup).array(),
                       PutMessageFormat.secondaryTargetDataOffset + NumConversion.BYTES_PER_SHORT,
                       secondaryTargetArr,
                       0,
                       stLength);
      return SecondaryTargetSerializer.deserialize(secondaryTargetArr);
    }
  }

  // User data

  private static int getUserDataLengthStart(MessageGroup messageGroup) {
    return PutMessageFormat.userDataLengthOffset(getSecondaryTargetLength(messageGroup));
  }

  public static byte[] getUserData(MessageGroup messageGroup) {
    // TODO (OPTIMUS-43373): Remove this legacy check once client side is using new puts
    boolean isLegacy = messageIsLegacy(messageGroup);
    int secondaryTargetLength = getSecondaryTargetLength(messageGroup);
    byte[] optionsBufferByteArr = getOptionBuffer(messageGroup).array();

    // if messageGroup is legacy, then offset is the actual userdata's offset
    // else the offset is the userdata length's offset
    int offset = PutMessageFormat.userDataLengthOffset(secondaryTargetLength);
    int userDataLength = isLegacy
                         ? optionsBufferByteArr.length - offset
                         : NumConversion.unsignedByteToInt(optionsBufferByteArr, offset);

    // note: cannot return null because it will cause a NullPointerException in StorageFormat.java:writeToBuf
    if (userDataLength == 0) { return ArrayUtil.emptyByteArray; }

    byte[] userData = new byte[userDataLength];
    int userdataStartOffset = isLegacy
                              ? offset
                              : offset + 1; // +1 because userDataLength takes 1 byte
    System.arraycopy(optionsBufferByteArr, userdataStartOffset, userData, 0, userDataLength);
    return userData;
  }

  // Authorization user

  private static int getAuthorizedUserLengthStart(MessageGroup messageGroup) {
    byte[] optionsBufferByteArr = getOptionBuffer(messageGroup).array();
    int userDataLengthStart = getUserDataLengthStart(messageGroup);
    int userDataLength = NumConversion.unsignedByteToInt(optionsBufferByteArr, userDataLengthStart);
    // +1 because userDataLength occupies 1 byte
    int authUserStart = userDataLengthStart + 1 + userDataLength;
    return authUserStart;
  }

  private static byte[] getAuthorizationUser(MessageGroup messageGroup) {
    int lengthStartIndex = getAuthorizedUserLengthStart(messageGroup);
    ByteBuffer byteBuffer = getOptionBuffer(messageGroup);
    int length = byteBuffer.getInt(lengthStartIndex);

    if (length == 0) {
      return PutOptions.noAuthorizationUser;
    } else {
      byte[] authUser = new byte[length];
      int dataStartIndex = lengthStartIndex + NumConversion.BYTES_PER_INT;
      System.arraycopy(byteBuffer.array(), dataStartIndex, authUser, 0, length);
      return authUser;
    }
  }

  // Other Helper Methods

  private static int optionBufferLength(PutOptions putOptions) {
    return PutMessageFormat.getOptionsBufferLength(putOptions);
  }

  private boolean ensureMultiValueBufferValid(int valueSize) {
    if (valueBuffer == null || valueSize > valueBuffer.remaining()) {
      valueBuffer = ByteBuffer.allocate(Math.max(valueBufferSize, valueSize));
      if (!addMultiValueBuffer(valueBuffer)) {
        return false;
      }
    }
    return true;
  }

  public enum ValueAdditionResult {Added, MessageGroupFull, ValueNeedsFragmentation}

  /**
   * Add a raw value to this message group.
   * <p>
   * Currently, this code eagerly serializes, compresses, and checksums this value.
   *
   * @param dhtKey
   * @param value
   * @return
   */
  public ValueAdditionResult addValue(DHTKey dhtKey, V value) {
    int uncompressedValueSize;
    int compressedValueSize;
    byte[] bytesToStore;
    int bytesToStorePosition;
    int bytesToStoreSize;
    byte[] bytesToChecksum;
    int bytesToChecksumOffset;
    int bytesToChecksumLength;
    ByteBuffer bytesToChecksumBuf;
    int _bufferIndex;
    int _bufferPosition;

    // FUTURE - in the future offload serialization, compression and/or checksum computation to a worker?
    // for values over some threshold? or for number of keys over some threshold?

    // compression
    if (compressor != null || encrypterDecrypter != null) {
      ByteBuffer serializedBytes = bdSerializer.serializeToBuffer(value);

      bytesToChecksum = serializedBytes.array();
      bytesToChecksumLength = serializedBytes.remaining();
      uncompressedValueSize = bytesToChecksumLength;
      bytesToChecksumBuf = serializedBytes;

      if (serializedBytes.limit() != 0) {
        try {
          if (compressor != null) {
            bytesToStore = compressor.compress(serializedBytes.array(),
                                               serializedBytes.position(),
                                               serializedBytes.remaining());
            if (bytesToStore.length >= bytesToChecksumLength) {
              // If compression is not useful, then use the
              // uncompressed data. Note that NamespaceStore must
              // notice this change in order to correctly set the
              // compression type to NONE since the message will
              // still show the attempted compression type.
              assert serializedBytes.position() == 0;
              bytesToStore = serializedBytes.array();
            }
          } else {
            bytesToStore = serializedBytes.array();
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        if (encrypterDecrypter != null) {
          bytesToStore = encrypterDecrypter.encrypt(bytesToStore);
          bytesToChecksumBuf = ByteBuffer.wrap(bytesToStore);
        }
      } else {
        bytesToStore = emptyValue;
      }
      bytesToStorePosition = 0;
      bytesToStoreSize = bytesToStore.length;
    } else {
      uncompressedValueSize = 0;
      bytesToStore = null;
      // FUTURE - think about whether we can avoid or reduce estimation
      bytesToStoreSize = bdSerializer.estimateSerializedSize(value);
      bytesToStorePosition = -1;

      bytesToChecksum = null;
      bytesToChecksumBuf = null;
    }
    if (!canBeAdded(bytesToStoreSize)) {
      if (bytesToStoreSize <= fragmentationThreshold) {
        return ValueAdditionResult.MessageGroupFull;
      } else {
        return ValueAdditionResult.ValueNeedsFragmentation;
      }
    }

    boolean copyValue = true;
    // decide whether or not to copy the value
    if (opSize == 1 || bytesToStoreSize >= dedicatedBufferSizeThreshold) {
      copyValue = false;
    }

    // buffer storage
    if (copyValue) {
      if (!ensureMultiValueBufferValid(bytesToStoreSize)) {
        return ValueAdditionResult.MessageGroupFull;
      }
      // record where the value will be written into the key buffer
      _bufferIndex = curMultiValueBufferIndex;
      _bufferPosition = valueBuffer.position();
      int prevPosition = _bufferPosition;
      if (bytesToStore == null) {
        bdSerializer.serializeToBuffer(value, valueBuffer);
        bytesToChecksumOffset = prevPosition;
        bytesToChecksumLength = valueBuffer.position() - prevPosition;
        bytesToChecksumBuf = (ByteBuffer) valueBuffer.asReadOnlyBuffer().position(bytesToChecksumOffset).limit(
            prevPosition + bytesToChecksumLength);
      } else {
        assert bytesToChecksum != null;
        valueBuffer.put(bytesToStore, bytesToStorePosition, bytesToStoreSize);
      }
    } else {
      ByteBuffer newBuf;

      if (bytesToStore == null) {
        newBuf = bdSerializer.serializeToBuffer(value);
        bytesToStorePosition = newBuf.position();
        bytesToStoreSize = newBuf.limit();
      } else {
        newBuf = ByteBuffer.wrap(bytesToStore, bytesToStorePosition, bytesToStoreSize);
      }
      if (bytesToChecksum == null) {
        bytesToChecksum = newBuf.array();
        bytesToChecksumOffset = newBuf.position();
        bytesToChecksumLength = newBuf.remaining();
        bytesToChecksumBuf = ByteBuffer.wrap(bytesToChecksum, bytesToChecksumOffset, bytesToChecksumLength);
      }
      newBuf.position(bytesToStorePosition + bytesToStoreSize);

      // record where the value will be located in the key buffer
      _bufferIndex = bufferList.size();
      _bufferPosition = 0;
      if (!addDedicatedBuffer(newBuf)) {
        return ValueAdditionResult.MessageGroupFull;
      }
    }

    if (uncompressedValueSize == 0) {
      uncompressedValueSize = bytesToStoreSize;
    }
    compressedValueSize = bytesToStoreSize;

    addValueHelper(dhtKey,
                   _bufferIndex,
                   _bufferPosition,
                   uncompressedValueSize,
                   compressedValueSize,
                   bytesToChecksumBuf);

    return ValueAdditionResult.Added;
  }

  public void addValueDedicated(DHTKey dhtKey, ByteBuffer valueBuf) {
    if (!addDedicatedBuffer(valueBuf)) {
      throw new RuntimeException("Too many buffers");
    }
    int _bufferIndex = bufferList.size() - 1;
    int _bufferPosition = 0;
    int uncompressedValueSize = valueBuf.limit();
    int compressedValueSize = uncompressedValueSize;
    addValueHelper(dhtKey, _bufferIndex, _bufferPosition, uncompressedValueSize, compressedValueSize, valueBuf);
    // Handle the side effect expected by addValueHelper.
    // In the future eliminate this side effect and this code.
    if (valueBuf.position() == 0) {
      valueBuf.position(valueBuf.limit());
    }
  }

  // NOTE: currently this method counts on the fact that checksum will move
  // the position of the bytesToChecksumBuf. FUTURE - eliminate this side effect.
  private void addValueHelper(DHTKey dhtKey,
                              int _bufferIndex,
                              int _bufferPosition,
                              int uncompressedValueSize,
                              int compressedValueSize,
                              ByteBuffer bytesToChecksumBuf) {
    // key byte buffer update
    addKey(dhtKey);
    keyByteBuffer.putInt(_bufferIndex);
    keyByteBuffer.putInt(_bufferPosition);
    keyByteBuffer.putInt(uncompressedValueSize);
    keyByteBuffer.putInt(compressedValueSize);

    // append the checksum
    if (uncompressedValueSize <= compressedValueSize || checksumCompressedValues) {
      checksum.checksum(bytesToChecksumBuf, keyByteBuffer);
    } else {
      checksum.emptyChecksum(keyByteBuffer);
    }
  }

  public static ByteBuffer getOptionBuffer(MessageGroup messageGroup) {
    // This protocol appends the optionBuffer to its baseClass(ProtoValueMessageGroupBase)'s bufferList
    boolean hasTraceId = TraceIDProvider.hasTraceID(messageGroup.getMessageType());
    int startIdx = ProtoValueMessageGroupBase.getOptionsByteBufferBaseOffset(hasTraceId);
    return messageGroup.getBuffers()[startIdx];
  }

  public void getMostRecentChecksum(byte[] checksum) {
    if (checksum.length > 0) {
      BufferUtil.get(keyByteBuffer, keyByteBuffer.limit() - checksum.length, checksum, checksum.length);
    }
  }

  // TODO (OPTIMUS-43373): Remove this legacy check once client is using new puts
  private static boolean messageIsLegacy(MessageGroup messageGroup) {
    return messageGroup.getMessageType() == MessageType.LEGACY_PUT ||
           messageGroup.getMessageType() == MessageType.LEGACY_PUT_TRACE;
  }
}