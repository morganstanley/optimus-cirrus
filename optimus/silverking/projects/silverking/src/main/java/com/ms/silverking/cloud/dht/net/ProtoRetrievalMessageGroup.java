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
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoRetrievalMessageGroup extends ProtoKeyedMessageGroup {

  public ProtoRetrievalMessageGroup(
      UUIDBase uuid,
      long context,
      InternalRetrievalOptions retrievalOptions,
      byte[] originator,
      int size,
      int deadlineRelativeMillis,
      ForwardingMode forward,
      SkTraceId maybeTraceID) {
    super(
        TraceIDProvider.isValidTraceID(maybeTraceID)
            ? MessageType.RETRIEVE_TRACE
            : MessageType.RETRIEVE,
        uuid,
        context,
        ByteBuffer.allocate(
            RetrievalMessageFormat.getOptionsBufferLength(retrievalOptions.getRetrievalOptions())),
        size,
        RetrievalMessageFormat.size - KeyedMessageFormat.baseBytesPerKeyEntry,
        originator,
        deadlineRelativeMillis,
        forward,
        maybeTraceID);
    Set<SecondaryTarget> secondaryTargets;
    byte[] userOptions;
    byte[] authorizationUser;
    String zoneId;

    // FUTURE - merge with ProtoValueMessagGroup code?
    bufferList.add(optionsByteBuffer);
    // begin retrievalType, waitMode encoding
    // see getRetrievalOptions() for decoding
    optionsByteBuffer.put(
        (byte)
            ((retrievalOptions.getRetrievalType().ordinal() << 4)
                | retrievalOptions.getWaitMode().ordinal()));
    // end retrievalType, waitMode encoding
    optionsByteBuffer.put(
        (byte)
            (((retrievalOptions.getVerifyIntegrity() ? 1 : 0) << 1)
                | (retrievalOptions.getRetrievalOptions().getUpdateSecondariesOnMiss() ? 1 : 0)));
    VersionConstraint vc = retrievalOptions.getVersionConstraint();
    optionsByteBuffer.putLong(vc.getMin());
    optionsByteBuffer.putLong(vc.getMax());
    optionsByteBuffer.put((byte) vc.getMode().ordinal());
    optionsByteBuffer.putLong(vc.getMaxCreationTime());
    secondaryTargets = retrievalOptions.getRetrievalOptions().getSecondaryTargets();
    if (secondaryTargets == null) {
      optionsByteBuffer.putShort((short) 0);
    } else {
      byte[] serializedST;

      serializedST = SecondaryTargetSerializer.serialize(secondaryTargets);
      optionsByteBuffer.putShort((short) serializedST.length);
      optionsByteBuffer.put(serializedST);
    }

    userOptions = retrievalOptions.getUserOptions();
    if (userOptions == null) {
      optionsByteBuffer.putInt(0);
    } else {
      optionsByteBuffer.putInt(userOptions.length);
      optionsByteBuffer.put(userOptions);
    }

    authorizationUser = retrievalOptions.getAuthorizationUser();
    if (authorizationUser == null) {
      optionsByteBuffer.putInt(0);
      optionsByteBuffer.put(DHTConstants.emptyByteArray);
    } else {
      optionsByteBuffer.putInt(authorizationUser.length);
      optionsByteBuffer.put(authorizationUser);
    }

    zoneId = retrievalOptions.getZoneId();
    if (zoneId == null || zoneId.isEmpty()) {
      optionsByteBuffer.putInt(0);
    } else {
      optionsByteBuffer.putInt(zoneId.getBytes().length);
      optionsByteBuffer.put(zoneId.getBytes());
    }
  }

  /*
  public ProtoRetrievalMessageGroup(UUIDBase uuid, long context, RetrievalOptions retrievalOptions,
          byte[] originator, List<? extends DHTKey> destEntries, int deadlineRelativeMillis) {
      this(uuid, context, retrievalOptions, originator, destEntries.size(),
              deadlineRelativeMillis, ForwardingMode.DO_NOT_FORWARD);
      for (DHTKey key : destEntries) {
          addKey(key);
      }
  }
  */

  public ProtoRetrievalMessageGroup(
      UUIDBase uuid,
      long context,
      InternalRetrievalOptions retrievalOptions,
      byte[] originator,
      Collection<DHTKey> keys,
      int deadlineRelativeMillis,
      SkTraceId maybeTraceID) {
    this(
        uuid,
        context,
        retrievalOptions,
        originator,
        keys.size(),
        deadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD,
        maybeTraceID);
    for (DHTKey key : keys) {
      addKey(key);
    }
  }

  public static ByteBuffer getOptionBuffer(MessageGroup mg) {
    int startIdx;

    // This protocol appends the optionBuffer to its baseClass(ProtoKeyedMessageGroup)'s bufferList
    startIdx =
        ProtoKeyedMessageGroup.getOptionsByteBufferBaseOffset(
            TraceIDProvider.hasTraceID(mg.getMessageType()));
    return mg.getBuffers()[startIdx];
  }

  public static InternalRetrievalOptions getRetrievalOptions(MessageGroup mg) {
    int retrievalWaitByte;
    RetrievalType retrievalType;
    WaitMode waitMode;
    int miscOptionsByte;
    VersionConstraint vc;
    ByteBuffer optionBuffer;
    boolean verifyIntegrity;
    boolean updateSecondariesOnMiss;

    optionBuffer = getOptionBuffer(mg);
    retrievalWaitByte =
        optionBuffer.get(RetrievalResponseMessageFormat.retrievalTypeWaitModeOffset);
    // begin retrievalType, waitMode decoding
    // see ProtoRetrievalMessageGroup() for encoding
    retrievalType = EnumValues.retrievalType[retrievalWaitByte >> 4];
    waitMode = EnumValues.waitMode[retrievalWaitByte & 0x0f];
    // end retrievalType, waitMode decoding
    miscOptionsByte = optionBuffer.get(RetrievalResponseMessageFormat.miscOptionsOffset);
    verifyIntegrity = (miscOptionsByte & 0x2) != 0;
    updateSecondariesOnMiss = (miscOptionsByte & 0x1) != 0;
    vc =
        new VersionConstraint(
            optionBuffer.getLong(RetrievalResponseMessageFormat.vcMinOffset),
            optionBuffer.getLong(RetrievalResponseMessageFormat.vcMaxOffset),
            EnumValues.versionConstraint_Mode[
                optionBuffer.get(RetrievalResponseMessageFormat.vcModeOffset)],
            optionBuffer.getLong(RetrievalResponseMessageFormat.vcMaxStorageTimeOffset));
    return new InternalRetrievalOptions(
            OptionsHelper.newRetrievalOptions(
                retrievalType,
                waitMode,
                vc,
                updateSecondariesOnMiss,
                getSecondaryTargets(mg),
                getUserOptions(mg),
                getAuthorizationUser(mg),
                getZoneId(mg)),
            verifyIntegrity)
        .originator(mg.getOriginator());
  }

  public static int getSTLength(MessageGroup mg) {
    return getOptionBuffer(mg).getShort(RetrievalMessageFormat.stDataOffset);
  }

  private static Set<SecondaryTarget> getSecondaryTargets(MessageGroup mg) {
    int stLength;
    byte[] stDef;

    stLength = getSTLength(mg);
    if (stLength == 0) {
      return DHTConstants.noSecondaryTargets;
    } else {
      stDef = new byte[stLength];
      System.arraycopy(
          getOptionBuffer(mg).array(),
          RetrievalMessageFormat.stDataOffset + NumConversion.BYTES_PER_SHORT,
          stDef,
          0,
          stLength);
      return SecondaryTargetSerializer.deserialize(stDef);
    }
  }

  private static int getUOStart(MessageGroup mg) {
    return RetrievalMessageFormat.stDataOffset + NumConversion.BYTES_PER_SHORT + getSTLength(mg);
  }

  private static int getAUStart(MessageGroup mg) {
    return getUOStart(mg) + NumConversion.BYTES_PER_INT + getUOLength(mg);
  }

  private static int getZIStart(MessageGroup mg) {
    return getAUStart(mg) + NumConversion.BYTES_PER_INT + getAULength(mg);
  }

  private static int getUOLength(MessageGroup mg) {
    int start = getUOStart(mg);
    return getOptionBuffer(mg).getInt(start);
  }

  private static int getAULength(MessageGroup mg) {
    int start = getAUStart(mg);
    return getOptionBuffer(mg).getInt(start);
  }

  private static int getZILength(MessageGroup mg) {
    int start = getZIStart(mg);
    return getOptionBuffer(mg).getInt(start);
  }

  private static byte[] getUserOptions(MessageGroup mg) {
    int uoLength;
    byte[] uo;
    int uoStartPos;

    uoStartPos = getUOStart(mg);
    uoLength = getUOLength(mg);
    if (uoLength == 0) {
      return RetrievalOptions.noUserOptions;
    } else {
      uo = new byte[uoLength];
      ByteBuffer opts = getOptionBuffer(mg);
      opts.position(uoStartPos + NumConversion.BYTES_PER_INT);
      opts.get(uo, 0, uoLength);
      return uo;
    }
  }

  private static byte[] getAuthorizationUser(MessageGroup mg) {
    byte[] au;
    int auLength = getAULength(mg);
    int auStartPos = getAUStart(mg);

    if (auLength == 0) {
      return RetrievalOptions.noAuthorizationUser;
    } else {
      au = new byte[auLength];
      ByteBuffer opts = getOptionBuffer(mg);
      opts.position(auStartPos + NumConversion.BYTES_PER_INT);
      opts.get(au, 0, auLength);
      return au;
    }
  }

  private static String getZoneId(MessageGroup mg) {
    byte[] zi;
    int ziLength = getZILength(mg);
    int ziStartPos = getZIStart(mg);

    if (ziLength == 0) {
      return DHTConstants.noZoneId;
    } else {
      zi = new byte[ziLength];
      ByteBuffer opts = getOptionBuffer(mg);
      opts.position(ziStartPos + NumConversion.BYTES_PER_INT);
      opts.get(zi, 0, ziLength);
      return new String(zi, StandardCharsets.UTF_8);
    }
  }

  protected MessageGroup toMessageGroup(boolean flip) {
    MessageGroup mg;

    mg = super.toMessageGroup(flip);
    return mg;
  }
}
