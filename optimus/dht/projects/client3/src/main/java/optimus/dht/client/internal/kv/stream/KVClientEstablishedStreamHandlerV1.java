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
package optimus.dht.client.internal.kv.stream;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import optimus.dht.client.api.kv.KVLargeValue;
import optimus.dht.client.internal.kv.message.KVGenericResponse;
import optimus.dht.client.internal.kv.message.KVResponse;
import optimus.dht.client.internal.kv.message.KVValueResponse;
import optimus.dht.common.util.transport.RawStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import optimus.dht.client.api.callback.CallbackRegistry;
import optimus.dht.client.api.kv.KVValue;
import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.common.api.kv.KVResponseType;
import optimus.dht.common.api.transport.CorruptedStreamException;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.GenericResponseMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.ValueEntryMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.ValueResponseMessage;
import optimus.dht.common.util.transport.BaseHybridEstablishedStreamHandler;

public class KVClientEstablishedStreamHandlerV1
    extends BaseHybridEstablishedStreamHandler<KVResponseType, KVResponse> {

  private static final int DIRECT_BUFFER_SIZE_SHIFT = 28; // 256MB direct buffers
  private static final int DIRECT_BUFFER_SIZE = 1 << DIRECT_BUFFER_SIZE_SHIFT;

  private static final Logger logger =
      LoggerFactory.getLogger(KVClientEstablishedStreamHandlerV1.class);

  private final ServerConnection serverNode;
  private final CallbackRegistry callbackRegistry;

  public KVClientEstablishedStreamHandlerV1(
      ServerConnection serverNode, CallbackRegistry callbackRegistry) {
    this.serverNode = serverNode;
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  protected KVResponseType lookupMessageType(int type) {
    return KVResponseType.getTypeById(type);
  }

  @Override
  protected void processMessageHeader() {
    try {
      switch (messageType) {
        case VALUE:
          processProtobufHeader(ValueResponseMessage.parseFrom(headerMsg));
          break;

        case GENERIC:
          processProtobufHeader(GenericResponseMessage.parseFrom(headerMsg));
          break;
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptedStreamException("Received corrupted protobuf message", e);
    }
  }

  protected void processProtobufHeader(ValueResponseMessage message) {
    long requestId = message.getRequestId();

    if (message.hasErrorDetails()) {
      String errorCode = message.getErrorDetails().getErrorCode();
      String errorText = message.getErrorDetails().getErrorText();
      currentMessage = new KVValueResponse(requestId, null, errorCode, errorText);
    } else {
      int valuesCount = message.getValuesCount();

      if (valuesCount == 0) {
        throw new CorruptedStreamException("Message cannot have no values");
      }

      payloadsToRead = new ArrayDeque<>(valuesCount);
      List<KVLargeValue> values = new ArrayList<>(valuesCount);

      List<ValueEntryMessage> valuesList = message.getValuesList();
      for (int i = 0; i < valuesCount; ++i) {
        ValueEntryMessage valueMessage = valuesList.get(i);
        long valueLength = valueMessage.getValueLength();
        if (valueLength == -1) {
          values.add(null);
        } else {
          String entryInfo = valueMessage.hasEntryInfo() ? valueMessage.getEntryInfo() : null;
          Instant expiry =
              valueMessage.hasExpiry() ? Instant.ofEpochMilli(valueMessage.getExpiry()) : null;

          if (valueLength < Integer.MAX_VALUE) {
            byte[] value = new byte[(int) valueLength];
            KVLargeValue valueEntry =
                new KVLargeValue(new ByteBuffer[] {ByteBuffer.wrap(value)}, entryInfo, expiry);
            values.add(valueEntry);
            payloadsToRead.add(RawStreamReader.create(value));
          } else {
            int fullDirectBuffers = (int) (valueLength >> DIRECT_BUFFER_SIZE_SHIFT);
            int lastDirectBufferSize = (int) (valueLength & (DIRECT_BUFFER_SIZE - 1));
            ByteBuffer[] directBuffers =
                new ByteBuffer[fullDirectBuffers + (lastDirectBufferSize > 0 ? 1 : 0)];
            for (int j = 0; j < fullDirectBuffers; ++j) {
              directBuffers[j] = ByteBuffer.allocateDirect(DIRECT_BUFFER_SIZE);
              payloadsToRead.add(RawStreamReader.create(directBuffers[j]));
            }
            if (lastDirectBufferSize > 0) {
              directBuffers[fullDirectBuffers] = ByteBuffer.allocateDirect(lastDirectBufferSize);
              payloadsToRead.add(RawStreamReader.create(directBuffers[fullDirectBuffers]));
            }
            KVLargeValue valueEntry = new KVLargeValue(directBuffers, entryInfo, expiry);
            values.add(valueEntry);
          }
        }
      }
      currentMessage = new KVValueResponse(requestId, values, null, null);
    }
  }

  protected void processProtobufHeader(GenericResponseMessage message) {
    long requestId = message.getRequestId();

    if (message.hasErrorDetails()) {
      String errorCode = message.getErrorDetails().getErrorCode();
      String errorText = message.getErrorDetails().getErrorText();
      currentMessage = new KVGenericResponse(requestId, null, errorCode, errorText);
    } else {
      int responseCount = message.getResponseCount();

      if (responseCount == 0) {
        throw new CorruptedStreamException("Message cannot have no responses");
      }

      List<Boolean> responses = new ArrayList<>(responseCount);

      responses.addAll(message.getResponseList());
      currentMessage = new KVGenericResponse(requestId, responses, null, null);
    }
  }

  @Override
  protected void processMessageCompleted(ReceivedMessageMetrics metrics) {
    if (!callbackRegistry.complete(
        serverNode, currentMessage.requestId(), currentMessage, metrics)) {
      logger.warn(
          "Unable to find callback for server={}, requestId={}, receivedMessageMetrics={}",
          serverNode,
          currentMessage.requestId(),
          metrics);
    }
  }
}
