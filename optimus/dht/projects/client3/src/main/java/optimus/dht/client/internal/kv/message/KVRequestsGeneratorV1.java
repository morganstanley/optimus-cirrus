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
package optimus.dht.client.internal.kv.message;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import com.google.protobuf.UnsafeByteOperations;
import optimus.dht.client.api.Key;
import optimus.dht.client.api.kv.KVEntry;
import optimus.dht.client.api.kv.KVLargeEntry;
import optimus.dht.client.api.kv.KVLargeValue;
import optimus.dht.client.api.kv.KVValue;
import optimus.dht.common.api.Keyspace;
import optimus.dht.common.api.kv.KVRequestType;
import optimus.dht.common.api.transport.MessageStream;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.KeyAndValueMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.KeyEntryMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.KeyOnlyRequestMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.KeyspaceAttributeMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.KeyspaceMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.PutRequestMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.RemoveTransientKeyspaceRequestMessage;
import optimus.dht.common.internal.transport.generated.KeyValueProto2V1.ValueEntryMessage;
import optimus.dht.common.util.transport.HybridProtobufByteArrayMessageStream;
import optimus.dht.common.util.transport.HybridProtobufByteBufferMessageStream;

public class KVRequestsGeneratorV1 {

  private KVRequestsGeneratorV1() {}

  private static KeyspaceMessage.Builder keyspace(Keyspace keyspace) {
    KeyspaceMessage.Builder keyspaceBuilder = KeyspaceMessage.newBuilder();
    keyspaceBuilder.setName(keyspace.name());
    if (keyspace.allocation() != null) {
      keyspaceBuilder.setAllocation(keyspace.allocation());
    }
    keyspace
        .extraAttrs()
        .forEach(
            (key, value) -> {
              if (key.startsWith(Keyspace.SPECIAL_PREFIX)) {
                throw new IllegalArgumentException(
                    "Custom keyspace attribute " + key + " starts with illegal character");
              }
              keyspaceBuilder.addAttributes(
                  KeyspaceAttributeMessage.newBuilder().setKey(key).setValue(value));
            });

    return keyspaceBuilder;
  }

  public static MessageStream keyOnly(
      KVRequestType requestType,
      long requestId,
      Keyspace keyspace,
      List<? extends Key> keys,
      String correlationName) {

    Deque<byte[]> payloads = new LinkedList<>();
    KeyOnlyRequestMessage.Builder requestBuilder = KeyOnlyRequestMessage.newBuilder();
    requestBuilder.setRequestId(requestId);
    if (correlationName != null) {
      requestBuilder.setCorrelationName(correlationName);
    }

    requestBuilder.setKeyspace(keyspace(keyspace));

    for (Key key : keys) {
      requestBuilder.addKeys(
          KeyEntryMessage.newBuilder()
              .setKeyHash(UnsafeByteOperations.unsafeWrap(key.hash()))
              .setKeyLength(key.key().length));
      payloads.add(key.key());
    }

    KeyOnlyRequestMessage requestMessage = requestBuilder.build();

    return new HybridProtobufByteArrayMessageStream(
        requestType.getTypeIdShort(), requestMessage, payloads);
  }

  public static <K extends Key> MessageStream put(
      long requestId, Keyspace keyspace, List<KVEntry<K>> values, String correlationName) {

    PutRequestMessage.Builder requestBuilder = PutRequestMessage.newBuilder();
    requestBuilder.setRequestId(requestId);
    if (correlationName != null) {
      requestBuilder.setCorrelationName(correlationName);
    }

    requestBuilder.setKeyspace(keyspace(keyspace));

    Deque<byte[]> payloads = new LinkedList<>();

    for (KVEntry<?> entry : values) {
      Key key = entry.key();
      KVValue value = entry.value();

      KeyEntryMessage.Builder keyMessage =
          KeyEntryMessage.newBuilder()
              .setKeyHash(UnsafeByteOperations.unsafeWrap(key.hash()))
              .setKeyLength(key.key().length);
      ValueEntryMessage.Builder valueMessage =
          ValueEntryMessage.newBuilder().setValueLength(value.bytes().length);
      if (value.entryInfo() != null) {
        valueMessage.setEntryInfo(value.entryInfo());
      }
      if (value.expiry() != null) {
        valueMessage.setExpiry(value.expiry().toEpochMilli());
      }

      requestBuilder.addValues(
          KeyAndValueMessage.newBuilder().setKey(keyMessage).setValue(valueMessage));

      payloads.add(key.key());
      payloads.add(value.bytes());
    }

    PutRequestMessage requestMessage = requestBuilder.build();
    return new HybridProtobufByteArrayMessageStream(
        KVRequestType.PUT.getTypeIdShort(), requestMessage, payloads);
  }

  public static <K extends Key> MessageStream largePut(
      long requestId, Keyspace keyspace, KVLargeEntry<K> largeEntry, String correlationName) {

    PutRequestMessage.Builder requestBuilder = PutRequestMessage.newBuilder();
    requestBuilder.setRequestId(requestId);
    if (correlationName != null) {
      requestBuilder.setCorrelationName(correlationName);
    }

    requestBuilder.setKeyspace(keyspace(keyspace));

    Deque<ByteBuffer> payloads = new LinkedList<>();

    Key key = largeEntry.key();
    KVLargeValue value = largeEntry.value();

    long valueLength = 0;
    for (ByteBuffer buffer : largeEntry.value().buffers()) {
      valueLength += buffer.remaining();
    }

    KeyEntryMessage.Builder keyMessage =
        KeyEntryMessage.newBuilder()
            .setKeyHash(UnsafeByteOperations.unsafeWrap(key.hash()))
            .setKeyLength(key.key().length);
    ValueEntryMessage.Builder valueMessage =
        ValueEntryMessage.newBuilder().setValueLength(valueLength);
    if (value.entryInfo() != null) {
      valueMessage.setEntryInfo(value.entryInfo());
    }
    if (value.expiry() != null) {
      valueMessage.setExpiry(value.expiry().toEpochMilli());
    }

    requestBuilder.addValues(
        KeyAndValueMessage.newBuilder().setKey(keyMessage).setValue(valueMessage));

    payloads.add(ByteBuffer.wrap(key.key()));
    for (ByteBuffer byteBuffer : value.buffers()) {
      payloads.add(byteBuffer.slice());
    }

    PutRequestMessage requestMessage = requestBuilder.build();
    return new HybridProtobufByteBufferMessageStream(
        KVRequestType.PUT.getTypeIdShort(), requestMessage, payloads);
  }

  public static MessageStream removeTransientKeyspace(
      long requestId, Keyspace keyspace, String correlationName) {
    RemoveTransientKeyspaceRequestMessage.Builder requestBuilder =
        RemoveTransientKeyspaceRequestMessage.newBuilder();
    requestBuilder.setRequestId(requestId);
    if (correlationName != null) {
      requestBuilder.setCorrelationName(correlationName);
    }

    requestBuilder.setKeyspace(keyspace(keyspace));
    RemoveTransientKeyspaceRequestMessage requestMessage = requestBuilder.build();

    return new HybridProtobufByteArrayMessageStream(
        KVRequestType.REMOVE_TRANSIENT_KEYSPACE.getTypeIdShort(),
        requestMessage,
        new ArrayDeque<>(1));
  }
}
