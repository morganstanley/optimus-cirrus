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
package com.ms.silverking.cloud.dht.client.impl;

import java.util.List;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoVersionedBasicOpMessageGroup;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public abstract class AsyncVersionedBasicOperationImpl extends AsyncNamespaceOperationImpl {
  private final VersionedBasicNamespaceOperation versionedOperation;

  private static Logger log = LoggerFactory.getLogger(AsyncVersionedBasicOperationImpl.class);

  public AsyncVersionedBasicOperationImpl(
      VersionedBasicNamespaceOperation versionedOperation,
      Context context,
      long curTime,
      byte[] originator) {
    super(versionedOperation, context, curTime, originator);
    this.versionedOperation = versionedOperation;
  }

  @Override
  protected NonExistenceResponse getNonExistenceResponse() {
    return null;
  }

  @Override
  public void waitForCompletion() throws OperationException {
    super._waitForCompletion();
  }

  @Override
  protected int opWorkItems() {
    return 1;
  }

  @Override
  void addToEstimate(MessageEstimate estimate) {}

  @Override
  MessageEstimate createMessageEstimate() {
    return null;
  }

  // FUTURE - think about moving this
  private static MessageType clientOpTypeToMessageType(ClientOpType clientOpType) {
    switch (clientOpType) {
      case SNAPSHOT:
        return MessageType.SNAPSHOT;
      case SYNC_REQUEST:
        return MessageType.SYNC_REQUEST;
      default:
        throw new RuntimeException("Unsupported clientOpType: " + clientOpType);
    }
  }

  @Override
  ProtoMessageGroup createProtoMG(MessageEstimate estimate) {
    return new ProtoVersionedBasicOpMessageGroup(
        clientOpTypeToMessageType(operation.getOpType()),
        operation.getUUID(),
        context.contextAsLong(),
        versionedOperation.getVersion(),
        originator);
  }

  @Override
  ProtoMessageGroup createMessagesForIncomplete(
      ProtoMessageGroup protoMG, List<MessageGroup> messageGroups, MessageEstimate estimate) {
    ((ProtoVersionedBasicOpMessageGroup) protoMG).setNonEmpty();
    protoMG.addToMessageGroupList(messageGroups);
    return createProtoMG(estimate);
  }
}
