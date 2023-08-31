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
import java.nio.channels.SocketChannel;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.async.AsyncSendListener;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.OutgoingData.Priority;
import com.ms.silverking.net.async.QueueingConnection;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;

public class MessageGroupConnection
    extends QueueingConnection<OutgoingMessageGroup, IncomingMessageGroup> {
  private final MessageGroupReceiver receiver;

  public MessageGroupConnection(
      SocketChannel channel,
      SelectorController<MessageGroupConnection> selectorController,
      ConnectionListener connectionListener,
      MessageGroupReceiver receiver,
      QueueingConnectionLimitListener limitListener,
      int queueLimit) {
    super(channel, selectorController, connectionListener, true, false, limitListener, queueLimit);
    this.receiver = receiver;
  }

  public MessageGroupConnection(
      SocketChannel channel,
      SelectorController<MessageGroupConnection> selectorController,
      ConnectionListener connectionListener,
      MessageGroupReceiver receiver) {
    this(channel, selectorController, connectionListener, receiver, null, Integer.MAX_VALUE);
  }

  @Override
  protected IncomingMessageGroup createIncomingData(boolean debug) {
    return new IncomingMessageGroup(debug);
  }

  @Override
  protected OutgoingMessageGroup wrapForSend(
      Object data, UUIDBase sendUUID, AsyncSendListener asyncSendListener, long deadline)
      throws IOException {
    return new OutgoingMessageGroup(
        (MessageGroup) data, sendUUID, asyncSendListener, deadline, Priority.NORMAL);
  }

  protected void readComplete(IncomingMessageGroup incomingMessageGroup) throws IOException {
    receiver.receive(incomingMessageGroup.getMessageGroup(), this);
  }
}
