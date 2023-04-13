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

import java.nio.channels.SocketChannel;

import com.ms.silverking.net.async.ConnectionCreator;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;

public class MessageGroupConnectionCreator implements ConnectionCreator<MessageGroupConnection> {
  private final MessageGroupReceiver messageGroupReceiver;
  private final QueueingConnectionLimitListener limitListener;
  private final int queueLimit;

  public MessageGroupConnectionCreator(MessageGroupReceiver messageGroupReceiver,
      QueueingConnectionLimitListener limitListener, int queueLimit) {
    this.messageGroupReceiver = messageGroupReceiver;
    this.limitListener = limitListener;
    this.queueLimit = queueLimit;
  }

  public MessageGroupConnectionCreator(MessageGroupReceiver messageGroupReceiver) {
    this(messageGroupReceiver, null, Integer.MAX_VALUE);
  }

  @Override
  public MessageGroupConnection createConnection(SocketChannel channel,
      SelectorController<MessageGroupConnection> selectorController, ConnectionListener connectionListener,
      boolean debug) {
    return new MessageGroupConnection(channel, selectorController, connectionListener, messageGroupReceiver,
        limitListener, queueLimit);
  }
}
