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
package com.ms.silverking.net.async;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/** Creates DefaultConnections. */
public class DefaultConnectionCreator implements ConnectionCreator<DefaultConnection> {
  private final ReceiverProvider receiverProvider;
  private final Receiver defaultReceiver;

  public DefaultConnectionCreator(Receiver defaultReceiver, ReceiverProvider receiverProvider) {
    this.defaultReceiver = defaultReceiver;
    this.receiverProvider = receiverProvider;
  }

  public DefaultConnectionCreator(Receiver defaultReceiver) {
    this(defaultReceiver, null);
  }
  //////////////////////////////////////////////////////////////////////

  private Receiver getReceiver(SocketChannel channel) {
    return getReceiver((InetSocketAddress) channel.socket().getRemoteSocketAddress());
  }

  private Receiver getReceiver(InetSocketAddress addr) {
    if (receiverProvider != null) {
      Receiver receiver;

      receiver = receiverProvider.getReceiver(addr);
      if (receiver != null) {
        return receiver;
      } else {
        return defaultReceiver;
      }
    } else {
      return defaultReceiver;
    }
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public DefaultConnection createConnection(
      SocketChannel channel,
      SelectorController<DefaultConnection> selectorController,
      ConnectionListener connectionListener,
      boolean debug) {
    return new DefaultConnection(
        channel, selectorController, connectionListener, getReceiver(channel));
  }
}
