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

import java.nio.channels.ServerSocketChannel;

/**
 * Wraps a ServerSocketChannel with a ChannelRegistrationWorker so that
 * ChannelRegistrationWorker can be informed when this channel is registered.
 */
final class NewServerSocketChannel {
  private final ServerSocketChannel serverSocketChannel;
  private final ChannelRegistrationWorker channelRegistrationWorker;

  public NewServerSocketChannel(ServerSocketChannel serverSocketChannel,
      ChannelRegistrationWorker channelRegistrationWorker) {
    this.serverSocketChannel = serverSocketChannel;
    this.channelRegistrationWorker = channelRegistrationWorker;
  }

  public ServerSocketChannel getServerSocketChannel() {
    return serverSocketChannel;
  }

  public ChannelRegistrationWorker getChannelRegistrationWorker() {
    return channelRegistrationWorker;
  }

  public String toString() {
    return serverSocketChannel + ":" + channelRegistrationWorker;
  }
}
