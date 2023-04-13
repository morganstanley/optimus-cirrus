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
package com.ms.silverking.net.test;

import java.nio.channels.SocketChannel;

import com.ms.silverking.net.async.ConnectionCreator;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.SelectorController;

/**
 * ConnectionCreator for BufferedData.
 */
public class BufferedDataConnectionCreator implements ConnectionCreator<BufferedDataConnection> {
  private final BufferedDataReceiver bufferedDataReceiver;

  public BufferedDataConnectionCreator(BufferedDataReceiver bufferedDataReceiver) {
    this.bufferedDataReceiver = bufferedDataReceiver;
  }

  @Override
  public BufferedDataConnection createConnection(SocketChannel channel,
      SelectorController<BufferedDataConnection> selectorController, ConnectionListener connectionListener,
      boolean debug) {
    return new BufferedDataConnection(channel, selectorController, connectionListener, bufferedDataReceiver);
  }
}
