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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ms.silverking.id.UUIDBase;

/** A simple default ByteBuffer-based connection. */
public class DefaultConnection
    extends QueueingConnection<OutgoingByteBuffer, NewIncomingBufferedData> {
  private final Receiver receiver;
  private final ByteBuffer readBuffer;

  private static final int defaultReceiveBufferSize = 2 * 1024;

  public DefaultConnection(
      SocketChannel channel,
      SelectorController<DefaultConnection> selectorController,
      ConnectionListener connectionListener,
      Receiver receiver,
      ByteBuffer readBuffer) {
    super(channel, selectorController, connectionListener, true);
    this.receiver = receiver;
    this.readBuffer = readBuffer;
  }

  public DefaultConnection(
      SocketChannel channel,
      SelectorController<DefaultConnection> selectorController,
      ConnectionListener connectionListener,
      Receiver receiver) {
    this(
        channel,
        selectorController,
        connectionListener,
        receiver,
        ByteBuffer.allocate(defaultReceiveBufferSize));
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  protected NewIncomingBufferedData createIncomingData(boolean debug) {
    return new NewIncomingBufferedData(debug);
  }

  @Override
  protected OutgoingByteBuffer wrapForSend(
      Object data, UUIDBase sendUUID, AsyncSendListener asyncSendListener, long deadline)
      throws IOException {
    return OutgoingByteBuffer.wrap((byte[]) data, sendUUID, asyncSendListener);
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public int lockedRead() throws IOException {
    int numRead;

    // Clear out our read buffer so it's ready for new data
    readBuffer.clear();
    // Attempt to read off the channel
    numRead = channel.read(readBuffer);
    if (numRead > 0) {
      receiver.receive(this, readBuffer.array().clone(), numRead);
    }
    return numRead;
  }

  protected void readComplete(NewIncomingBufferedData incomingData) throws IOException {
    // would need a real implementation to use for any non-test purpose
  }

  //////////////////////////////////////////////////////////////////////

  public String toString() {
    return "DefaultConnection:" + super.toString();
  }
}
