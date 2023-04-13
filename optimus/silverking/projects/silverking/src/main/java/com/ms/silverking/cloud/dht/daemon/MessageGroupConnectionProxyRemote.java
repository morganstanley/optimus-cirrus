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
package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;
import java.util.Optional;

import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.async.AsyncSendListener;

class MessageGroupConnectionProxyRemote implements MessageGroupConnectionProxy {
  private final MessageGroupConnection connection;

  MessageGroupConnectionProxyRemote(MessageGroupConnection connection) {
    this.connection = connection;
  }

  @Override
  public void sendAsynchronous(Object data, long deadline) throws IOException {
    //Log.info("MessageGroupConnectionProxyRemote sending to ", connection.getRemoteIPAndPort());
    connection.sendAsynchronous(data, deadline);
  }

  @Override
  public void sendAsynchronousWithTrace(Object data, long deadline, UUIDBase sendID, AsyncSendListener asyncSendListener) throws IOException {
    connection.sendAsynchronous(data, sendID, asyncSendListener, deadline);
  }

  @Override
  public String getConnectionID() {
    return connection.getRemoteSocketAddress().toString();
  }

  public MessageGroupConnection getConnection() {
    return connection;
  }

  @Override
  public Optional<String> getAuthenticatedUser() {
    return connection.getAuthenticatedUser();
  }

}
