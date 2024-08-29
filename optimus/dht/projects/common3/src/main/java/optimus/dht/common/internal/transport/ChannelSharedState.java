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
package optimus.dht.common.internal.transport;

import java.util.List;
import java.util.Map;

import optimus.dht.common.api.RemoteInstanceIdentity;
import optimus.dht.common.api.transport.CloseableStreamHandler;

/** Instance of this class is shared between all netty handlers for a given connection. */
public class ChannelSharedState {

  private AuthenticatedConnection authenticatedConnection;
  private RemoteInstanceIdentity remoteInstanceIdentity;

  private Map<Integer, String> remoteCodeVersions;
  private Map<Integer, Integer> negotiatedProtocolVersions;

  private List<CloseableStreamHandler> handlers;
  private FailableTransportMessagesQueue messagesQueue;

  public AuthenticatedConnection authenticatedConnection() {
    return authenticatedConnection;
  }

  public void authenticatedConnection(AuthenticatedConnection authenticatedConnection) {
    this.authenticatedConnection = authenticatedConnection;
  }

  public RemoteInstanceIdentity remoteInstanceIdentity() {
    return remoteInstanceIdentity;
  }

  public void remoteInstanceIdentity(RemoteInstanceIdentity remoteInstanceIdentity) {
    this.remoteInstanceIdentity = remoteInstanceIdentity;
  }

  public Map<Integer, String> remoteCodeVersions() {
    return remoteCodeVersions;
  }

  public void remoteCodeVersions(Map<Integer, String> remoteCodeVersions) {
    this.remoteCodeVersions = remoteCodeVersions;
  }

  public Map<Integer, Integer> negotiatedProtocolVersions() {
    return negotiatedProtocolVersions;
  }

  public void negotiatedProtocolVersions(Map<Integer, Integer> negotiatedProtocolVersions) {
    this.negotiatedProtocolVersions = negotiatedProtocolVersions;
  }

  public List<CloseableStreamHandler> handlers() {
    return handlers;
  }

  public void handlers(List<CloseableStreamHandler> handlers) {
    this.handlers = handlers;
  }

  public void messagesQueue(FailableTransportMessagesQueue messagesQueue) {
    this.messagesQueue = messagesQueue;
  }

  public FailableTransportMessagesQueue messagesQueue() {
    return messagesQueue;
  }
}
