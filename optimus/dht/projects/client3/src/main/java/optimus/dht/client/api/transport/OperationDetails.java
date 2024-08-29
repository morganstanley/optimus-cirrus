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
package optimus.dht.client.api.transport;

import javax.annotation.Nullable;

import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;
import optimus.dht.common.api.transport.SentMessageMetrics;
import optimus.dht.common.util.StringTool;

public class OperationDetails {

  public static final OperationDetails EMPTY = new OperationDetails(null, null, null);

  @Nullable private final ServerConnection server;
  @Nullable private final SentMessageMetrics sentMessageMetrics;
  @Nullable private final ReceivedMessageMetrics receivedMessageMetrics;

  public OperationDetails(
      @Nullable ServerConnection server,
      @Nullable SentMessageMetrics sentMessageMetrics,
      @Nullable ReceivedMessageMetrics receivedMessageMetrics) {
    this.server = server;
    this.sentMessageMetrics = sentMessageMetrics;
    this.receivedMessageMetrics = receivedMessageMetrics;
  }

  @Nullable
  public ServerConnection server() {
    return server;
  }

  @Nullable
  public SentMessageMetrics sentMessageMetrics() {
    return sentMessageMetrics;
  }

  @Nullable
  public ReceivedMessageMetrics receivedMessageMetrics() {
    return receivedMessageMetrics;
  }

  @Override
  public String toString() {
    return "["
        + ((sentMessageMetrics != null && receivedMessageMetrics != null)
            ? "requestToResponseTime="
                + StringTool.formatNanosAsMillisFraction(
                    receivedMessageMetrics.firstReadNanoTs() - sentMessageMetrics.lastWriteNanoTs())
                + ", "
            : "")
        + "server="
        + server
        + ", sentMessageMetrics="
        + sentMessageMetrics
        + ", receivedMessageMetrics="
        + receivedMessageMetrics
        + "]";
  }
}
