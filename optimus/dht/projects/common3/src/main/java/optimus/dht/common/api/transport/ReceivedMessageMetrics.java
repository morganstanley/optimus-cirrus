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
package optimus.dht.common.api.transport;

import optimus.dht.common.util.StringTool;

public class ReceivedMessageMetrics {

  private final long bytesRead;
  private final long firstReadNanoTs;
  private final long firstReadMilliTs;
  private final long lastReadNanoTs;
  private final long completedNanoTs;
  private final long readHandlerTimeNano;
  private final long remoteMilliTs;
  private final long remoteSendNano;

  public ReceivedMessageMetrics(
      long bytesRead,
      long firstReadNanoTs,
      long firstReadMilliTs,
      long lastReadNanoTs,
      long completedNanoTs,
      long readHandlerTimeNano,
      long remoteMilliTs,
      long remoteSendNano) {
    this.bytesRead = bytesRead;
    this.firstReadNanoTs = firstReadNanoTs;
    this.firstReadMilliTs = firstReadMilliTs;
    this.lastReadNanoTs = lastReadNanoTs;
    this.completedNanoTs = completedNanoTs;
    this.readHandlerTimeNano = readHandlerTimeNano;
    this.remoteMilliTs = remoteMilliTs;
    this.remoteSendNano = remoteSendNano;
  }

  public long bytesRead() {
    return bytesRead;
  }

  public long firstReadNanoTs() {
    return firstReadNanoTs;
  }

  public long firstReadMilliTs() {
    return firstReadMilliTs;
  }

  public long lastReadNanoTs() {
    return lastReadNanoTs;
  }

  public long completedNanoTs() {
    return completedNanoTs;
  }

  public long readHandlerTimeNano() {
    return readHandlerTimeNano;
  }

  public long remoteMilliTs() {
    return remoteMilliTs;
  }

  public long remoteSendNano() {
    return remoteSendNano;
  }

  public long readTimeNano() {
    return lastReadNanoTs - firstReadNanoTs;
  }

  public long estimatedRemoteLatencyMilli() {
    return firstReadMilliTs - remoteMilliTs;
  }

  @Override
  public String toString() {
    return "[bytesRead="
        + bytesRead
        + ", readTime="
        + StringTool.formatNanosAsMillisFraction(readTimeNano())
        + ", readHandlerTime="
        + StringTool.formatNanosAsMillisFraction(readHandlerTimeNano)
        + ", estimatedRemoteLatency="
        + estimatedRemoteLatencyMilli()
        + " ms, remoteSendTime="
        + StringTool.formatNanosAsMillisFraction(remoteSendNano)
        + "]";
  }
}
