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

public class SentMessageMetrics {

  public static SentMessageMetrics NOT_SENT = new SentMessageMetrics(0, 0, 0, 0, 0, 0, 0);

  private final long bytesWritten;
  private final long submittedNanoTs;
  private final long connectedNanoTs;
  private final long accessedNanoTs;
  private final long firstWriteNanoTs;
  private final long lastWriteNanoTs;
  private final long writeHandlerTimeNano;

  public SentMessageMetrics(
      long bytesWritten,
      long submittedNanoTs,
      long connectedNanoTs,
      long accessedNanoTs,
      long firstWriteNanoTs,
      long lastWriteNanoTs,
      long writeHandlerTimeNano) {
    this.bytesWritten = bytesWritten;
    this.submittedNanoTs = submittedNanoTs;
    this.connectedNanoTs = connectedNanoTs;
    this.accessedNanoTs = accessedNanoTs;
    this.firstWriteNanoTs = firstWriteNanoTs;
    this.lastWriteNanoTs = lastWriteNanoTs;
    this.writeHandlerTimeNano = writeHandlerTimeNano;
  }

  public long bytesWritten() {
    return bytesWritten;
  }

  public long submittedNanoTs() {
    return submittedNanoTs;
  }

  public long connectedNanoTs() {
    return connectedNanoTs;
  }

  public long connectTimeNano() {
    return connectedNanoTs - submittedNanoTs;
  }

  public long accessedNanoTs() {
    return accessedNanoTs;
  }

  public long firstWriteNanoTs() {
    return firstWriteNanoTs;
  }

  public long queueTimeNano() {
    return accessedNanoTs - connectedNanoTs;
  }

  public long lastWriteNanoTs() {
    return lastWriteNanoTs;
  }

  public long writeTimeNano() {
    return lastWriteNanoTs - accessedNanoTs;
  }

  public long writeHandlerTimeNano() {
    return writeHandlerTimeNano;
  }

  @Override
  public String toString() {
    return "[bytesWritten="
        + bytesWritten
        + ", connectedTime="
        + StringTool.formatNanosAsMillisFraction(connectTimeNano())
        + ", queueTime="
        + StringTool.formatNanosAsMillisFraction(queueTimeNano())
        + ", writeTime="
        + StringTool.formatNanosAsMillisFraction(writeTimeNano())
        + ", writeHandlerTime="
        + StringTool.formatNanosAsMillisFraction(writeHandlerTimeNano)
        + "]";
  }
}
