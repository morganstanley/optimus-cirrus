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

import optimus.dht.common.api.transport.SentMessageMetrics;

public class MutableSentMessageMetrics {

  private final long createdNano = System.nanoTime();

  private long connectedNanoTs;
  private long accessedNanoTs;
  private long firstWriteNanoTs;
  private long lastWriteNanoTs;
  private long writeHandlerTimeNano = 0;

  public static MutableSentMessageMetrics connected() {
    MutableSentMessageMetrics metrics = new MutableSentMessageMetrics();
    metrics.connectedNanoTs = metrics.createdNano;
    return metrics;
  }

  public void touchConnected() {
    this.connectedNanoTs = System.nanoTime();
  }

  public void touchAccessed() {
    this.accessedNanoTs = System.nanoTime();
  }

  public void touchFirstWrite() {
    this.firstWriteNanoTs = System.nanoTime();
  }

  public void touchLastWrite() {
    this.lastWriteNanoTs = System.nanoTime();
  }

  public void touchFirstAndLastWrite() {
    long nanoTime = System.nanoTime();
    this.firstWriteNanoTs = nanoTime;
    this.lastWriteNanoTs = nanoTime;
  }

  public void addToHandlerTime(long timeInNanos) {
    writeHandlerTimeNano += timeInNanos;
  }

  public SentMessageMetrics toMetrics(long bytesWritten) {
    return new SentMessageMetrics(
        bytesWritten,
        createdNano,
        connectedNanoTs,
        accessedNanoTs,
        firstWriteNanoTs,
        lastWriteNanoTs,
        writeHandlerTimeNano);
  }
}
