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
package optimus.dht.client.internal.transport;

import java.time.Duration;
import java.util.concurrent.Executor;

import optimus.dht.client.api.transport.IdleConnectionStrategy;
import optimus.dht.common.internal.transport.SslTransportConfig;

public class ClientTransportConfig {

  private final Executor kerberosExecutor;
  private final SslTransportConfig sslTransportConfig;
  private final int threads;
  private final Duration connectTimeout;
  private final int sendBufferSize;
  private final int writeLowWaterMark;
  private final int writeHighWaterMark;
  private final int readMinBufferSize;
  private final int readMaxBufferSize;
  private final boolean asyncDns;
  private final IdleConnectionStrategy idleConnectionStrategy;

  public ClientTransportConfig(
      Executor kerberosExecutor,
      SslTransportConfig sslTransportConfig,
      int threads,
      Duration connectTimeout,
      int sendBufferSize,
      int writeLowWaterMark,
      int writeHighWaterMark,
      int readMinBufferSize,
      int readMaxBufferSize,
      boolean asyncDns,
      IdleConnectionStrategy idleConnectionStrategy) {
    this.kerberosExecutor = kerberosExecutor;
    this.sslTransportConfig = sslTransportConfig;
    this.threads = threads;
    this.connectTimeout = connectTimeout;
    this.sendBufferSize = sendBufferSize;
    this.writeLowWaterMark = writeLowWaterMark;
    this.writeHighWaterMark = writeHighWaterMark;
    this.readMinBufferSize = readMinBufferSize;
    this.readMaxBufferSize = readMaxBufferSize;
    this.asyncDns = asyncDns;
    this.idleConnectionStrategy = idleConnectionStrategy;
  }

  public Executor kerberosExecutor() {
    return kerberosExecutor;
  }

  public SslTransportConfig sslTransportConfig() {
    return sslTransportConfig;
  }

  public int threads() {
    return threads;
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public int sendBufferSize() {
    return sendBufferSize;
  }

  public int writeLowWaterMark() {
    return writeLowWaterMark;
  }

  public int writeHighWaterMark() {
    return writeHighWaterMark;
  }

  public int readMinBufferSize() {
    return readMinBufferSize;
  }

  public int readMaxBufferSize() {
    return readMaxBufferSize;
  }

  public boolean asyncDns() {
    return asyncDns;
  }

  public IdleConnectionStrategy idleConnectionStrategy() {
    return idleConnectionStrategy;
  }
}
