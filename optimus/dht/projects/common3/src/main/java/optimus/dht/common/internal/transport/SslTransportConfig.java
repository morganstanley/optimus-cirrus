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

import java.time.Duration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import optimus.dht.common.api.transport.SslRuntimeImplementation;

public class SslTransportConfig {

  private final KeyManagerFactory keyManagerFactory;
  private final TrustManagerFactory trustManagerFactory;
  private final SslRuntimeImplementation runtimeImplementation;
  private final Duration handshakeTimeout;

  public SslTransportConfig(
      KeyManagerFactory keyManagerFactory,
      TrustManagerFactory trustManagerFactory,
      SslRuntimeImplementation runtimeImplementation,
      Duration handshakeTimeout) {
    this.keyManagerFactory = keyManagerFactory;
    this.trustManagerFactory = trustManagerFactory;
    this.runtimeImplementation = runtimeImplementation;
    this.handshakeTimeout = handshakeTimeout;
  }

  public KeyManagerFactory keyManagerFactory() {
    return keyManagerFactory;
  }

  public TrustManagerFactory trustManagerFactory() {
    return trustManagerFactory;
  }

  public SslRuntimeImplementation runtimeImplementation() {
    return runtimeImplementation;
  }

  public Duration handshakeTimeout() {
    return handshakeTimeout;
  }

  @Override
  public String toString() {
    return "SslTransportConfig[trustManagerFactory="
        + (trustManagerFactory != null)
        + ", runtimeImplementation="
        + runtimeImplementation
        + ", handshakeTimeout="
        + handshakeTimeout
        + ']';
  }
}
