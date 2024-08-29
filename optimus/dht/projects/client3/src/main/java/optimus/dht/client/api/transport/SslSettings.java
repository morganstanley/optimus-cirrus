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

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import optimus.dht.common.api.transport.SslRuntimeImplementation;

public class SslSettings implements Serializable {

  private final String keystoreFile;
  private final SslStoreFormat keystoreFormat;
  private final String keystorePass;
  private final String keyPass;
  private final KeyManagerFactory keyManagerFactory;
  private final String truststoreFile;
  private final SslStoreFormat truststoreFormat;
  private final String truststorePass;
  private final TrustManagerFactory trustManagerFactory;
  private final SslRuntimeImplementation runtimeImplementation;
  private final Duration handshakeTimeout;

  private SslSettings(
      String keystoreFile,
      SslStoreFormat keystoreFormat,
      String keystorePass,
      String keyPass,
      KeyManagerFactory keyManagerFactory,
      String truststoreFile,
      SslStoreFormat truststoreFormat,
      String truststorePass,
      TrustManagerFactory trustManagerFactory,
      SslRuntimeImplementation runtimeImplementation,
      Duration handshakeTimeout) {
    this.keystoreFile = keystoreFile;
    this.keystoreFormat = keystoreFormat;
    this.keystorePass = keystorePass;
    this.keyPass = keyPass;
    this.keyManagerFactory = keyManagerFactory;
    this.truststoreFile = truststoreFile;
    this.truststoreFormat = truststoreFormat;
    this.truststorePass = truststorePass;
    this.trustManagerFactory = trustManagerFactory;
    this.runtimeImplementation = runtimeImplementation;
    this.handshakeTimeout = handshakeTimeout;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String keystoreFile() {
    return keystoreFile;
  }

  public SslStoreFormat keystoreFormat() {
    return keystoreFormat;
  }

  public String keystorePass() {
    return keystorePass;
  }

  public String keyPass() {
    return keyPass;
  }

  public KeyManagerFactory keyManagerFactory() {
    return keyManagerFactory;
  }

  public String truststoreFile() {
    return truststoreFile;
  }

  public SslStoreFormat truststoreFormat() {
    return truststoreFormat;
  }

  public String truststorePass() {
    return truststorePass;
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

  public static class Builder {
    private String keystoreFile = null;
    private SslStoreFormat keystoreFormat = SslStoreFormat.JKS;
    private String keystorePass = null;
    private String keyPass = null;
    private KeyManagerFactory keyManagerFactory = null;
    private String truststoreFile = null;
    private SslStoreFormat truststoreFormat = SslStoreFormat.JKS;
    private String truststorePass = null;
    private TrustManagerFactory trustManagerFactory = null;
    private SslRuntimeImplementation runtimeImplementation = SslRuntimeImplementation.TCNATIVE;
    private Duration handshakeTimeout = Duration.ofSeconds(10);

    public Builder keystoreFile(String keystoreFile) {
      this.keystoreFile = keystoreFile;
      this.keyManagerFactory = null;
      return this;
    }

    public Builder keystoreFormat(SslStoreFormat keystoreFormat) {
      this.keystoreFormat = keystoreFormat;
      return this;
    }

    public Builder keystorePass(String keystorePass) {
      this.keystorePass = keystorePass;
      return this;
    }

    public Builder keyPass(String keyPass) {
      this.keyPass = keyPass;
      return this;
    }

    public Builder keyManagerFactory(KeyManagerFactory keyManagerFactory) {
      this.keyManagerFactory = keyManagerFactory;
      this.keystoreFile = null;
      return this;
    }

    public Builder truststoreFile(String truststoreFile) {
      this.truststoreFile = truststoreFile;
      this.trustManagerFactory = null;
      return this;
    }

    public Builder truststoreFormat(SslStoreFormat truststoreFormat) {
      this.truststoreFormat = truststoreFormat;
      return this;
    }

    public Builder truststorePass(String truststorePass) {
      this.truststorePass = truststorePass;
      return this;
    }

    public Builder trustManagerFactory(TrustManagerFactory trustManagerFactory) {
      this.trustManagerFactory = trustManagerFactory;
      this.truststoreFile = null;
      return this;
    }

    public Builder runtimeImplementation(SslRuntimeImplementation runtimeImplementation) {
      this.runtimeImplementation = runtimeImplementation;
      return this;
    }

    public Builder handshakeTimeout(Duration handshakeTimeout) {
      this.handshakeTimeout = handshakeTimeout;
      return this;
    }

    public SslSettings build() {
      if (keystoreFile == null && keyManagerFactory == null) {
        throw new IllegalArgumentException("Either keystoreFile or keyManagerFactory must be set");
      }
      return new SslSettings(
          keystoreFile,
          keystoreFormat,
          keystorePass,
          keyPass,
          keyManagerFactory,
          truststoreFile,
          truststoreFormat,
          truststorePass,
          trustManagerFactory,
          runtimeImplementation,
          handshakeTimeout);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SslSettings that = (SslSettings) o;
    return Objects.equals(keystoreFile, that.keystoreFile)
        && keystoreFormat == that.keystoreFormat
        && Objects.equals(keystorePass, that.keystorePass)
        && Objects.equals(keyPass, that.keyPass)
        && Objects.equals(keyManagerFactory, that.keyManagerFactory)
        && Objects.equals(truststoreFile, that.truststoreFile)
        && truststoreFormat == that.truststoreFormat
        && Objects.equals(truststorePass, that.truststorePass)
        && Objects.equals(trustManagerFactory, that.trustManagerFactory)
        && Objects.equals(handshakeTimeout, that.handshakeTimeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keystoreFile,
        keystoreFormat,
        keystorePass,
        keyPass,
        keyManagerFactory,
        truststoreFile,
        truststoreFormat,
        truststorePass,
        trustManagerFactory,
        handshakeTimeout);
  }

  @Override
  public String toString() {
    return "SslSettings["
        + "keystoreFile="
        + keystoreFile
        + ", keystoreFormat="
        + keystoreFormat
        + ", keyManagerFactory="
        + keyManagerFactory
        + ", truststoreFile="
        + truststoreFile
        + ", truststoreFormat="
        + truststoreFormat
        + ", trustManagerFactory="
        + trustManagerFactory
        + ", runtimeImplementation="
        + runtimeImplementation
        + ", handshakeTimeout="
        + handshakeTimeout
        + ']';
  }
}
