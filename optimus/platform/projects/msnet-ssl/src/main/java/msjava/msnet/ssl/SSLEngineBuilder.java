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

package msjava.msnet.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/** Builds SSLEngine object based on the SSLEngineConfig */
public class SSLEngineBuilder {
  private static final String SSLENGINE_TYPE = System.getProperty("msjava.msnet.ssl.engine", "JDK");
  private static final Logger LOGGER = LoggerFactory.getLogger(SSLEngineBuilder.class);

  private final SSLEngineConfig config;

  private final KeyManagerFactory keyManagerFactory;
  private final TrustManagerFactory trustManagerFactory;

  public SSLEngineBuilder(SSLEngineConfig config) {
    this.config = config;
    this.keyManagerFactory =
        Optional.ofNullable(config.keystorePath).map(this::createKeyManagerFactory).orElse(null);
    this.trustManagerFactory =
        Optional.ofNullable(config.truststorePath)
            .map(this::createTrustManagerFactory)
            .orElse(null);
  }

  public SSLEngine build(boolean isServer) {
    try {
      if (SSLENGINE_TYPE.equals("OpenSSL")) {
        return createOpenSSLEngine(isServer);
      } else if (SSLENGINE_TYPE.equals("JDK")) {
        return createJdkSSLEngine(isServer);
      } else {
        throw new IllegalArgumentException(
            "Unknown SSLEngine type " + SSLENGINE_TYPE + ", please use OpenSSL or JDK");
      }
    } catch (Exception e) {
      throw new IllegalStateException("Could not create SSLEngine", e);
    }
  }

  private SSLEngine createJdkSSLEngine(boolean isServer) throws Exception {
    KeyManager[] keyManagers =
        Optional.ofNullable(keyManagerFactory).map(KeyManagerFactory::getKeyManagers).orElse(null);
    TrustManager[] trustManagers =
        Optional.ofNullable(trustManagerFactory)
            .map(TrustManagerFactory::getTrustManagers)
            .orElse(null);

    SSLContext context = SSLContext.getInstance(SSLEngineConfig.TLS_PROTOCOL_VERSION);
    context.init(keyManagers, trustManagers, new SecureRandom());

    SSLEngine sslEngine = context.createSSLEngine();
    if (isServer) {
      sslEngine.setUseClientMode(false);
      if (config.enabledClientAuthSSL) {
        sslEngine.setNeedClientAuth(true);
      }
    } else {
      sslEngine.setUseClientMode(true);
    }

    LOGGER.debug(
        "Creating SSLEngine with keystore "
            + config.keystorePath
            + ", truststore="
            + config.truststorePath);
    LOGGER.debug(
        "Creating SSLEngine in Server mode="
            + isServer
            + " and clientAuth="
            + config.enabledClientAuthSSL);

    return sslEngine;
  }

  private List<String> getCiphers() {
    String ciphers =
        System.getProperty("msjava.msnet.ssl.ciphers", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    return Arrays.asList(ciphers.split(":"));
  }

  private SSLEngine createOpenSSLEngine(boolean isServer) throws Exception {
    io.netty.handler.ssl.SslContext sslContext = null;
    if (isServer) {
      List<String> ciphers = getCiphers();
      sslContext =
          SslContextBuilder.forServer(keyManagerFactory)
              .trustManager(trustManagerFactory)
              .clientAuth(ClientAuth.REQUIRE)
              .sslProvider(SslProvider.OPENSSL)
              .protocols(SSLEngineConfig.TLS_PROTOCOL_VERSION)
              .ciphers(ciphers)
              .build();
    } else {
      ClientAuth clientAuth = ClientAuth.NONE;
      if (config.enabledClientAuthSSL) {
        clientAuth = ClientAuth.REQUIRE;
      }
      sslContext =
          SslContextBuilder.forClient()
              .keyManager(keyManagerFactory)
              .trustManager(trustManagerFactory)
              .clientAuth(clientAuth)
              .sslProvider(SslProvider.OPENSSL)
              .protocols(SSLEngineConfig.TLS_PROTOCOL_VERSION)
              .build();
    }

    SSLEngine sslEngine = sslContext.newEngine(PooledByteBufAllocator.DEFAULT);

    LOGGER.debug(
        "Creating SSLEngine with keystore "
            + config.keystorePath
            + ", truststore="
            + config.truststorePath);
    LOGGER.debug(
        "Creating SSLEngine in Server mode="
            + isServer
            + " and clientAuth="
            + config.enabledClientAuthSSL);

    return sslEngine;
  }

  private KeyManagerFactory createKeyManagerFactory(String filepath) {
    try {
      assertPasswordNotNull("Keystore password cannot be set to null", config.keystorePassword);
      assertPasswordNotNull("Key password cannot be set to null", config.keyPassword);

      KeyStore keyStore = KeyStore.getInstance(config.keystoreType.name());
      try (InputStream keyStoreIS = new FileInputStream(filepath)) {
        keyStore.load(keyStoreIS, config.keystorePassword.toCharArray());
      }
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, config.keyPassword.toCharArray());
      return kmf;
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not create key managers", e);
    }
  }

  private TrustManagerFactory createTrustManagerFactory(String filepath) {
    try {
      assertPasswordNotNull("Truststore password cannot be set to null", config.truststorePassword);
      KeyStore trustStore = KeyStore.getInstance(config.truststoreType.name());
      try (InputStream trustStoreIS = new FileInputStream(filepath)) {
        trustStore.load(trustStoreIS, config.truststorePassword.toCharArray());
      }
      TrustManagerFactory trustFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustFactory.init(trustStore);
      return trustFactory;
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not create trust managers", e);
    }
  }

  private void assertPasswordNotNull(String message, String password) {
    if (password == null) {
      throw new IllegalArgumentException(message);
    }
  }
}
