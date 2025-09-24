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

import static msjava.base.util.internal.SystemPropertyUtils.getBoolean;
import static msjava.base.util.internal.SystemPropertyUtils.getProperty;
import java.util.Objects;
import msjava.base.slf4j.ContextLogger;
import org.slf4j.Logger;

/**
 * Config for SSLEngine. Default values are obtained from vm options. Can override any of them using
 * corresponding methods. Protocol version used in SSL Engine: TLSv1.2
 *
 * <p>For general library overview and code examples refer to the {@link SSLEstablisher}
 * documentation.
 */
public final class SSLEngineConfig {
  private static final Logger LOGGER = ContextLogger.safeLogger();

  public static final String TLS_PROTOCOL_VERSION = "TLSv1.2";

  public static final String KEY_STORE_TYPE_PARAMETER = "msjava.msnet.ssl.keystore_type";
  public static final String KEY_STORE_PATH_PARAMETER = "msjava.msnet.ssl.keystore";
  public static final String CLIENT_KEY_STORE_PATH_PARAMETER = "msjava.msnet.ssl.keystore_client";
  public static final String SERVER_KEY_STORE_PATH_PARAMETER = "msjava.msnet.ssl.keystore_server";
  public static final String TRUST_STORE_TYPE_PARAMETER = "msjava.msnet.ssl.truststore_type";
  public static final String TRUST_STORE_PATH_PARAMETER = "msjava.msnet.ssl.truststore";
  public static final String KEY_STORE_PASSWORD_PARAMETER = "msjava.msnet.ssl.keystore_password";
  public static final String KEY_PASSWORD_PARAMETER = "msjava.msnet.ssl.key_password";
  public static final String TRUST_STORE_PASSWORD_PARAMETER =
      "msjava.msnet.ssl.truststore_password";
  public static final String ENABLED_CLIENT_AUTH_PARAMETER = "msjava.msnet.ssl.enabled_client_auth";

  public SSLStoreType keystoreType =
      SSLStoreType.valueOf(getProperty(KEY_STORE_TYPE_PARAMETER, SSLStoreType.JKS.name(), LOGGER));
  public SSLStoreType truststoreType =
      SSLStoreType.valueOf(
          getProperty(TRUST_STORE_TYPE_PARAMETER, SSLStoreType.JKS.name(), LOGGER));
  public String keystorePassword = getProperty(KEY_STORE_PASSWORD_PARAMETER, LOGGER);
  public String keyPassword = getProperty(KEY_PASSWORD_PARAMETER, LOGGER);
  public String truststorePassword = getProperty(TRUST_STORE_PASSWORD_PARAMETER, LOGGER);

  private String keystorePath = getProperty(KEY_STORE_PATH_PARAMETER, LOGGER);
  public String clientKeystorePath =
      getProperty(CLIENT_KEY_STORE_PATH_PARAMETER, keystorePath, LOGGER);
  public String serverKeystorePath =
      getProperty(SERVER_KEY_STORE_PATH_PARAMETER, keystorePath, LOGGER);

  public String truststorePath = getProperty(TRUST_STORE_PATH_PARAMETER, LOGGER);

  boolean enabledClientAuthSSL = getBoolean(ENABLED_CLIENT_AUTH_PARAMETER, true, LOGGER);

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.keystore_type
   *
   * @param keystoreType
   */
  public SSLEngineConfig withKeystoreType(SSLStoreType keystoreType) {
    this.keystoreType = keystoreType;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.truststore_type
   *
   * @param truststoreType
   */
  public SSLEngineConfig withTruststoreType(SSLStoreType truststoreType) {
    this.truststoreType = truststoreType;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.keystore_password
   *
   * @param keystorePassword
   */
  public SSLEngineConfig withKeystorePassword(String keystorePassword) {
    this.keystorePassword = keystorePassword;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.truststore
   *
   * @param trustStorePassword
   */
  public SSLEngineConfig withTruststorePassword(String trustStorePassword) {
    this.truststorePassword = trustStorePassword;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.key_password
   *
   * @param keyPassword
   */
  public SSLEngineConfig withKeyPassword(String keyPassword) {
    this.keyPassword = keyPassword;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.enabled_client_auth
   *
   * @param enabledClientAuthSSL
   */
  public SSLEngineConfig withClientAuthEnabled(boolean enabledClientAuthSSL) {
    this.enabledClientAuthSSL = enabledClientAuthSSL;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.keystore
   *
   * @param keystorePath
   */
  public SSLEngineConfig withKeystorePath(String keystorePath) {
    this.keystorePath = keystorePath;
    this.clientKeystorePath = keystorePath;
    this.serverKeystorePath = keystorePath;
    return this;
  }

  /**
   * Overrides value obtained from vm arg: msjava.msnet.ssl.truststore
   *
   * @param truststorePath
   */
  public SSLEngineConfig withTruststorePath(String truststorePath) {
    this.truststorePath = truststorePath;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SSLEngineConfig that = (SSLEngineConfig) o;

    return enabledClientAuthSSL == that.enabledClientAuthSSL
        && Objects.equals(keystoreType, that.keystoreType)
        && Objects.equals(truststoreType, that.truststoreType)
        && Objects.equals(keystorePassword, that.keystorePassword)
        && Objects.equals(keyPassword, that.keyPassword)
        && Objects.equals(truststorePassword, that.truststorePassword)
        && Objects.equals(keystorePath, that.keystorePath)
        && Objects.equals(clientKeystorePath, that.clientKeystorePath)
        && Objects.equals(serverKeystorePath, that.serverKeystorePath)
        && Objects.equals(truststorePath, that.truststorePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keystoreType,
        truststoreType,
        keystorePassword,
        keyPassword,
        truststorePassword,
        keystorePath,
        clientKeystorePath,
        serverKeystorePath,
        truststorePath,
        enabledClientAuthSSL);
  }
}
