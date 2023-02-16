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

package msjava.msnet.ssl.verification;

import java.security.cert.Certificate;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import msjava.base.slf4j.ContextLogger;
import msjava.base.util.internal.SystemPropertyUtils;

/**
 * Certificate verifier checks two things: 1. if peer certificate is not expired 2. checks whether
 * DNS name in the peer certificate matches with the one we are connected to.
 *
 * <p>While client always performs step 1 server performs step 1 only when client auth is enabled.
 *
 * <p>Step 2 is performed when msjava.msnet.ssl.verification.enabledHostnameVerification is set tu
 * true
 */
public class CertificateVerifier {

  private static final String VERIFY_HOSTNAME_PARAMETER = "msjava.msnet.ssl.verify_hostnames";
  private static final boolean DEFAULT_VERIFY_HOSTNAMES =
      SystemPropertyUtils.getBoolean(VERIFY_HOSTNAME_PARAMETER, true, ContextLogger.safeLogger());

  private static FallbackHostNameVerifier fallbackHostNameVerifier = (hostName, certs) -> false;

  private final boolean verifyHostnames;
  private final SSLEngine sslEngine;

  public CertificateVerifier(SSLEngine sslEngine) {
    this.verifyHostnames = DEFAULT_VERIFY_HOSTNAMES;
    this.sslEngine = sslEngine;
  }

  CertificateVerifier(boolean verifyHostnames, SSLEngine sslEngine) {
    this.verifyHostnames = verifyHostnames;
    this.sslEngine = sslEngine;
  }

  public static void setFallbackHostNameVerifier(
      FallbackHostNameVerifier fallbackHostNameVerifier) {
    CertificateVerifier.fallbackHostNameVerifier = fallbackHostNameVerifier;
  }

  public boolean verify(String hostName, boolean encryptionOnly)
      throws SSLException, javax.security.cert.CertificateException, CertificateException {
    if (isServerWithClientAuthDisabled(encryptionOnly)) {
      return true;
    }
    Certificate[] certs = sslEngine.getSession().getPeerCertificates();

    boolean expirationValidationResult = CertificateExpirationValidator.validate(sslEngine, certs);
    boolean validateHostnameResult =
        validateHostname(hostName, certs, !sslEngine.getUseClientMode());

    return expirationValidationResult && validateHostnameResult;
  }

  private boolean isServerWithClientAuthDisabled(boolean encryptionOnly) {
    boolean isServer = !sslEngine.getUseClientMode();
    boolean clientAuthDisabled = !sslEngine.getNeedClientAuth();

    return isServer && (clientAuthDisabled || encryptionOnly);
  }

  private boolean validateHostname(String hostName, Certificate[] certs, boolean isServer)
      throws CertificateException {
    if (!verifyHostnames) {
      return true;
    }

    if (isServer) {
      return true;
    }

    if (!HostNameVerifier.verifyHostname(hostName, certs)) {
      return fallbackHostNameVerifier.validatePeerCertificate(hostName, certs);
    }

    return true;
  }
}
