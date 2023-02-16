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
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;

/**
 * Client is always supposed to validate server. Server only if client auth is enabled. Otherwise it
 * will not get client's certificate.
 */
class CertificateExpirationValidator {

  static boolean validate(SSLEngine engine, Certificate[] certs)
      throws CertificateNotYetValidException, CertificateExpiredException {
    if (engine.getUseClientMode() || engine.getNeedClientAuth()) {
      for (Certificate cert : certs) {
        if (cert instanceof X509Certificate) ((X509Certificate) cert).checkValidity();
        else
          throw new CertificateNotYetValidException(
              "Unsupported certificate: " + cert.getType() + " " + cert.toString());
      }
    }
    return true;
  }
}
