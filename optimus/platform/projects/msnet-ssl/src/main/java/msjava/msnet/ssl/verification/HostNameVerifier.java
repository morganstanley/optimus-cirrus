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

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Compares provided hostname with SAN or CN names from the certificates. */
class HostNameVerifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(CertificateVerifier.class);

  static boolean verifyHostname(String hostName, Certificate[] certs) throws CertificateException {
    Set<String> allowedHostNames = getAllowedServerNames(convertToJavaX509Certificates(certs));
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Comparing received hostname {} with certificate host names {}",
          hostName,
          allowedHostNames);
    }

    boolean hostNameMatches =
        allowedHostNames.contains(hostName)
            || (!hostName.endsWith(".") && allowedHostNames.contains(hostName + "."));
    if (!hostNameMatches) {
      LOGGER.warn("The host name '{}' is not a legit host name of a server.", hostName);
    }

    return hostNameMatches;
  }

  public static X509Certificate[] convertToJavaX509Certificates(Certificate[] certs)
      throws CertificateException {
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    X509Certificate[] convertedCerts = new X509Certificate[certs.length];
    for (int i = 0; i < certs.length; i++) {
      convertedCerts[i] = convertCert(certFactory, certs[i]);
    }
    return convertedCerts;
  }

  private static X509Certificate convertCert(
      CertificateFactory certificateFactory, Certificate cert) throws CertificateException {
    try {
      ByteArrayInputStream stream = new ByteArrayInputStream(cert.getEncoded());
      return (X509Certificate) certificateFactory.generateCertificate(stream);
    } catch (CertificateException e) {
      throw new CertificateException("Could not convert cert to X509Certificate.", e);
    }
  }

  private static Set<String> getAllowedServerNames(X509Certificate[] certs)
      throws CertificateParsingException {
    Set<String> allowedSubjectAlternativeNames = new HashSet<>();
    for (X509Certificate cert : certs) {
      allowedSubjectAlternativeNames.addAll(getSubjectAlternativeNames(cert));
      getCommonName(cert).ifPresent(allowedSubjectAlternativeNames::add);
    }
    return allowedSubjectAlternativeNames;
  }

  private static Optional<String> getCommonName(X509Certificate cert) {
    /*
     * The original code below contain `sun.security.x509.X509Name`, which can't be used as import for Java 11+.
     * Packages sun.* hold internal stuff, and should not be used by thirdparty applications in general case.
     * Since java 9 the module system has been introduced, java 9+ would "protect" these packages even in compile time.
     *
     * try {
     *     String commonName = new X500Name(cert.getSubjectX500Principal().getName()).getCommonName();
     *     return Optional.of(commonName);
     * } catch (IOException e) {
     * // if anything happen when extra the common name from certs, we just ignore that
     * }
     *
     * Therefore, alternative solution by 'LdapName' should be used to get common name in roughly RFC 1779 DN
     * dn = "CN=commonName, OU=organizationUnit, ON=organizationName, LN=localityName, SN=stateName, ....";
     */
    try {
      String dn = cert.getSubjectX500Principal().getName();
      LdapName ln = new LdapName(dn);
      String commonName = "";
      for (Rdn rdn : ln.getRdns()) {
        if (rdn.getType().equalsIgnoreCase("CN")) {
          commonName = rdn.getValue().toString();
          break;
        }
      }
      return Optional.of(commonName);
    } catch (InvalidNameException e) {
      // if anything happen when extra the common name from certs, we just ignore that
    }

    return Optional.empty();
  }

  private static List<String> getSubjectAlternativeNames(X509Certificate cert)
      throws CertificateParsingException {
    // this list contains host names but also some unexpected numbers
    Collection<List<?>> entries =
        Optional.ofNullable(cert.getSubjectAlternativeNames()).orElse(Collections.emptyList());

    return entries.stream()
        .flatMap(Collection::stream)
        .filter(entry -> entry instanceof String)
        .map(entry -> (String) entry)
        .collect(Collectors.toList());
  }
}
