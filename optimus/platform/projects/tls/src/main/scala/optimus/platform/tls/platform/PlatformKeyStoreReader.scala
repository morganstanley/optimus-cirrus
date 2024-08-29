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
package optimus.platform.tls.platform

import optimus.platform.tls.config.StaticConfig
import optimus.platform.tls.simple.KeyStoreReader

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import javax.naming.ldap.LdapName
import scala.jdk.CollectionConverters._

private class PlatformKeyStoreReader(private val selector: Option[Set[LdapName]]) extends KeyStoreReader {
  require(selector.forall(_.nonEmpty))

  override def read(): (KeyStore, Option[Array[Char]]) = {
    val keyStore = Some(
      Seq(
        StaticConfig.string("TLS_CA_BUNDLE_PEM_FILE"),
        StaticConfig.string("TLS_CA_BUNDLE_CRT_FILE"),
        StaticConfig.string("SSL_CA_CERT_CRT_FILE")
      ).map(new File(_)).filter(_.isFile))
      .filter(_.nonEmpty)
      .map { caFiles =>
        val certificateFactory = CertificateFactory.getInstance("X.509")
        val keyStore = KeyStore.getInstance("JKS")
        keyStore.load(null, null)

        val certificates = caFiles.flatMap { caFile =>
          val in = new BufferedInputStream(new FileInputStream(caFile))
          var closed = false
          try {
            require(in.markSupported())
            Iterator
              .continually {
                in.mark(1)
                if (in.read() == -1) {
                  in.reset()
                  None
                } else {
                  in.reset()
                  Option(certificateFactory.generateCertificate(in))
                }
              }
              .takeWhile(_.nonEmpty)
              .flatten
              .filter { certificate =>
                selector.forall { selection =>
                  certificate match {
                    case certificate: X509Certificate =>
                      selection.contains(new LdapName(certificate.getSubjectX500Principal.getName))
                    case _ => false
                  }
                }
              }
              .toList
          } catch {
            case throwable: Throwable =>
              closed = true
              try in.close()
              catch { case exception: Exception => throwable.addSuppressed(exception) }
              throw throwable
          } finally if (!closed) in.close()
        }

        certificates.zipWithIndex
          .foreach { case (certificate, i) =>
            keyStore.setCertificateEntry(s"ca-cert-$i", certificate)
          }

        keyStore
      }
      .getOrElse {
        val keyStore =
          try KeyStore.getInstance("WINDOWS-ROOT")
          catch { case _: KeyStoreException => KeyStore.getInstance(KeyStore.getDefaultType) }
        keyStore.load(null, null)

        selector match {
          case Some(selection) =>
            val filteredKeyStore = KeyStore.getInstance("JKS")
            filteredKeyStore.load(null, null)

            keyStore.aliases().asIterator().asScala.foreach { alias =>
              keyStore.getCertificate(alias) match {
                case certificate: X509Certificate
                    if selection.contains(new LdapName(certificate.getSubjectX500Principal.getName)) =>
                  filteredKeyStore.setCertificateEntry(alias, certificate)
                case _ =>
              }
            }

            filteredKeyStore
          case None => keyStore
        }
      }

    (keyStore, None)
  }
}
private object PlatformKeyStoreReader {
  def apply(selection: Option[Set[LdapName]]): PlatformKeyStoreReader = new PlatformKeyStoreReader(selection)
}
