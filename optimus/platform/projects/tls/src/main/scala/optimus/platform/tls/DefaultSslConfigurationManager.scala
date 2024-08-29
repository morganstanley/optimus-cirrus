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
package optimus.platform.tls

import optimus.platform.tls.config.StaticConfig
import optimus.platform.tls.simple.KeyStoreReader
import optimus.platform.tls.simple.KeyStoreReaderProvider
import optimus.platform.tls.simple.SimpleSslConfigurationManager
import optimus.platform.tls.simple.TlsFeatureSet

import java.lang.{Boolean => JBoolean}

class DefaultSslConfigurationManager private (keyStoreReader: KeyStoreReader, slicingBuffers: Boolean)
    extends SimpleSslConfigurationManager(
      TlsFeatureSet(Some(Seq(StaticConfig.string("TLS_VERSION"))), Some(Seq(StaticConfig.string("CIPHER_SUITE")))),
      keyStoreReader,
      slicingBuffers
    )
object DefaultSslConfigurationManager {
  import SslConfigurationManager.Configuration

  val javaOptionPrefixForSystemConfiguration: String = "optimus.dal.tls."

  def apply(configuration: Configuration): DefaultSslConfigurationManager = {
    new DefaultSslConfigurationManager(
      KeyStoreReaderProvider
        .resolve((key: String) => configuration.get(key))
        .getOrElse(throw new IllegalArgumentException("No KeyStoreReader is resolved")),
      configuration.get("fix.slicing_buffers") match {
        case Some(value: JBoolean) => value: Boolean
        case Some(value: String)   => value.toBoolean
        case None                  => true
        case _                     => throw new IllegalArgumentException
      }
    )
  }

  def systemConfiguration(): Configuration =
    systemConfiguration(System.getProperty)

  // Properties:
  //   optimus.dal.tls.usage
  //   optimus.dal.tls.fix.slicing_buffers
  //   optimus.dal.tls.clm.usages
  //   optimus.dal.tls.clm.id
  //   optimus.dal.tls.clm.scv.backoff_initial_value
  //   optimus.dal.tls.clm.scv.backoff_factor
  //   optimus.dal.tls.clm.scv.backoff_bound
  //   optimus.dal.tls.clm.scv.retry_limit
  //   optimus.dal.tls.clm.scv.retry_for_client_errors
  //   optimus.dal.tls.platform.usages
  //   optimus.dal.tls.platform.selector.defaults
  //   optimus.dal.tls.platform.selector.principals
  //   optimus.dal.tls.platform.selector.principal.*
  //   optimus.dal.tls.pkcs12file.usages
  //   optimus.dal.tls.pkcs12file.path
  //   optimus.dal.tls.pkcs12file.askpass
  def systemConfiguration(get: Function[String, String]): Configuration = {
    val prefix = javaOptionPrefixForSystemConfiguration

    { (key: String) =>
      Option(get(s"$prefix$key"))
    }
  }
}
