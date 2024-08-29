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

import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslProvider

import java.security.KeyStore
import java.util
import java.util.concurrent.locks.ReentrantLock
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import scala.jdk.CollectionConverters._
import msjava.slf4jutils.scalalog.getLogger

abstract class SslConfigurationManager private[tls] () {
  import SslConfigurationManager._

  private val keyStoreLock = new ReentrantLock()
  @volatile private var maybeKeyStore: Option[(KeyStore, Option[Array[Char]])] = None

  private val openSslEngineFactoryLock = new ReentrantLock()
  @volatile private var maybeOpenSslEngineFactory: Option[((KeyStore, Option[Array[Char]]), OpenSslEngineFactory)] =
    None

  protected def protocols: Option[Seq[String]]
  protected def ciphers: Option[Seq[String]]

  protected def readKeyStore(): (KeyStore, Option[Array[Char]])

  private def loadKeyStore(forced: Boolean = false) = {
    keyStoreLock.lock()
    try {
      maybeKeyStore match {
        case Some(keyStore) if !forced => keyStore
        case _ =>
          val keyStore = readKeyStore()
          this.maybeKeyStore = Some(keyStore)
          keyStore
      }
    } finally keyStoreLock.unlock()
  }

  private def keyStore: (KeyStore, Option[Array[Char]]) = {
    maybeKeyStore match {
      case Some(keyStore) => keyStore
      case None =>
        loadKeyStore()
    }
  }

  private def makeKeyManagerFactory(keyStore: (KeyStore, Option[Array[Char]])): KeyManagerFactory = {
    val factory: KeyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyStore match {
      case (keyStore, maybePassphrase) =>
        factory.init(keyStore, maybePassphrase.map(p => util.Arrays.copyOf(p, p.length)).orNull)
    }
    factory
  }

  private def makeTrustManagerFactory(keyStore: (KeyStore, Option[Array[Char]])): TrustManagerFactory = {
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    keyStore match {
      case (keyStore, _) =>
        factory.init(keyStore)
    }
    factory
  }

  private def makeOpenSslEngineFactory(keyStore: (KeyStore, Option[Array[Char]])): OpenSslEngineFactory = {
    val keyManagerFactory = makeKeyManagerFactory(keyStore)
    val trustManagerFactory = makeTrustManagerFactory(keyStore)

    val serverContext = SslContextBuilder
      .forServer(keyManagerFactory)
      .trustManager(trustManagerFactory)
      .sslProvider(SslProvider.OPENSSL_REFCNT)
      .protocols(protocols.map(_.asJava).orNull)
      .ciphers(ciphers.map(_.asJava).orNull)
      .clientAuth(ClientAuth.OPTIONAL)
      .build()

    val clientContext = SslContextBuilder
      .forClient()
      .keyManager(keyManagerFactory)
      .trustManager(trustManagerFactory)
      .sslProvider(SslProvider.OPENSSL_REFCNT)
      .protocols(protocols.map(_.asJava).orNull)
      .ciphers(ciphers.map(_.asJava).orNull)
      .build()

    new OpenSslEngineFactory(serverContext, clientContext, keyStore)
  }

  def reload(): Unit = {
    loadKeyStore(forced = true)
  }

  def openSslEngineFactory: OpenSslEngineFactory = {
    def recentOpenSslEngineFactory =
      maybeOpenSslEngineFactory.collect {
        case (keyStore, openSslEngineFactory) if maybeKeyStore.exists(_ eq keyStore) =>
          openSslEngineFactory
      }

    recentOpenSslEngineFactory.getOrElse {
      openSslEngineFactoryLock.lock()
      try {
        recentOpenSslEngineFactory.getOrElse {
          try {
            val openSslEngineFactory = makeOpenSslEngineFactory(keyStore)
            maybeOpenSslEngineFactory = Some(keyStore -> openSslEngineFactory)
            openSslEngineFactory
          } catch {
            case exception: Exception =>
              maybeOpenSslEngineFactory match {
                case Some((_, openSslEngineFactory)) =>
                  logger.error(
                    "Failed to create OpenSslEngineFactory after the key store is reloaded; keeping using the old one.",
                    exception)
                  openSslEngineFactory
                case None => throw exception
              }
          }
        }
      } finally openSslEngineFactoryLock.unlock()
    }
  }

  def slicingBuffers: Boolean
}
object SslConfigurationManager {
  trait Configuration {
    def get(key: String): Option[AnyRef]
    final def ++(properties: Map[String, AnyRef]): Configuration = { (key: String) =>
      properties.get(key).orElse(get(key))
    }
  }

  def apply(configuration: Configuration): SslConfigurationManager = DefaultSslConfigurationManager(configuration)
  def systemConfiguration(): Configuration =
    DefaultSslConfigurationManager.systemConfiguration()
  def systemConfiguration(get: Function[String, String]): Configuration =
    DefaultSslConfigurationManager.systemConfiguration(get)
  def clientConfiguration(base: Configuration): Configuration =
    base ++ Map("usage" -> "client")
  def serverConfiguration(base: Configuration): Configuration =
    base ++ Map("usage" -> "server")

  private val logger = getLogger[SslConfigurationManager]
}
