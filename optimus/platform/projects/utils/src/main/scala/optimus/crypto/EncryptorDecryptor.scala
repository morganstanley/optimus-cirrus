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
package optimus.crypto

import java.nio.file.Files
import java.nio.file.Paths

import optimus.utils.PropertyUtils
import org.slf4j.LoggerFactory

object EncryptorDecryptor {
  val log = LoggerFactory.getLogger(EncryptorDecryptor.getClass)

  object EncryptionScheme extends Enumeration {
    val AES = Value
    val RSA = Value
    val X25519_AES = Value
  }

  val EncryptionSchemeParam = "optimus.crypto.encryption_scheme"
  val AsymmetricEncryptionEnabledParam = "optimus.crypto.enable_asymmetric_encryption"
  val AsymmetricEncryptionMessageSizeParam = "optimus.crypto.asymmetric_enc_max_message_size"
  val KeyPathParam = "optimus.crypto.key_path"

  val SymmetricAlgorithmName: String = "AES"
  val AsymmetricAlgorithmName: String = "RSA"

  def buildEncryptor(version: Int): Encryptor = {
    build(true, version).asInstanceOf[Encryptor]
  }

  def buildDecryptor(): Decryptor = {
    build(false, 1).asInstanceOf[Decryptor]
  }

  private def build(encryptMode: Boolean, version: Int): EncryptorDecryptor = {

    val scheme = {
      sys.props.get(EncryptionSchemeParam) match {
        case Some(schemeString) =>
          EncryptionScheme.withName(schemeString.toUpperCase)
        case None =>
          if (PropertyUtils.flag(AsymmetricEncryptionEnabledParam))
            EncryptionScheme.RSA
          else
            EncryptionScheme.AES
      }
    }

    val keyFile = System.getProperty(KeyPathParam)
    val keyBytes = Files.readAllBytes(Paths.get(keyFile))

    log.info(
      s"Building crumbs payload EncryptorDecryptor in encryptMode=$encryptMode with scheme=$scheme, keyFile=$keyFile")

    scheme match {
      case EncryptionScheme.AES =>
        new SymmetricEncryptorDecryptorBuilder(keyBytes).build(encryptMode)
      case EncryptionScheme.RSA =>
        val maxMessageSize = Integer.getInteger(AsymmetricEncryptionMessageSizeParam, -1)
        new AsymmetricEncryptionProviderBuilder(keyBytes, maxMessageSize).build(encryptMode)
      case EncryptionScheme.X25519_AES =>
        if (encryptMode) {
          X25519AesSchemeUtils.encryptor(keyBytes, version)
        } else {
          X25519AesSchemeUtils.decryptor(keyBytes)
        }
    }
  }

  private[crypto] case class EncryptorDecryptorConfig(
      compressed: Boolean = false,
      encoded: Boolean = false,
      payloadDeSerializer: PayloadDeSerializer) {}
}

private[crypto] trait EncryptorDecryptor {}

trait Encryptor extends EncryptorDecryptor {
  def encrypt(text: String): Array[Byte]
}

trait Decryptor extends EncryptorDecryptor {
  def decrypt(bytes: Array[Byte]): Array[Byte]
}

object NoopEncryptor extends Encryptor {
  def encryptAndEncode(text: String): String = text
  override def encrypt(text: String): Array[Byte] = text.getBytes
}

object NoopDecryptor extends Decryptor {
  override def decrypt(bytes: Array[Byte]): Array[Byte] = bytes
}
