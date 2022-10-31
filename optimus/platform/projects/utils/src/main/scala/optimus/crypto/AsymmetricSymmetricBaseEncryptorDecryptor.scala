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

import java.security.Key
import java.util.Base64

import com.google.common.base.Splitter
import javax.crypto.Cipher

import scala.jdk.CollectionConverters._

trait EncryptionProviderBuilder {
  def cipherName: String
  def decryptionKey: Key
  def encryptionKey: Key
  def maxMessageSize: Int

  def buildEncryptor(): Encryptor = {
    build(true).asInstanceOf[Encryptor]
  }

  def buildDecryptor(): Decryptor = {
    build(false).asInstanceOf[Decryptor]
  }

  private[crypto] def build(encryptMode: Boolean): EncryptorDecryptor = {
    if (encryptMode) {
      val key = encryptionKey
      new EncryptorImpl(key, cipherName, Cipher.ENCRYPT_MODE, maxMessageSize)
    } else {
      val key = decryptionKey
      new DecryptorImpl(key, cipherName, Cipher.DECRYPT_MODE)
    }
  }
}

private class EncryptorImpl(key: Key, cipherName: String, mode: Int, maxMessageSize: Int) extends Encryptor {
  val cipher: Cipher = Cipher.getInstance(cipherName)
  cipher.init(mode, key)

  def encryptAndEncode(text: String): String = {

    val splitText = Splitter.fixedLength(maxMessageSize).split(text).asScala

    splitText
      .map { chunk =>
        cipher.synchronized {
          val encrypted = cipher.doFinal(chunk.getBytes())
          Base64.getEncoder.encodeToString(encrypted)
        }
      }
      .mkString(",")
  }
  override def encrypt(text: String): Array[Byte] = encryptAndEncode(text).getBytes
}

private class DecryptorImpl(key: Key, cipherName: String, mode: Int) extends Decryptor {
  import EncryptorDecryptor._
  val cipher: Cipher = Cipher.getInstance(cipherName)
  cipher.init(mode, key)

  def decodeAndDecrypt(text: String): String = {

    if (log.isTraceEnabled()) {
      log.trace(s"Text to decrypt length: ${text.length} and content: $text")
    }

    text
      .split(",")
      .map { chunk =>
        val decoded = Base64.getDecoder.decode(chunk)
        cipher.synchronized {
          new String(cipher.doFinal(decoded))
        }
      }
      .mkString
  }
  override def decrypt(bytes: Array[Byte]): Array[Byte] =
    decodeAndDecrypt(new String(bytes)).getBytes
}
