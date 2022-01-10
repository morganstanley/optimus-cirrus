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
import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec

import optimus.crypto.EncryptorDecryptor.AsymmetricAlgorithmName

class AsymmetricEncryptionProviderBuilder(keyBytes: Array[Byte], messageSize: Int) extends EncryptionProviderBuilder {

  val kf: KeyFactory = KeyFactory.getInstance(AsymmetricAlgorithmName)

  override def cipherName: String = AsymmetricAlgorithmName
  override def decryptionKey: Key = {
    val spec = new PKCS8EncodedKeySpec(keyBytes)
    kf.generatePrivate(spec)
  }

  override def encryptionKey: Key = {
    val spec = new X509EncodedKeySpec(keyBytes)
    kf.generatePublic(spec)
  }
  override def maxMessageSize: Int = messageSize
}
