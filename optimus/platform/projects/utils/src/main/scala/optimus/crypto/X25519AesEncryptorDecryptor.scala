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

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CharsetDecoder
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.PublicKey
import java.security.spec.InvalidKeySpecException
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import java.time.Duration
import java.util
import java.util.Base64

import at.favre.lib.crypto.HKDF
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import optimus.crypto.EncryptorDecryptor.EncryptorDecryptorConfig
import optimus.utils.ZstdUtils
import org.slf4j.LoggerFactory.getLogger

private[crypto] object X25519AesSchemeUtils {
  private[crypto] val log = getLogger(getClass)

  private val AesStringLiteral = "AES".getBytes(StandardCharsets.UTF_8)
  private val IvStringLiteral = "IV".getBytes(StandardCharsets.UTF_8)

  private val PublicKeyHeader = {
    val randomEncodedPublicKey = keyGenerator().generateKeyPair().getPublic.getEncoded
    // we generate random public key, only to extract (constant) header from it
    randomEncodedPublicKey.slice(0, randomEncodedPublicKey.length - 32)
  }

  // The returned objects are expensive to create, not thread-safe, but can be reused
  def aesCbcPkcs5(): Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")

  private[this] val xdhKeyAgreementTl = new ThreadLocal[KeyAgreement] {
    override def initialValue(): KeyAgreement = KeyAgreement.getInstance("XDH")
  }
  def xdhKeyAgreement(): KeyAgreement = xdhKeyAgreementTl.get()

  private[this] val xdhKeyFactoryTl = new ThreadLocal[KeyFactory] {
    override def initialValue(): KeyFactory = KeyFactory.getInstance("XDH")
  }
  def xdhKeyFactory: KeyFactory = xdhKeyFactoryTl.get()

  def loadPrivateKey(keyBytes: Array[Byte]): PrivateKey = {
    if (keyBytes.length != 48) {
      throw new IllegalArgumentException(
        s"PKCS8 file with X25519 private key should be 48 bytes long, but is ${keyBytes.length} bytes long")
    }
    try {
      xdhKeyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyBytes))
    } catch {
      case e: InvalidKeySpecException =>
        // workaround for https://bugs.openjdk.java.net/browse/JDK-8213363 - fixed in JDK12+
        val butchered = new Array[Byte](46)
        System.arraycopy(keyBytes, 0, butchered, 0, 12)
        System.arraycopy(keyBytes, 14, butchered, 12, 34)
        butchered(1) = (butchered(1) - 2).toByte // correct embedded length
        xdhKeyFactory.generatePrivate(new PKCS8EncodedKeySpec(butchered))
    }
  }

  def keyGenerator(): KeyPairGenerator = {
    val keyPairGenerator = KeyPairGenerator.getInstance("XDH")
    keyPairGenerator.initialize(255) // X25519
    keyPairGenerator
  }

  def deriveAesKeyAndIv(
      privateKey: PrivateKey,
      publicKey: PublicKey,
      rawEphemeralPublicKey: Array[Byte]): (SecretKey, IvParameterSpec) = {
    val keyAgreement = xdhKeyAgreement()
    keyAgreement.init(privateKey)
    keyAgreement.doPhase(publicKey, true)
    deriveAesKeyAndIv(keyAgreement.generateSecret(), rawEphemeralPublicKey)
  }

  def deriveAesKeyAndIv(sharedSecret: Array[Byte], rawEphemeralPublicKey: Array[Byte]): (SecretKey, IvParameterSpec) = {
    val nullArray: Array[Byte] = null // to make scala compiler happy
    val hkdf = HKDF.fromHmacSha256()
    val extracted = hkdf.extract(nullArray, sharedSecret ++ rawEphemeralPublicKey)
    val aesKey = hkdf.expand(extracted, AesStringLiteral, 16)
    val aesIv = hkdf.expand(extracted, IvStringLiteral, 16)
    (new SecretKeySpec(aesKey, "AES"), new IvParameterSpec(aesIv))
  }

  def rawPublicKey(publicKey: PublicKey): Array[Byte] = {
    val encoded = publicKey.getEncoded
    encoded.slice(encoded.length - 32, encoded.length)
  }

  def publicKeyFromRaw(rawPublicKey: ByteBuffer): PublicKey = {
    xdhKeyFactory.generatePublic(new X509EncodedKeySpec(PublicKeyHeader ++ rawPublicKey.array()))
  }

  def encryptor(keyBytes: Array[Byte], version: Int): Encryptor = {
    encryptor(xdhKeyFactory.generatePublic(new X509EncodedKeySpec(keyBytes)), version)
  }

  def encryptor(publicKey: PublicKey, version: Int): Encryptor = {
    val ephemeralKeyPair = keyGenerator().generateKeyPair()
    val rawEphemeralPublicKey = rawPublicKey(ephemeralKeyPair.getPublic)
    val (secretKey, ivParameterSpec) = deriveAesKeyAndIv(ephemeralKeyPair.getPrivate, publicKey, rawEphemeralPublicKey)

    log.info(s"Building crumbs encryptor with version $version")
    new X25519AesEncryptor(rawEphemeralPublicKey, secretKey, ivParameterSpec, version)
  }

  def decryptor(keyBytes: Array[Byte]): Decryptor = {
    decryptor(loadPrivateKey(keyBytes))
  }

  def decryptor(privateKey: PrivateKey): Decryptor = {
    new X25519AesDecryptor(new CachingX25519AesKeyCalculator(privateKey))
  }
}

private class X25519AesEncryptor(
    rawEphemeralPublicKey: Array[Byte],
    aesKey: SecretKey,
    aesIV: IvParameterSpec,
    version: Int)
    extends Encryptor {

  private val encryptionConfig = Map(
    1 -> EncryptorDecryptorConfig(compressed = false, encoded = true, new CommaPayloadDeSerializer("1")),
    2 -> EncryptorDecryptorConfig(compressed = true, encoded = false, new ByteArrayPayloadDeSerializer("2")),
    3 -> EncryptorDecryptorConfig(compressed = false, encoded = false, new ByteArrayPayloadDeSerializer("3")),
  )

  private val base64RawEphemeralPublicKey = Base64.getEncoder.encodeToString(rawEphemeralPublicKey)

  private val cipher = X25519AesSchemeUtils.aesCbcPkcs5()
  override def encrypt(text: String): Array[Byte] = {
    val inputBytes = text.getBytes(StandardCharsets.UTF_8)
    val compressed: Array[Byte] = compressPayload(inputBytes, encryptionConfig(version).compressed)
    val encryptedBytes = {
      cipher.init(Cipher.ENCRYPT_MODE, aesKey, aesIV)
      cipher.doFinal(compressed)
    }
    val encoded: Array[Byte] = encodePayload(encryptedBytes, encryptionConfig(version).encoded)
    val publicKey: Array[Byte] =
      if (encryptionConfig(version).encoded) base64RawEphemeralPublicKey.getBytes else rawEphemeralPublicKey

    encryptionConfig(version).payloadDeSerializer.serialize(publicKey, encoded)
  }

  private def compressPayload(inputBytes: Array[Byte], compress: Boolean): Array[Byte] = {
    val compressed = if (compress) {
      ZstdUtils.compress(inputBytes)
    } else {
      inputBytes
    }
    compressed
  }

  private def encodePayload(encryptedBytes: Array[Byte], encode: Boolean): Array[Byte] = {
    val encoded = if (encode) {
      Base64.getEncoder.encode(encryptedBytes)
    } else {
      encryptedBytes
    }
    encoded
  }

}

trait X25519AesKeyCalculator {
  def getAesKeyAndIV(schemeVersion: String, rawEphemeralPublicKey: ByteBuffer): (SecretKey, IvParameterSpec)
}

class SimpleX25519AesKeyCalculator(privateKey: PrivateKey) extends X25519AesKeyCalculator {

  override def getAesKeyAndIV(
      schemeVersion: String,
      rawEphemeralPublicKey: ByteBuffer): (SecretKey, IvParameterSpec) = {

    if (schemeVersion != "1") {
      throw new IllegalArgumentException(s"Unsupported scheme version: ${schemeVersion}")
    }

    val ephemeralPublicKey = X25519AesSchemeUtils.publicKeyFromRaw(rawEphemeralPublicKey)

    X25519AesSchemeUtils.deriveAesKeyAndIv(privateKey, ephemeralPublicKey, rawEphemeralPublicKey.array())
  }
}

class CachingX25519AesKeyCalculator(privateKey: PrivateKey) extends SimpleX25519AesKeyCalculator(privateKey) {

  val keysCache: LoadingCache[ByteBuffer, (SecretKey, IvParameterSpec)] =
    Caffeine
      .newBuilder()
      .maximumSize(50000)
      .expireAfterAccess(Duration.ofHours(4))
      .build(super.getAesKeyAndIV("1", _))

  override def getAesKeyAndIV(
      schemeVersion: String,
      rawEphemeralPublicKey: ByteBuffer): (SecretKey, IvParameterSpec) = {

    if (schemeVersion != "1" && schemeVersion != "2" && schemeVersion != "3") {
      throw new IllegalArgumentException(s"Unsupported scheme version: ${schemeVersion}")
    }

    keysCache.get(rawEphemeralPublicKey)
  }
}

object X25519AesDecryptor {
  private val log = getLogger(getClass)

  private val charsetDecoder = new ThreadLocal[CharsetDecoder] {
    override def initialValue: CharsetDecoder = {
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
    }
  }
}

private class X25519AesDecryptor(keyGenerator: X25519AesKeyCalculator) extends Decryptor {

  import X25519AesDecryptor._

  private val decryptionConfig = Map(
    '1' -> EncryptorDecryptorConfig(compressed = false, encoded = true, new CommaPayloadDeSerializer("1")),
    '2' -> EncryptorDecryptorConfig(compressed = true, encoded = false, new ByteArrayPayloadDeSerializer("2")),
    '3' -> EncryptorDecryptorConfig(compressed = false, encoded = false, new ByteArrayPayloadDeSerializer("3")),
  )

  private val cipher = X25519AesSchemeUtils.aesCbcPkcs5()

  override def decrypt(bytes: Array[Byte]): Array[Byte] = {
    val version = getPayloadVersion(bytes)
    val deserializer = decryptionConfig(version).payloadDeSerializer
    val payload = deserializer.deserialize(bytes)
    val publicKey = decode(payload.key, decryptionConfig(version).encoded)
    val (secretKey, ivSpec) = keyGenerator.getAesKeyAndIV(version.toString, ByteBuffer.wrap(publicKey))

    try {
      val decodedBytes = decode(payload.data, decryptionConfig(version).encoded)
      val decryptedBytes = {
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec)
        cipher.doFinal(decodedBytes)
      }
      //verify that decrypted bytes are valid UTF-8 before sending to kafka
      val decompressedBytes = decompressPayload(decryptedBytes, decryptionConfig(version).compressed)
      charsetDecoder.get.reset()
      charsetDecoder.get.decode(ByteBuffer.wrap(decompressedBytes))
      decompressedBytes
    } catch {
      case e: CharacterCodingException =>
        log.error("Failed to decode message", e)
        logError(payload)
        "CharacterCodingException".getBytes(StandardCharsets.UTF_8)
      case e: IllegalArgumentException =>
        log.error("Failed to decode message", e)
        logError(payload)
        "IllegalArgument".getBytes(StandardCharsets.UTF_8)
      case e: Throwable =>
        log.error("Unexpected exception while decoding/decrypting payload", e)
        logError(payload)
        "UnexpectedException".getBytes(StandardCharsets.UTF_8)
    }
  }

  private def logError(payload: Payload): Unit = {
    log.error("Payload: " + util.Arrays.toString(payload.data))
    log.error("Ephemeral Public Key: " + util.Arrays.toString(payload.key))
  }

  private def getPayloadVersion(payload: Array[Byte]): Char = {
    payload.head.toChar
  }

  private def decode(payload: Array[Byte], decode: Boolean): Array[Byte] = {
    val decodedBytes = if (decode) {
      Base64.getDecoder.decode(payload)
    } else {
      payload
    }
    decodedBytes
  }

  private def decompressPayload(decryptedBytes: Array[Byte], decompress: Boolean): Array[Byte] = {
    if (decompress) {
      ZstdUtils.decompress(decryptedBytes)
    } else {
      decryptedBytes
    }
  }
}

trait PayloadDeSerializer {
  def protocolVersion: Array[Byte]
  def serialize(key: Array[Byte], data: Array[Byte]): Array[Byte]
  def deserialize(array: Array[Byte]): Payload
}

private[crypto] case class Payload(key: Array[Byte], data: Array[Byte]) {}

/**
 * CommaPayloadDeSerializer scheme:
 * array[0] = 0 - encoding version
 * array[1] = comma
 * array[2..M] = public_key
 * array[M+1] = comma
 * array[M+2, N] = payload
 */
private class CommaPayloadDeSerializer(protocol: String) extends PayloadDeSerializer {
  private val separator: Byte = ','.toByte
  private val separatorArray: Array[Byte] = Array(separator)
  override def protocolVersion: Array[Byte] = protocol.getBytes

  override def serialize(key: Array[Byte], data: Array[Byte]): Array[Byte] = {
    protocolVersion ++ separatorArray ++ key ++ separatorArray ++ data
  }

  override def deserialize(array: Array[Byte]): Payload = {
    val versionIdx = array.indexOf(separator, 0)
    val keyIdx = array.indexOf(separator, versionIdx + 1)
    val keySlice = array.slice(versionIdx + 1, keyIdx)
    val payloadSlice = array.slice(keyIdx + 1, array.length)

    Payload(keySlice, payloadSlice)
  }
}

/**
 * ByteArrayPayloadDeSerializer scheme:
 * array[0] = 0 - encoding version
 * array[1] = length_of_public_key
 * array[2..M] = public_key
 * array[M+1, N] = payload
 */
private class ByteArrayPayloadDeSerializer(protocol: String) extends PayloadDeSerializer {

  override def protocolVersion: Array[Byte] = protocol.getBytes

  override def serialize(key: Array[Byte], data: Array[Byte]): Array[Byte] = {
    protocolVersion ++ Array(key.length.toByte) ++ key ++ data
  }

  override def deserialize(array: Array[Byte]): Payload = {
    val keyLength = array(1).toInt
    val keyStartIdx = 2
    val keyEndIdx = 2 + keyLength
    val publicKey = array.slice(keyStartIdx, keyEndIdx)
    val payload = array.slice(keyEndIdx, array.length)

    Payload(publicKey, payload)
  }
}
