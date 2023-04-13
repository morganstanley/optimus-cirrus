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
package com.ms.silverking.cloud.dht.client.crypto;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

/**
 * AES EncrypterDecrypter
 */
public class AESEncrypterDecrypter implements EncrypterDecrypter {
  private final SecretKey secretKey;
  private final IvParameterSpec iv;

  public static final String name = "AES";

  private static final int saltLength = 8;

  private static SecretKey generateKey() throws NoSuchAlgorithmException {
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(128);
    SecretKey key = keyGenerator.generateKey();
    return key;
  }

  public AESEncrypterDecrypter() {
    try {
      SecureRandom secureRandom;
      secureRandom = new SecureRandom();
      iv = new IvParameterSpec(secureRandom.generateSeed(16));
      secretKey = generateKey();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Cipher getCipher(int mode) {
    if (secretKey == null) {
      throw new RuntimeException("No key defined");
    } else {
      try {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(mode, secretKey, iv);
        return cipher;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public byte[] encrypt(byte[] plainText) {
    byte[] cipherText;
    Cipher cipher = getCipher(Cipher.ENCRYPT_MODE);
    try {
      cipherText = cipher.doFinal(plainText);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return cipherText;
  }

  @Override
  public byte[] decrypt(byte[] cipherTextWithLength, int offset, int length) {
    byte[] plainText;
    Cipher cipher = getCipher(Cipher.DECRYPT_MODE);
    try {
      plainText = cipher.doFinal(cipherTextWithLength, offset, length);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return plainText;
  }

  @Override
  public int hashCode() {
    return secretKey.hashCode() ^ iv.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    AESEncrypterDecrypter other = (AESEncrypterDecrypter) o;
    return secretKey.equals(other.secretKey) && iv.equals(other.iv);
  }

  @Override
  public String toString() {
    return "[Name: " + name + ", Salt length: " + saltLength + ", secretKey: " + " , iv: " + "]";
  }
}
