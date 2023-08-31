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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.ms.silverking.io.FileUtil;

/** Trivial EncrypterDecrypter. This implementation provides basic obfuscation only. */
public class XOREncrypterDecrypter implements EncrypterDecrypter {
  private final byte[] key;

  public static final String name = "xor";

  public XOREncrypterDecrypter(byte[] key) {
    this.key = key;
  }

  public XOREncrypterDecrypter(File file) throws IOException {
    this(FileUtil.readFileAsBytes(file));
  }

  public XOREncrypterDecrypter() throws IOException {
    this(Util.getBytesFromKeyFile());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public byte[] encrypt(byte[] plainText) {
    byte[] cipherText;

    cipherText = new byte[plainText.length];
    for (int i = 0; i < plainText.length; i++) {
      cipherText[i] = (byte) (plainText[i] ^ key[i % key.length]);
    }
    return cipherText;
  }

  @Override
  public byte[] decrypt(byte[] cipherText, int offset, int length) {
    byte[] plainText = new byte[length];
    for (int i = 0; i < length; i++) {
      plainText[i] = (byte) (cipherText[offset + i] ^ key[i % key.length]);
    }
    return plainText;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    XOREncrypterDecrypter other = (XOREncrypterDecrypter) o;
    return Arrays.equals(key, other.key);
  }
}
