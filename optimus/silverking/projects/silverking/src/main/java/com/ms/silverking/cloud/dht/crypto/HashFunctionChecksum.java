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
package com.ms.silverking.cloud.dht.crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.io.util.BufferUtil;

public class HashFunctionChecksum implements Checksum {
  private final HashFunction hashFunction;
  private final int bytes;
  private final byte[] emptyChecksum;

  public enum Type {
    Murmur3_32,
    Murmur3_128, /*Adler32*/
  };

  public HashFunctionChecksum(Type type) {
    switch (type) {
      case Murmur3_32:
        hashFunction = Hashing.murmur3_32_fixed();
        break;
      case Murmur3_128:
        hashFunction = Hashing.murmur3_128();
        break;
        /*
        case Adler32:
            hashFunction = Hashing.adler32();
            break;
            */
      default:
        throw new RuntimeException("panic");
    }
    bytes = hashFunction.bits() / 8;
    emptyChecksum = new byte[bytes];
  }

  @Override
  public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
    dest.put(checksum(source, position, length));
  }

  @Override
  public byte[] checksum(byte[] source, int position, int length) {
    HashCode hashCode;

    hashCode = hashFunction.hashBytes(source, position, length);
    return hashCode.asBytes();
  }

  @Override
  public void checksum(ByteBuffer source, ByteBuffer dest) {
    dest.put(checksum(source));
  }

  @Override
  public byte[] checksum(ByteBuffer source) {
    HashCode hashCode;

    if (source.hasArray()) {
      hashCode = hashFunction.hashBytes(source.array(), source.position(), source.remaining());
    } else {
      byte[] tmp;

      tmp = new byte[source.remaining()];
      BufferUtil.get(source, source.position(), tmp, tmp.length);
      hashCode = hashFunction.hashBytes(tmp);
    }
    return hashCode.asBytes();
  }

  @Override
  public void emptyChecksum(ByteBuffer dest) {
    dest.put(emptyChecksum);
  }

  @Override
  public boolean isEmpty(byte[] checksum) {
    return Arrays.equals(checksum, emptyChecksum);
  }

  @Override
  public boolean uniquelyIdentifiesValues() {
    return false;
  }
}
