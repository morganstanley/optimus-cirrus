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
package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.crypto.HashFunctionChecksum;
import com.ms.silverking.cloud.dht.crypto.MD5Checksum;
import com.ms.silverking.cloud.dht.crypto.NullChecksum;

public class ChecksumProvider {
  private static final Checksum md5Checksum = new MD5Checksum();
  private static final Checksum nullChecksum = new NullChecksum();
  private static final Checksum murmur3_32Checksum =
      new HashFunctionChecksum(HashFunctionChecksum.Type.Murmur3_32);
  private static final Checksum murmur3_128Checksum =
      new HashFunctionChecksum(HashFunctionChecksum.Type.Murmur3_128);
  private static final Checksum systemChecksum = new SystemChecksum();

  public static Checksum getChecksum(ChecksumType checksumType) {
    switch (checksumType) {
      case MD5:
        return md5Checksum;
      case NONE:
        return nullChecksum;
      case MURMUR3_32:
        return murmur3_32Checksum;
      case MURMUR3_128:
        return murmur3_128Checksum;
      case SYSTEM:
        return systemChecksum;
      default:
        throw new RuntimeException("No provider for: " + checksumType);
    }
  }
}
