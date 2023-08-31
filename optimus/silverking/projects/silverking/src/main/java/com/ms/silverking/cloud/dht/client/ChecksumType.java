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
package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.impl.SystemChecksum;

/** Checksum to use for DHT values. */
public enum ChecksumType {
  NONE,
  MD5,
  SHA_1,
  MURMUR3_32,
  MURMUR3_128,
  SYSTEM;

  public int length() {
    switch (this) {
      case NONE:
        return 0;
      case MD5:
        return 16;
      case SHA_1:
        return 20;
      case MURMUR3_32:
        return 4;
      case MURMUR3_128:
        return 16;
      case SYSTEM:
        return SystemChecksum.BYTES;
      default:
        throw new RuntimeException("panic");
    }
  }
}
