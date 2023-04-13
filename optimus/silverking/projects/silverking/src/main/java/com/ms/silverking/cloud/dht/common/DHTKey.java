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
package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.numeric.NumConversion;

/**
 * Internal DHT key representation derived from a hash of the user key
 * DHTKey is created by hashing the key using MD5, which results in 128 bits
 * The 128 bits is represented using 2 longs: MSL (Most significant long) and LSL (Least significant long)
 * We're using longs to represent it because a long in Java is 64 bits (8 bytes)
 */
public interface DHTKey {

  // longs are 8 bytes (64 bits), hence BYTES_PER_KEY is 16 bytes (128 bits)
  public static int BYTES_PER_KEY = 2 * NumConversion.BYTES_PER_LONG;

  // Most significant long
  public long getMSL();

  // Least significant long
  public long getLSL();
}