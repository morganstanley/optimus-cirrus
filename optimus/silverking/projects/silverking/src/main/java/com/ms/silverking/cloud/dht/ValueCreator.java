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
package com.ms.silverking.cloud.dht;

import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.numeric.NumConversion;

/**
 * Provides information identifying the creator of values.
 * This typically consists of the IP address and process ID of the creator.
 */
public interface ValueCreator {
  /**
   * Number of bytes in the byte[] representation of this ValueCreator.
   */
  public static final int BYTES = IPAddrUtil.IPV4_BYTES + NumConversion.BYTES_PER_INT;

  /**
   * Return the IP address of the creator.
   *
   * @return the IP address of the creator.
   */
  public byte[] getIP();

  /**
   * Return the ID of the creator.
   *
   * @return the ID of the creator.
   */
  public int getID();

  /**
   * Return the IP address and ID as bytes.
   *
   * @return the IP address and ID as bytes
   */
  public byte[] getBytes();
}
