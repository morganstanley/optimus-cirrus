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
package com.ms.silverking.net.async;

import java.net.InetSocketAddress;

/**
 * Implemented by users desiring to specify the Receiver for an
 * incoming connection.
 */
public interface ReceiverProvider {
  /**
   * Given a InetSocketAddress, return the Receiver for
   * this address.
   *
   * @param addr
   * @return
   */
  public Receiver getReceiver(InetSocketAddress addr);
}
