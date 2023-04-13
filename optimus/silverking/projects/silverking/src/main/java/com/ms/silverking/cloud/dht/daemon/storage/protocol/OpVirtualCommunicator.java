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
package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.net.IPAndPort;

/**
 * Used during processing of either an incoming forward message or an
 * incoming response. All entries of the message must be processed
 * and then either forwarded, or resolved locally. In the local
 * resolution case, a result must be sent back.
 * <p>
 * For efficiency purposes, we need to group both the forwards and
 * results. This class groups messages for forwarding by destination.
 * It also groups results for sending in batches.
 * <p>
 * Client classes are provided with the illusion of single object
 * sends in both cases while we maintain efficiency in the actual
 * implementation.
 */
public interface OpVirtualCommunicator<T extends DHTKey, R extends KeyedResult> {
  public void forwardEntry(IPAndPort replica, T entry);

  public void sendResult(R result);
}
