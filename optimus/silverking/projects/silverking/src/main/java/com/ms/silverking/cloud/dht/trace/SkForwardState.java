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
package com.ms.silverking.cloud.dht.trace;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.DHTNodePort;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;

/**
 * A SkTrace has the following three ways of state transformation: 1. Proxy doesn't forward
 * NotForwarded@Client -> NotForwarded@Proxy -> NotForwarded@Client 2. Proxy does local forward
 * NotForwarded@Client -> LocalForwarded@Proxy -> LocalForwarded@Client 3. Proxy does remote forward
 * NotForwarded@Client -> RemoteForwarded@Proxy -> RemoteForwarded@Remote -> RemoteForwarded@Proxy
 * -> RemoteForwarded@Client
 */
public enum SkForwardState {
  NotForwarded,
  LocalForwarded,
  RemoteForwarded;

  public static IPAndPort getLocalIpAndPort() {
    return new IPAndPort(
        IPAddrUtil.localIP(),
        DHTNodePort.getDhtPort() == DHTConstants.uninitializedPort ? 0 : DHTNodePort.getDhtPort());
  }

  public static SkForwardState nextForwardState(AddrAndPort replica) {
    if (getLocalIpAndPort().equals(replica)) {
      return LocalForwarded;
    } else {
      return RemoteForwarded;
    }
  }
}
