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
package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.common.DHTConstants;

public class DHTNodePort {
  // FUTURE - make port non-static
  // also possibly make it a per-node rather than per-DHT notion
  private static volatile int actualPort = DHTConstants.uninitializedPort;
  private static volatile int dhtPort = DHTConstants.uninitializedPort;

  // For limited use by tooling
  public static void setDHTPort(int _dhtPort) {
    dhtPort = _dhtPort;
  }

  // this is the port fixed in DHT config, though the server may really be listening on another port
  // this port forms the identity of the node. For a node to run on a different port to this,
  // alias mapping must be used - See MessageModuleBase for a comment describing this feature
  public static int getDhtPort() {
    return dhtPort;
  }

  // The port the server is listening on, which may != dhtPort, if overridden in constructor
  public static int getActualPort() {
    return actualPort;
  }

  static void setActualPort(int actualPort) {
    DHTNodePort.actualPort = actualPort;
  }
}
