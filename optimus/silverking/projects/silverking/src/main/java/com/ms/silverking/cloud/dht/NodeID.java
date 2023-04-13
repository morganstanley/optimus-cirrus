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

import com.ms.silverking.net.IPAndPort;

// Consider deprecating

/**
 * Unique identifier of a DHT node (server/daemon).
 */
public class NodeID implements Comparable<NodeID> {
  private final IPAndPort ipAndPort;

  public NodeID(IPAndPort ipAndPort) {
    this.ipAndPort = ipAndPort;
  }

  @Override
  public int hashCode() {
    return ipAndPort.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    NodeID oID;

    oID = (NodeID) other;
    return this.ipAndPort.equals(oID.ipAndPort);
  }

  @Override
  public int compareTo(NodeID o) {
    return this.ipAndPort.compareTo(o.ipAndPort);
  }

  @Override
  public String toString() {
    return ipAndPort.toString();
  }
}
