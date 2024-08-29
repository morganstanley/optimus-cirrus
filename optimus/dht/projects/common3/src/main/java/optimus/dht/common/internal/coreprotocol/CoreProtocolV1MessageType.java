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
package optimus.dht.common.internal.coreprotocol;

public enum CoreProtocolV1MessageType {
  HEARTBEAT(1),
  PING_REQUEST(2),
  PING_REPLY(3);

  private final byte id;

  CoreProtocolV1MessageType(int id) {
    this.id = (byte) id;
  }

  public byte id() {
    return id;
  }

  public static CoreProtocolV1MessageType fromId(byte id) {
    switch (id) {
      case 1:
        return HEARTBEAT;
      case 2:
        return PING_REQUEST;
      case 3:
        return PING_REPLY;
      default:
        throw new IllegalArgumentException("Unknown message id " + id);
    }
  }
}
