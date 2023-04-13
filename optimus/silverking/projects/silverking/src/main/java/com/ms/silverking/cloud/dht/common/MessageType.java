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

/**
 * Type of client operation.
 */
public enum MessageType {
  // The order matters (since SK serde the type by its ordinal)
  LEGACY_PUT(0), // TODO (OPTIMUS-43373): Remove this legacy put and put_trace
  RETRIEVE(1),
  PUT_RESPONSE(2),
  PUT_UPDATE(3),
  RETRIEVE_RESPONSE(4),
  SNAPSHOT(5),
  OP_RESPONSE(6),
  SYNC_REQUEST(7),
  CHECKSUM_TREE_REQUEST(8),
  CHECKSUM_TREE(9),
  OP_UPDATE(10),
  OP_NOP(11),
  OP_PING(12),
  OP_PING_ACK(13),
  NAMESPACE_REQUEST(14),
  NAMESPACE_RESPONSE(15),
  SET_CONVERGENCE_STATE(16),
  // TODO (OPTIMUS-40641): remove the deprecated REAP MessageType and adjust deserialization logic in
  // EnumValues.messageType and related code in IncomingMessageGroup.readFromChannel
  DO_NOT_USE_REAP(17),
  GLOBAL_COMMAND_NEW(18),
  GLOBAL_COMMAND_UPDATE(19),
  GLOBAL_COMMAND_RESPONSE(20),
  PROGRESS(21),
  LEGACY_PUT_TRACE(22), // TODO (OPTIMUS-43373): Remove this legacy put and put_trace
  RETRIEVE_TRACE(23),
  PUT_RESPONSE_TRACE(24),
  PUT_UPDATE_TRACE(25),
  RETRIEVE_RESPONSE_TRACE(26),
  ERROR_RESPONSE(27),
  PUT(28),
  PUT_TRACE(29);

  private final int ordinal;

  MessageType(int ordinal) {
    this.ordinal = ordinal;
  }

  public int getOrdinal() {
    return ordinal;
  }
}
