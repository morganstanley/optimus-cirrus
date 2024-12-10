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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._

private[proto] trait IsEntitledProtoSerialization {
  implicit val isEntitledCommandSerializer: IsEntitledProtoSerialization.type = IsEntitledProtoSerialization
  implicit val isEntitledResultSerializer: IsEntitledResultSerializer.type = IsEntitledResultSerializer
}

object IsEntitledProtoSerialization
    extends CommandProtoSerializationBase
    with ProtoSerializer[IsEntitled, IsEntitledProto] {

  override def deserialize(proto: IsEntitledProto): IsEntitled = ??? /* {
    IsEntitled(fromProto(proto.getCmd))
  } */

  override def serialize(write: IsEntitled): IsEntitledProto = ??? /* {
    IsEntitledProto.newBuilder
      .setCmd(toProto(write.cmd))
      .build
  } */
}

object IsEntitledResultSerializer extends ProtoSerializer[IsEntitledResult, IsEntitledResultProto] {

  override def deserialize(proto: IsEntitledResultProto): IsEntitledResult = ??? /* {
    IsEntitledResult(proto.getValue)
  } */

  override def serialize(entitled: IsEntitledResult): IsEntitledResultProto = ??? /* {
    IsEntitledResultProto.newBuilder
      .setValue(entitled.res)
      .build
  } */
}
