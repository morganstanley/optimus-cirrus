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
package optimus.platform.relational.serialization

import optimus.entity.ClassEntityInfo
import optimus.entity.EntityInfoRegistry
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.bitemporal.proto.ProtoSerialization
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer

trait ClassEntityInfoSerialization extends ProtoSerialization {
  implicit val classEntityInfoSerializer: ClassEntityInfoSerializer.type = ClassEntityInfoSerializer
}

object ClassEntityInfoSerializer extends ProtoSerializer[ClassEntityInfo, ClassEntityInfoProto] {
  override def serialize(cei: ClassEntityInfo): ClassEntityInfoProto = {
    ClassEntityInfoProto.newBuilder
      .setFqn(FqnProto.newBuilder.setFqClassName(cei.runtimeClass.getName))
      .build
  }

  override def deserialize(proto: ClassEntityInfoProto): ClassEntityInfo = {
    EntityInfoRegistry.getClassInfo(proto.getFqn.getFqClassName)
  }
}
