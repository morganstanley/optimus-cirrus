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
package optimus.platform.relational.dal.persistence

import optimus.platform.dsi.bitemporal.proto.ProtoPickleSerializer
import optimus.platform.dsi.bitemporal.proto.Dsi.FieldProto
import optimus.platform.relational.persistence.ConstValueSerializer

object DalConstValueProtoSerializer extends ConstValueSerializer {

  override def toBytes(o: Any): Array[Byte] = ProtoPickleSerializer.propertiesToProto(o, None).toByteArray

  override def fromBytes(proto: Array[Byte]): Any = ??? /* ProtoPickleSerializer.protoToProperties(FieldProto.parseFrom(proto)) */
}
