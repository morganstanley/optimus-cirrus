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

import optimus.breadcrumbs.ChainedID
import optimus.platform.dsi.bitemporal.proto.Dsi.ChainedIdProto

import scala.jdk.CollectionConverters._

trait ChainedIdSerialization extends ProtoSerialization {
  implicit val chainedIdSerializer: ChainedIdSerializer.type = ChainedIdSerializer

  final def toProto(chainedId: ChainedID): ChainedIdProto = ChainedIdSerializer.serialize(chainedId)
  final def fromProto(proto: ChainedIdProto): ChainedID = ChainedIdSerializer.deserialize(proto)
}

object ChainedIdSerializer extends ProtoSerializer[ChainedID, ChainedIdProto] {
  final def serialize(chainedId: ChainedID): ChainedIdProto = ??? /* {
    ChainedIdProto.newBuilder
      .addAllData(chainedId.asList)
      .build
  } */

  final def deserialize(chainedIdProto: ChainedIdProto): ChainedID = ??? /* {
    ChainedID.fromList(chainedIdProto.getDataList)
  } */
}
