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
package optimus.platform.dsi.prc.bitemporal.serde

import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.proto.ExpressionDecoder
import optimus.platform.dsi.expressions.proto.ExpressionEncoder
import optimus.platform.dsi.expressions.proto.Expressions.TemporalityProto

private[optimus] trait DSIQueryTemporalitySerialization {
  final def fromProto(proto: TemporalityProto): DSIQueryTemporality = DSIQueryTemporalitySerializer.deserialize(proto)
  final def toProto(value: DSIQueryTemporality): TemporalityProto = DSIQueryTemporalitySerializer.serialize(value)
}
private[optimus] object DSIQueryTemporalitySerializer
    extends ProtoSerializer[DSIQueryTemporality, TemporalityProto]
    with ExpressionEncoder
    with ExpressionDecoder {
  override def serialize(value: DSIQueryTemporality): TemporalityProto = ??? /* {
    val proto: TemporalityProto = encode(value)
    proto
  } */
  override def deserialize(proto: TemporalityProto): DSIQueryTemporality = ??? /* {
    val temporality: DSIQueryTemporality = decode(proto)
    temporality
  } */
  override protected def encode(id: Id): Int =
    throw new NotImplementedError(s"Cannot encode($id) with ${this.getClass.getSimpleName}")
  override protected def decode(id: Int): Id =
    throw new NotImplementedError(s"Cannot decode($id) with ${this.getClass.getSimpleName}")
}
