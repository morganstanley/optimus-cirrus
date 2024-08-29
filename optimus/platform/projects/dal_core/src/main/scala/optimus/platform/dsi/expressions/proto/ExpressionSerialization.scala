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
package optimus.platform.dsi.expressions.proto

import optimus.platform.dsi.bitemporal.proto.ProtoSerialization
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.proto.Expressions.ExpressionProto

import scala.collection.mutable

trait ExpressionSerialization extends ProtoSerialization {
  implicit val expressionSerializer: ExpressionSerializer.type = ExpressionSerializer
}

object ExpressionSerializer extends ProtoSerializer[Expression, ExpressionProto] {
  def serialize(obj: Expression): ExpressionProto = {
    newEncoder.encode(obj)
  }

  def deserialize(proto: ExpressionProto): Expression = {
    newDecoder.decode(proto)
  }

  private class Encoder extends ExpressionEncoder {
    private[this] val idToInt = mutable.HashMap.empty[Id, Int]

    override protected def encode(id: Id): Int = {
      if (id eq Id.EmptyId) -1 else idToInt.getOrElseUpdate(id, idToInt.size)
    }
  }
  private def newEncoder = new Encoder()

  private class Decoder extends ExpressionDecoder {
    private[this] val intToId = mutable.HashMap.empty[Int, Id]

    override protected def decode(id: Int): Id = {
      if (id == -1) Id.EmptyId else intToId.getOrElseUpdate(id, Id())
    }
  }
  private def newDecoder = new Decoder()
}
