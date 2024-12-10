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
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.proto.ExpressionDecoder
import optimus.platform.dsi.expressions.proto.ExpressionEncoder
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.SerializedBusinessEvent

import optimus.scalacompat.collection.javaapi.CollectionConverters
import scala.jdk.CollectionConverters._
import scala.collection.mutable

class QueryResultEncoder extends ExpressionEncoder {
  private[this] val idToInt = mutable.HashMap.empty[Id, Int]

  override protected def encode(id: Id): Int = {
    idToInt.getOrElseUpdate(id, idToInt.size)
  }

  def encode(result: PartialQueryResult): PartialQueryResultProto = ??? /* {
    val values = result.values.map(row => encode(row))
    result.metaData
      .map(metaData =>
        PartialQueryResultProto.newBuilder
          .addAllResult(values.asJava)
          .setMetadata(encode(metaData))
          .setIsLast(result.isLast)
          .build())
      .getOrElse(PartialQueryResultProto.newBuilder.addAllResult(values.asJava).setIsLast(result.isLast).build())
  } */

  def encode(result: QueryResult): QueryResultProto = ??? /* {
    val values = result.value.map(row => encode(row))
    QueryResultProto.newBuilder.addAllResult(values.asJava).setMetadata(encode(result.metaData)).build()
  } */

  def encode(row: Array[Any]): ValuesProto = ??? /* {
    val newRow = row.map(v => encode(v))
    ValuesProto.newBuilder.addAllRow(CollectionConverters.asJavaIterable(newRow)).build()
  } */

  def encode(row: Any): ValueProto = ??? /* {
    val builder = ValueProto.newBuilder
    row match {
      case x: PersistentEntity        => builder.setEntityValue(PersistentEntitySerializer.serialize(x)).build()
      case x: SerializedBusinessEvent => builder.setEventValue(SerializedBusinessEventSerializer.serialize(x)).build()
      case x: Result                  => builder.setResultValue(ResultSerializer.serialize(x)).build()
      case x: SelectSpaceResult.Rectangle =>
        builder.setRectangleValue(SelectSpaceRectangleSerializer.serialize(x)).build()
      case _ => builder.setFieldValue(ProtoPickleSerializer.propertiesToProto(row, None)).build()
    }
  } */

  def encode(metaData: QueryResultMetaData): QueryResultMetaDataProto = ??? /* {
    val fields = metaData.fields.map(f => encode(f))
    QueryResultMetaDataProto.newBuilder.addAllFields(CollectionConverters.asJavaIterable(fields)).build()
  } */

  def encode(field: Field): QueryFieldProto = ??? /* {
    QueryFieldProto.newBuilder.setName(field.name).setTypecode(encode(field.typeCode)).build()
  } */
}

class QueryResultDecoder extends ExpressionDecoder {
  private[this] val intToId = mutable.HashMap.empty[Int, Id]

  override protected def decode(id: Int): Id = {
    intToId.getOrElseUpdate(id, Id())
  }

  def decode(result: PartialQueryResultProto): PartialQueryResult = ??? /* {
    if (result.getIsLast && result.hasMetadata)
      FinalPartialQueryResult(result.getResultList.asScala.map(decode(_)), Some(decode(result.getMetadata)))
    else
      IntermediatePartialQueryResult(result.getResultList.asScala.map(decode(_)))
  } */

  def decode(result: QueryResultProto): QueryResult = ??? /* {
    QueryResult(result.getResultList.asScala.map(decode(_)), decode(result.getMetadata))
  } */

  def decode(row: ValuesProto): Array[Any] = ??? /* {
    row.getRowList.asScala.iterator.map(decode(_)).toArray
  } */

  def decode(row: ValueProto): Any = ??? /* {
    if (row.hasFieldValue) ProtoPickleSerializer.protoToProperties(row.getFieldValue)
    else if (row.hasEntityValue) PersistentEntitySerializer.deserialize(row.getEntityValue)
    else if (row.hasEventValue) SerializedBusinessEventSerializer.deserialize(row.getEventValue)
    else if (row.hasResultValue) ResultSerializer.deserialize(row.getResultValue)
    else if (row.hasRectangleValue) SelectSpaceRectangleSerializer.deserialize(row.getRectangleValue)
    else throw new UnsupportedOperationException(s"Unexpected row value: $row.")
  } */

  def decode(metaData: QueryResultMetaDataProto): QueryResultMetaData = ??? /* {
    QueryResultMetaData(metaData.getFieldsList.asScala.iterator.map(decode(_)).toArray)
  } */

  def decode(field: QueryFieldProto): Field = ??? /* {
    Field(field.getName, decode(field.getTypecode))
  } */
}

trait ResultSerializationBase extends ProtoSerialization {
  implicit val partialQueryResultSerializer: PartialQueryResultSerializer.type = PartialQueryResultSerializer
  implicit val queryResultSerializer: QueryResultSerializer.type = QueryResultSerializer
}

object PartialQueryResultSerializer
    extends ResultSerializationBase
    with ProtoSerializer[PartialQueryResult, PartialQueryResultProto] {
  override def serialize(result: PartialQueryResult): PartialQueryResultProto = {
    new QueryResultEncoder().encode(result)
  }

  override def deserialize(proto: PartialQueryResultProto): PartialQueryResult = {
    new QueryResultDecoder().decode(proto)
  }
}

object QueryResultSerializer extends ResultSerializationBase with ProtoSerializer[QueryResult, QueryResultProto] {
  override def serialize(result: QueryResult): QueryResultProto = {
    new QueryResultEncoder().encode(result)
  }

  override def deserialize(proto: QueryResultProto): QueryResult = {
    new QueryResultDecoder().decode(proto)
  }
}
