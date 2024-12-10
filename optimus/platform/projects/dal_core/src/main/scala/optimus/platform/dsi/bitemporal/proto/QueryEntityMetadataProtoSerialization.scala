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
import scala.jdk.CollectionConverters._
import optimus.platform.storable.EntityMetadata

trait QueryEntityMetadataProtoSerialization extends BasicProtoSerialization {
  implicit val queryEntityMetadataSerializer: QueryEntityMetadataSerializer.type = QueryEntityMetadataSerializer
  implicit val queryEntityMetadataResultSerializer: QueryEntityMetadataResultSerializer.type =
    QueryEntityMetadataResultSerializer
  implicit val entityMetadataSerializer: EntityMetadataSerializer.type = EntityMetadataSerializer
}

object QueryEntityMetadataSerializer
    extends QueryEntityMetadataProtoSerialization
    with ProtoSerializer[QueryEntityMetadata, QueryEntityMetadataProto] {
  override def serialize(meta: QueryEntityMetadata): QueryEntityMetadataProto = ??? /* {
    QueryEntityMetadataProto.newBuilder.setEntityRef(toProto(meta.entityRef)).build
  } */

  override def deserialize(proto: QueryEntityMetadataProto): QueryEntityMetadata = ??? /* {
    QueryEntityMetadata(fromProto(proto.getEntityRef))
  } */
}

object QueryEntityMetadataResultSerializer
    extends QueryEntityMetadataProtoSerialization
    with ProtoSerializer[QueryEntityMetadataResult, QueryEntityMetadataResultProto] {
  override def serialize(metaResult: QueryEntityMetadataResult): QueryEntityMetadataResultProto = ??? /* {
    QueryEntityMetadataResultProto.newBuilder.setEntityMetadata(toProto(metaResult.entityMetadata)).build
  } */

  override def deserialize(proto: QueryEntityMetadataResultProto): QueryEntityMetadataResult = ??? /* {
    QueryEntityMetadataResult(fromProto(proto.getEntityMetadata))
  } */
}

object EntityMetadataSerializer
    extends QueryEntityMetadataProtoSerialization
    with ProtoSerializer[EntityMetadata, EntityMetadataProto] {
  override def serialize(meta: EntityMetadata): EntityMetadataProto = ??? /* {
    EntityMetadataProto.newBuilder
      .setEntityRef(toProto(meta.entityRef))
      .setClassName(meta.className)
      .addAllTypes(meta.types.asJava)
      .build
  } */

  override def deserialize(proto: EntityMetadataProto): EntityMetadata = ??? /* {
    EntityMetadata(fromProto(proto.getEntityRef), proto.getClassName, Seq(proto.getTypesList().asScala: _*))
  } */
}
